package com.resilience.distributed;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * Compartilha o estado OPEN/CLOSED de CircuitBreakers entre múltiplos pods/réplicas do mesmo serviço.
 * <p>
 * Mecanismo de Funcionamento:
 * <ul>
 *   <li>Cada pod continua executando seu próprio {@link CircuitBreaker} local com cálculo
 *       independente da taxa de falhas — essa parte não é distribuída.</li>
 *   <li>Quando o breaker local de um pod muda de estado, ele transmite o desfecho (e não a matemática)
 *       para todos os outros pods via canal Redis pub/sub, e persiste o valor em uma chave com TTL
 *       no Redis para que novos pods possam adotar o estado atual na inicialização.</li>
 *   <li>Sinais remotos de OPEN forçam uma transição local imediata para OPEN,
 *       cancelando qualquer recuperação com jitter que estivesse pendente.</li>
 *   <li>Sinais remotos de CLOSED disparam uma transição com jitter aleatório para
 *       HALF_OPEN, evitando que todos os pods testem a dependência ao mesmo tempo.</li>
 *   <li>Não há agendador em segundo plano. A chave compartilhada é renovada apenas em
 *       transições de estado e em chamadas bloqueadas enquanto OPEN ({@code onCallNotPermitted}),
 *       com throttling controlado por {@link DistributedCircuitBreakerProperties#getTtlRefreshInterval()}.</li>
 * </ul>
 */
public class DistributedCircuitBreakerCoordinator implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(DistributedCircuitBreakerCoordinator.class);

    private static final String STATE_OPEN = "OPEN";
    private static final String STATE_CLOSED = "CLOSED";

    private final ReactiveStringRedisTemplate redis;
    private final ReactiveRedisMessageListenerContainer listenerContainer;
    private final CircuitBreakerRegistry registry;
    private final DistributedCircuitBreakerProperties properties;

    /**
     * Identificador único por JVM/pod. Nunca compartilhado — sua única finalidade é permitir
     * que o pod reconheça e ignore suas próprias mensagens refletidas de volta pelo pub/sub.
     */
    private final String instanceId = UUID.randomUUID().toString();

    /** Nomes dos breakers atualmente gerenciados, para introspecção e endpoints de diagnóstico. */
    private final Set<String> managedBreakerNames = ConcurrentHashMap.newKeySet();

    /** Registra o último timestamp de renovação de TTL por breaker para evitar sobrecarga no Redis em alto tráfego. */
    private final ConcurrentHashMap<String, Long> lastTtlRefreshTimestamps = new ConcurrentHashMap<>();

    /** Rastreia tarefas de jitter pendentes para permitir cancelamento imediato caso um novo OPEN chegue. */
    private final ConcurrentHashMap<String, Disposable> pendingJitterTasks = new ConcurrentHashMap<>();

    /** Rastreia todas as subscrições reativas ativas para encerramento gracioso (shutdown). */
    private final Disposable.Composite subscriptions = Disposables.composite();

    public DistributedCircuitBreakerCoordinator(ReactiveStringRedisTemplate redis,
                                                 ReactiveRedisMessageListenerContainer listenerContainer,
                                                 CircuitBreakerRegistry registry,
                                                 DistributedCircuitBreakerProperties properties) {
        this.redis = redis;
        this.listenerContainer = listenerContainer;
        this.registry = registry;
        this.properties = properties;
    }

    /**
     * Inicia a distribuição de estado para o circuit breaker informado. Idempotente:
     * múltiplas chamadas para o mesmo nome de breaker não duplicam listeners nem subscrições.
     */
    public void manage(String breakerName) {
        if (!managedBreakerNames.add(breakerName)) {
            log.debug("Circuit breaker '{}' já está sob gerenciamento", breakerName);
            return;
        }

        CircuitBreaker cb = registry.circuitBreaker(breakerName);
        String channel = channelFor(breakerName);
        String stateKey = stateKeyFor(breakerName);

        // Sincronização na inicialização: adota o estado já compartilhado no Redis,
        // garantindo que um pod recém-iniciado não comece CLOSED caso os demais pods
        // já tenham identificado que a dependência externa está fora do ar.
        Disposable syncSub = redis.opsForValue().get(stateKey)
            .doOnNext(state -> applyRemoteState(cb, state))
            .subscribe(
                v -> { /* no-op */ },
                error -> log.warn("Não foi possível sincronizar o estado inicial de {} do Redis: {}",
                    breakerName, error.toString())
            );
        subscriptions.add(syncSub);

        // Transmite transições de estado locais.
        cb.getEventPublisher().onStateTransition(event -> {
            CircuitBreaker.State toState = event.getStateTransition().getToState();

            // Intencionalmente não transmitimos HALF_OPEN: é um estado local de sondagem transitória,
            // e transmiti-lo faria com que todos os pods se engatilhassem mutuamente em testes simultâneos.
            if (toState == CircuitBreaker.State.HALF_OPEN) {
                return;
            }

            publish(stateKey, channel, toState.name());
            log.info("Circuit breaker '{}' transicionou para {} — transmitido aos demais pods", breakerName, toState);
        });

        // Renovação oportunística de TTL: aplica throttling via ttlRefreshInterval para evitar
        // bombardear o Redis com milhares de operações SET por segundo sob alto tráfego.
        cb.getEventPublisher().onCallNotPermitted(event -> {
            if (cb.getState() == CircuitBreaker.State.OPEN) {
                long now = System.currentTimeMillis();
                long minIntervalMs = properties.getTtlRefreshInterval().toMillis();
                Long last = lastTtlRefreshTimestamps.get(breakerName);

                if (last == null || (now - last) >= minIntervalMs) {
                    lastTtlRefreshTimestamps.put(breakerName, now);
                    Disposable refreshSub = redis.opsForValue()
                        .set(stateKey, encode(STATE_OPEN), properties.getStateTtl())
                        .subscribe(
                            v -> { /* no-op */ },
                            error -> log.debug("Não foi possível renovar o TTL de {}: {}", breakerName, error.toString())
                        );
                    subscriptions.add(refreshSub);
                }
            }
        });

        // Escuta transições dos outros pods com retry backoff para resiliência de rede.
        Disposable listenerSub = listenerContainer.receive(ChannelTopic.of(channel))
            .map(m -> m.getMessage())
            .doOnNext(payload -> applyRemoteState(cb, payload))
            .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                .maxBackoff(Duration.ofSeconds(15))
                .doBeforeRetry(sig -> log.warn("Listener pub/sub do Redis para '{}' desconectado, tentando reconectar: {}",
                    breakerName, sig.failure().getMessage())))
            .subscribe(
                v -> { /* no-op */ },
                error -> log.error("Listener do circuit breaker distribuído falhou para {}: {}",
                    breakerName, error.toString())
            );
        subscriptions.add(listenerSub);
    }

    private void publish(String stateKey, String channel, String state) {
        String payload = encode(state);
        Disposable pubSub = redis.opsForValue().set(stateKey, payload, properties.getStateTtl())
            .then(redis.convertAndSend(channel, payload))
            .subscribe(
                v -> { /* no-op */ },
                error -> log.warn("Failed to broadcast circuit breaker state to Redis: {}", error.toString())
            );
        subscriptions.add(pubSub);
    }

    private void applyRemoteState(CircuitBreaker cb, String payload) {
        if (payload == null || payload.isBlank()) {
            return;
        }

        String[] parts = payload.split("\\|", 2);
        if (parts.length < 2) {
            log.debug("Ignoring malformed distributed circuit breaker payload: {}", payload);
            return;
        }

        String senderId = parts[0];
        String state = parts[1];

        // Ignora nossa própria transmissão refletida de volta pelo pub/sub do Redis.
        if (senderId.equals(instanceId)) {
            return;
        }

        switch (state) {
            case STATE_OPEN -> handleRemoteOpen(cb);
            case STATE_CLOSED -> handleRemoteClosed(cb);
            default -> log.debug("Ignorando estado de circuit breaker distribuído não reconhecido '{}' para '{}'",
                state, cb.getName());
        }
    }

    private void handleRemoteOpen(CircuitBreaker cb) {
        // Cancela qualquer transição com jitter pendente para HALF_OPEN (evita race condition)
        cancelPendingJitter(cb.getName(), "devido ao recebimento de sinal remoto OPEN");

        if (cb.getState() != CircuitBreaker.State.OPEN) {
            log.warn("Forçando circuit breaker '{}' para OPEN devido a sinal remoto de outro pod",
                cb.getName());
            cb.transitionToOpenState();
        }
    }

    private void handleRemoteClosed(CircuitBreaker cb) {
        long jitterMs = ThreadLocalRandom.current()
            .nextLong(0, Math.max(1, properties.getMaxJitter().toMillis()));

        Disposable jitterTask = Mono.delay(Duration.ofMillis(jitterMs)).subscribe(tick -> {
            pendingJitterTasks.remove(cb.getName());
            if (cb.getState() == CircuitBreaker.State.OPEN) {
                log.info("Seguindo sinal remoto CLOSED para '{}' — transicionando para HALF_OPEN (jitter {}ms)",
                    cb.getName(), jitterMs);
                cb.transitionToHalfOpenState();
            }
        });

        Disposable old = pendingJitterTasks.put(cb.getName(), jitterTask);
        if (old != null && !old.isDisposed()) {
            old.dispose();
        }
    }

    private void cancelPendingJitter(String breakerName, String reason) {
        Disposable pending = pendingJitterTasks.remove(breakerName);
        if (pending != null && !pending.isDisposed()) {
            pending.dispose();
            log.debug("Transição com jitter cancelada para '{}' {}", breakerName, reason);
        }
    }

    private String encode(String state) {
        return instanceId + "|" + state;
    }

    private String channelFor(String breakerName) {
        return "cb-events:" + properties.getNamespace() + ":" + breakerName;
    }

    private String stateKeyFor(String breakerName) {
        return "cb-state:" + properties.getNamespace() + ":" + breakerName;
    }

    /** Identificador único deste pod — nunca compartilhado, usado apenas para ignorar auto-transmissões. */
    public String getInstanceId() {
        return instanceId;
    }

    /** Nomes dos circuit breakers atualmente distribuídos por este coordenador. */
    public Set<String> getManagedBreakerNames() {
        return Collections.unmodifiableSet(managedBreakerNames);
    }

    /**
     * Consulta o estado atualmente compartilhado no Redis para um breaker, sem efetuar mutações.
     * Retorna um Mono que completa vazio se nenhuma chave estiver definida (expirada ou nunca gravada).
     * Destinado para introspecção somente leitura, por exemplo, por {@link DistributedCircuitBreakerEndpoint}.
     */
    public Mono<String> peekRemoteState(String breakerName) {
        return redis.opsForValue().get(stateKeyFor(breakerName));
    }

    @Override
    public void destroy() {
        pendingJitterTasks.values().forEach(task -> {
            if (task != null && !task.isDisposed()) {
                task.dispose();
            }
        });
        pendingJitterTasks.clear();
        subscriptions.dispose();
        log.info("DistributedCircuitBreakerCoordinator destruído, todas as subscrições foram encerradas");
    }
}
