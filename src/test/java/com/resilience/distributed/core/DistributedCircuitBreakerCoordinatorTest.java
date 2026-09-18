package com.resilience.distributed.core;
 
 import com.resilience.distributed.autoconfigure.DistributedCircuitBreakerProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

@ExtendWith(MockitoExtension.class)
@DisplayName("Testes do Coordenador Distribuído (DistributedCircuitBreakerCoordinator)")
@SuppressWarnings("unchecked")
class DistributedCircuitBreakerCoordinatorTest {

    @Mock
    private ReactiveStringRedisTemplate redis;

    @Mock
    private ReactiveValueOperations<String, String> valueOps;

    @Mock
    private ReactiveRedisMessageListenerContainer listenerContainer;

    private CircuitBreakerRegistry registry;
    private DistributedCircuitBreakerProperties properties;
    private Sinks.Many<ReactiveSubscription.Message<String, String>> messageSink;
    private DistributedCircuitBreakerCoordinator coordinator;

    @BeforeEach
    void setUp() {
        registry = CircuitBreakerRegistry.ofDefaults();
        properties = new DistributedCircuitBreakerProperties();
        properties.setNamespace("test");
        properties.setMaxJitter(Duration.ofMillis(10));
        properties.setStateTtl(Duration.ofMinutes(5));
        properties.setTtlRefreshInterval(Duration.ofSeconds(5));

        lenient().when(redis.opsForValue()).thenReturn(valueOps);

        messageSink = Sinks.many().multicast().onBackpressureBuffer();
        lenient().when(listenerContainer.receive(any(ChannelTopic.class))).thenReturn((Flux) messageSink.asFlux());

        coordinator = new DistributedCircuitBreakerCoordinator(redis, listenerContainer, registry, properties);
    }

    @Test
    @DisplayName("Deve registrar o breaker e sincronizar o estado inicial OPEN a partir do Redis")
    void manage_registersBreakerAndSyncsInitialOpenStateFromRedis() {
        String stateKey = "cb-state:test:orderService";
        when(valueOps.get(stateKey)).thenReturn(Mono.just("remote-pod-999|OPEN"));

        coordinator.manage("orderService");

        CircuitBreaker cb = registry.circuitBreaker("orderService");
        assertThat(coordinator.getManagedBreakerNames()).contains("orderService");
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);
    }

    @Test
    @DisplayName("Deve ser idempotente e não duplicar subscrições em chamadas repetidas de manage")
    void manage_isIdempotent() {
        when(valueOps.get(anyString())).thenReturn(Mono.empty());

        coordinator.manage("orderService");
        coordinator.manage("orderService"); // A segunda chamada deve ser no-op

        verify(valueOps, times(1)).get("cb-state:test:orderService");
        verify(listenerContainer, times(1)).receive(any(ChannelTopic.class));
    }

    @Test
    @DisplayName("Deve ignorar o próprio identificador da instância na sincronização inicial do Redis")
    void manage_initialSyncIgnoresSelfEcho() {
        String stateKey = "cb-state:test:orderService";
        String selfPayload = coordinator.getInstanceId() + "|OPEN";
        when(valueOps.get(stateKey)).thenReturn(Mono.just(selfPayload));

        coordinator.manage("orderService");

        CircuitBreaker cb = registry.circuitBreaker("orderService");
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    @DisplayName("Deve publicar no canal pub/sub e gravar chave com TTL no Redis quando houver transição local para OPEN")
    void localTransitionToOpen_publishesToRedisAndSetsTtlKey() {
        when(valueOps.get(anyString())).thenReturn(Mono.empty());
        when(valueOps.set(anyString(), anyString(), any(Duration.class))).thenReturn(Mono.just(true));
        when(redis.convertAndSend(anyString(), anyString())).thenReturn(Mono.just(1L));

        coordinator.manage("orderService");
        CircuitBreaker cb = registry.circuitBreaker("orderService");

        cb.transitionToOpenState();

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Duration> ttlCaptor = ArgumentCaptor.forClass(Duration.class);

        verify(valueOps).set(keyCaptor.capture(), payloadCaptor.capture(), ttlCaptor.capture());
        assertThat(keyCaptor.getValue()).isEqualTo("cb-state:test:orderService");
        assertThat(payloadCaptor.getValue()).startsWith(coordinator.getInstanceId() + "|OPEN");
        assertThat(ttlCaptor.getValue()).isEqualTo(Duration.ofMinutes(5));

        ArgumentCaptor<String> channelCaptor = ArgumentCaptor.forClass(String.class);
        verify(redis).convertAndSend(channelCaptor.capture(), eq(payloadCaptor.getValue()));
        assertThat(channelCaptor.getValue()).isEqualTo("cb-events:test:orderService");
    }

    @Test
    @DisplayName("Não deve transmitir mensagens via Redis quando a transição local for para HALF_OPEN")
    void localTransitionToHalfOpen_isNeverBroadcast() {
        when(valueOps.get(anyString())).thenReturn(Mono.empty());
        when(valueOps.set(anyString(), anyString(), any(Duration.class))).thenReturn(Mono.just(true));
        when(redis.convertAndSend(anyString(), anyString())).thenReturn(Mono.just(1L));

        coordinator.manage("orderService");
        CircuitBreaker cb = registry.circuitBreaker("orderService");

        cb.transitionToOpenState();
        clearInvocations(valueOps, redis);

        cb.transitionToHalfOpenState();

        // HALF_OPEN NUNCA deve disparar publicação
        verify(valueOps, never()).set(anyString(), anyString(), any(Duration.class));
        verify(redis, never()).convertAndSend(anyString(), anyString());
    }

    @Test
    @DisplayName("Deve aplicar throttling nas renovações de TTL do Redis em chamadas bloqueadas consecutivas")
    void onCallNotPermitted_throttlesSubsequentTtlRefreshes() {
        when(valueOps.get(anyString())).thenReturn(Mono.empty());
        when(valueOps.set(anyString(), anyString(), any(Duration.class))).thenReturn(Mono.just(true));
        when(redis.convertAndSend(anyString(), anyString())).thenReturn(Mono.just(1L));

        coordinator.manage("orderService");
        CircuitBreaker cb = registry.circuitBreaker("orderService");

        cb.transitionToOpenState();
        clearInvocations(valueOps);

        // Primeira chamada bloqueada: dispara renovação de TTL
        boolean permitted1 = cb.tryAcquirePermission();
        assertThat(permitted1).isFalse();
        verify(valueOps, times(1)).set(eq("cb-state:test:orderService"), anyString(), eq(Duration.ofMinutes(5)));

        // Chamadas bloqueadas subsequentes imediatas: devem sofrer THROTTLING e NÃO acessar o Redis novamente
        boolean permitted2 = cb.tryAcquirePermission();
        boolean permitted3 = cb.tryAcquirePermission();
        assertThat(permitted2).isFalse();
        assertThat(permitted3).isFalse();

        // Continua com exatamente 1 invocação
        verify(valueOps, times(1)).set(eq("cb-state:test:orderService"), anyString(), eq(Duration.ofMinutes(5)));
    }

    @Test
    @DisplayName("Deve forçar o circuit breaker local a abrir ao receber sinal remoto de OPEN")
    void remoteOpenSignal_forcesLocalBreakerToOpen() {
        when(valueOps.get(anyString())).thenReturn(Mono.empty());
        coordinator.manage("orderService");
        CircuitBreaker cb = registry.circuitBreaker("orderService");
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.CLOSED);

        ReactiveSubscription.Message<String, String> message = mock(ReactiveSubscription.Message.class);
        when(message.getMessage()).thenReturn("another-pod-uuid|OPEN");
        messageSink.tryEmitNext(message);

        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);
    }

    @Test
    @DisplayName("Deve transicionar para HALF_OPEN após aplicar jitter ao receber sinal remoto de CLOSED")
    void remoteClosedSignal_transitionsToHalfOpenAfterJitter() throws Exception {
        when(valueOps.get(anyString())).thenReturn(Mono.empty());
        when(valueOps.set(anyString(), anyString(), any(Duration.class))).thenReturn(Mono.just(true));
        when(redis.convertAndSend(anyString(), anyString())).thenReturn(Mono.just(1L));

        coordinator.manage("orderService");
        CircuitBreaker cb = registry.circuitBreaker("orderService");
        cb.transitionToOpenState();
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        ReactiveSubscription.Message<String, String> message = mock(ReactiveSubscription.Message.class);
        when(message.getMessage()).thenReturn("another-pod-uuid|CLOSED");
        messageSink.tryEmitNext(message);

        // Aguarda jitter (máximo 10ms)
        Thread.sleep(150);

        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);
    }

    @Test
    @DisplayName("Deve cancelar tarefa de jitter pendente caso um novo sinal remoto de OPEN chegue antes do delay expirar")
    void remoteOpenSignal_cancelsPendingJitterTask() throws Exception {
        // Configura jitter mais longo para possibilitar cancelamento intermediário
        properties.setMaxJitter(Duration.ofMillis(300));
        when(valueOps.get(anyString())).thenReturn(Mono.empty());
        when(valueOps.set(anyString(), anyString(), any(Duration.class))).thenReturn(Mono.just(true));
        when(redis.convertAndSend(anyString(), anyString())).thenReturn(Mono.just(1L));

        coordinator.manage("orderService");
        CircuitBreaker cb = registry.circuitBreaker("orderService");
        cb.transitionToOpenState();
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        // Recebe CLOSED: agenda atraso com jitter
        ReactiveSubscription.Message<String, String> closedMsg = mock(ReactiveSubscription.Message.class);
        when(closedMsg.getMessage()).thenReturn("another-pod-uuid|CLOSED");
        messageSink.tryEmitNext(closedMsg);

        // Antes do jitter disparar, outro pod envia OPEN: deve cancelar a transição pendente para HALF_OPEN
        Thread.sleep(50);
        ReactiveSubscription.Message<String, String> openMsg = mock(ReactiveSubscription.Message.class);
        when(openMsg.getMessage()).thenReturn("yet-another-pod-uuid|OPEN");
        messageSink.tryEmitNext(openMsg);

        // Aguarda tempo superior ao jitter original
        Thread.sleep(350);

        // Deve permanecer OPEN, sem nunca ter ido para HALF_OPEN
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);
    }

    @Test
    @DisplayName("Deve ignorar mensagens com payloads mal formatados sem lançar exceções")
    void remoteSignal_ignoresMalformedPayload() {
        when(valueOps.get(anyString())).thenReturn(Mono.empty());
        coordinator.manage("orderService");
        CircuitBreaker cb = registry.circuitBreaker("orderService");

        ReactiveSubscription.Message<String, String> message = mock(ReactiveSubscription.Message.class);
        when(message.getMessage()).thenReturn("malformed-single-string");
        messageSink.tryEmitNext(message);

        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    @DisplayName("Deve consultar o estado atual gravado no Redis para introspecção")
    void peekRemoteState_readsFromRedis() {
        when(valueOps.get("cb-state:test:orderService")).thenReturn(Mono.just("pod-123|OPEN"));

        String state = coordinator.peekRemoteState("orderService").block();

        assertThat(state).isEqualTo("pod-123|OPEN");
    }

    @Test
    @DisplayName("Deve descartar todas as subscrições reativas e tarefas pendentes no shutdown sem erros")
    void destroy_disposesAllSubscriptionsAndPendingTasksWithoutErrors() {
        when(valueOps.get(anyString())).thenReturn(Mono.empty());
        coordinator.manage("orderService");

        // Deve descartar recursos sem lançar exceções
        assertThatNoException().isThrownBy(() -> coordinator.destroy());
    }
}
