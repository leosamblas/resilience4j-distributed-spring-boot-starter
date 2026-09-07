package com.resilience.distributed;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configurações para distribuição do estado dos CircuitBreakers do Resilience4j
 * entre múltiplos pods/réplicas do mesmo serviço via Redis pub/sub e chaves compartilhadas com TTL.
 * <p>
 * Vinculado sob o prefixo {@code resilience4j.distributed}.
 */
@ConfigurationProperties(prefix = "resilience4j.distributed")
public class DistributedCircuitBreakerProperties {

    /** Chave mestre. Quando false, a distribuição de estado é desativada por completo. */
    private boolean enabled = true;

    /**
     * Tempo de vida (TTL) da chave de estado compartilhado no Redis. Deve exceder com folga o
     * maior {@code waitDurationInOpenState} configurado entre seus circuit breakers, garantindo que
     * um pod recém-iniciado durante uma indisponibilidade encontre uma chave válida em vez de expirar.
     * O padrão é conservador (5 minutos), pois não há agendador em segundo plano — a chave é
     * atualizada apenas em transições de estado e em chamadas bloqueadas enquanto OPEN.
     */
    private Duration stateTtl = Duration.ofMinutes(5);

    /**
     * Intervalo mínimo entre gravações de renovação de TTL no Redis disparadas por {@code onCallNotPermitted}.
     * Aplica throttling nas operações do Redis sob alto tráfego enquanto o circuito estiver OPEN.
     */
    private Duration ttlRefreshInterval = Duration.ofSeconds(5);

    /**
     * Jitter aleatório máximo aplicado antes de um pod seguir um sinal remoto de CLOSED
     * para sua transição local para HALF_OPEN. Evita que todos os pods sondem o serviço
     * dependente simultaneamente na recuperação (efeito manada / thundering herd).
     */
    private Duration maxJitter = Duration.ofSeconds(3);

    /**
     * Lista com nomes dos circuit breakers a gerenciar. Vazio (padrão) significa que todo
     * circuit breaker registrado no {@code CircuitBreakerRegistry} será distribuído automaticamente,
     * incluindo aqueles criados dinamicamente em tempo de execução.
     */
    private List<String> breakerNames = new ArrayList<>();

    /**
     * Prefixo de namespace para chaves e canais do Redis (ex: nome de ambiente ou cluster).
     * Evita que ambientes distintos (staging/prod) que compartilhem a mesma instância do Redis
     * interfiram nos estados de circuit breaker uns dos outros.
     */
    private String namespace = "default";

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Duration getStateTtl() {
        return stateTtl;
    }

    public void setStateTtl(Duration stateTtl) {
        this.stateTtl = stateTtl;
    }

    public Duration getTtlRefreshInterval() {
        return ttlRefreshInterval;
    }

    public void setTtlRefreshInterval(Duration ttlRefreshInterval) {
        this.ttlRefreshInterval = ttlRefreshInterval;
    }

    public Duration getMaxJitter() {
        return maxJitter;
    }

    public void setMaxJitter(Duration maxJitter) {
        this.maxJitter = maxJitter;
    }

    public List<String> getBreakerNames() {
        return breakerNames;
    }

    public void setBreakerNames(List<String> breakerNames) {
        this.breakerNames = breakerNames;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
}
