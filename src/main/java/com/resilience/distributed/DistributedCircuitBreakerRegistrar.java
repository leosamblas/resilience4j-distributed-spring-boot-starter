package com.resilience.distributed;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

/**
 * Registra automaticamente cada {@link CircuitBreaker} relevante no
 * {@link DistributedCircuitBreakerCoordinator} — tanto os circuit breakers que já
 * existem na inicialização (declarados no {@code application.yml}) quanto aqueles criados
 * dinamicamente em tempo de execução. As aplicações consumidoras não precisam escrever
 * código Java: declarar um circuit breaker na configuração é suficiente para torná-lo
 * distribuído, a menos que {@code resilience4j.distributed.breaker-names} defina uma
 * lista explícita de nomes permitidos.
 */
public class DistributedCircuitBreakerRegistrar {

    private final DistributedCircuitBreakerCoordinator coordinator;
    private final DistributedCircuitBreakerProperties properties;

    public DistributedCircuitBreakerRegistrar(CircuitBreakerRegistry registry,
                                               DistributedCircuitBreakerCoordinator coordinator,
                                               DistributedCircuitBreakerProperties properties) {
        this.coordinator = coordinator;
        this.properties = properties;

        registry.getAllCircuitBreakers().forEach(this::maybeManage);
        registry.getEventPublisher().onEntryAdded(event -> maybeManage(event.getAddedEntry()));
    }

    private void maybeManage(CircuitBreaker cb) {
        boolean shouldManage = properties.getBreakerNames().isEmpty()
            || properties.getBreakerNames().contains(cb.getName());

        if (shouldManage) {
            coordinator.manage(cb.getName());
        }
    }
}
