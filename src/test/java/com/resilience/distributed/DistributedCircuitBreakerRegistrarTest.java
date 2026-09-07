package com.resilience.distributed;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

@ExtendWith(MockitoExtension.class)
@DisplayName("Testes do DistributedCircuitBreakerRegistrar")
class DistributedCircuitBreakerRegistrarTest {

    @Mock
    private DistributedCircuitBreakerCoordinator coordinator;

    private CircuitBreakerRegistry registry;
    private DistributedCircuitBreakerProperties properties;

    @BeforeEach
    void setUp() {
        registry = CircuitBreakerRegistry.ofDefaults();
        properties = new DistributedCircuitBreakerProperties();
    }

    @Test
    @DisplayName("Deve registrar todos os circuit breakers existentes quando breakerNames estiver vazio")
    void registersAllExistingBreakersWhenBreakerNamesIsEmpty() {
        registry.circuitBreaker("orderService");
        registry.circuitBreaker("paymentService");

        new DistributedCircuitBreakerRegistrar(registry, coordinator, properties);

        verify(coordinator).manage("orderService");
        verify(coordinator).manage("paymentService");
        verifyNoMoreInteractions(coordinator);
    }

    @Test
    @DisplayName("Deve registrar apenas os breakers da lista permitida quando breakerNames estiver configurado")
    void filtersBreakersWhenAllowListIsConfigured() {
        properties.setBreakerNames(List.of("orderService"));

        registry.circuitBreaker("orderService");
        registry.circuitBreaker("paymentService");

        new DistributedCircuitBreakerRegistrar(registry, coordinator, properties);

        verify(coordinator).manage("orderService");
        verify(coordinator, never()).manage("paymentService");
    }

    @Test
    @DisplayName("Deve registrar automaticamente breakers adicionados dinamicamente após a inicialização")
    void registersBreakersAddedDynamicallyAfterStartup() {
        new DistributedCircuitBreakerRegistrar(registry, coordinator, properties);

        verifyNoInteractions(coordinator);

        registry.circuitBreaker("dynamicService");

        verify(coordinator).manage("dynamicService");
    }

    @Test
    @DisplayName("Deve ignorar breakers adicionados dinamicamente que não estejam na lista de permitidos")
    void ignoresDynamicallyAddedBreakerIfNotInAllowList() {
        properties.setBreakerNames(List.of("allowedService"));

        new DistributedCircuitBreakerRegistrar(registry, coordinator, properties);

        registry.circuitBreaker("ignoredService");

        verify(coordinator, never()).manage("ignoredService");
    }
}
