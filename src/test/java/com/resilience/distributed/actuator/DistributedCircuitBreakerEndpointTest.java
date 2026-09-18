package com.resilience.distributed.actuator;
 
 import com.resilience.distributed.core.DistributedCircuitBreakerCoordinator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
@DisplayName("Testes do Endpoint do Actuator (DistributedCircuitBreakerEndpoint)")
class DistributedCircuitBreakerEndpointTest {

    @Mock
    private CircuitBreakerRegistry registry;

    @Mock
    private DistributedCircuitBreakerCoordinator coordinator;

    @Mock
    private CircuitBreaker circuitBreaker;

    private DistributedCircuitBreakerEndpoint endpoint;

    @BeforeEach
    void setUp() {
        endpoint = new DistributedCircuitBreakerEndpoint(registry, coordinator);
    }

    @Test
    @DisplayName("Deve retornar managed=false com nota explicativa quando o circuit breaker não for gerenciado")
    void breaker_returnsNotManaged_whenBreakerIsNotInCoordinator() {
        when(coordinator.getManagedBreakerNames()).thenReturn(Set.of("orderService"));

        Map<String, Object> result = endpoint.breaker("unknownService").block();

        assertThat(result).isNotNull();
        assertThat(result.get("breakerName")).isEqualTo("unknownService");
        assertThat(result.get("managed")).isEqualTo(false);
        assertThat(result.get("note")).isNotNull();
    }

    @Test
    @DisplayName("Deve retornar inSyncWithRemote=true quando os estados local e remoto coincidirem")
    void breaker_returnsStateAndSyncStatus_whenInSync() {
        when(coordinator.getManagedBreakerNames()).thenReturn(Set.of("orderService"));
        when(coordinator.getInstanceId()).thenReturn("pod-123");
        when(registry.circuitBreaker("orderService")).thenReturn(circuitBreaker);
        when(circuitBreaker.getState()).thenReturn(CircuitBreaker.State.OPEN);
        when(coordinator.peekRemoteState("orderService")).thenReturn(Mono.just("remote-pod-456|OPEN"));

        Map<String, Object> result = endpoint.breaker("orderService").block();

        assertThat(result).isNotNull();
        assertThat(result.get("breakerName")).isEqualTo("orderService");
        assertThat(result.get("localState")).isEqualTo("OPEN");
        assertThat(result.get("localInstanceId")).isEqualTo("pod-123");
        assertThat(result.get("remoteState")).isEqualTo("OPEN");
        assertThat(result.get("remoteInstanceId")).isEqualTo("remote-pod-456");
        assertThat(result.get("inSyncWithRemote")).isEqualTo(true);
    }

    @Test
    @DisplayName("Deve retornar inSyncWithRemote=false quando os estados local e remoto divergirem")
    void breaker_returnsInSyncFalse_whenStatesDiffer() {
        when(coordinator.getManagedBreakerNames()).thenReturn(Set.of("orderService"));
        when(coordinator.getInstanceId()).thenReturn("pod-123");
        when(registry.circuitBreaker("orderService")).thenReturn(circuitBreaker);
        when(circuitBreaker.getState()).thenReturn(CircuitBreaker.State.CLOSED);
        when(coordinator.peekRemoteState("orderService")).thenReturn(Mono.just("remote-pod-456|OPEN"));

        Map<String, Object> result = endpoint.breaker("orderService").block();

        assertThat(result).isNotNull();
        assertThat(result.get("localState")).isEqualTo("CLOSED");
        assertThat(result.get("remoteState")).isEqualTo("OPEN");
        assertThat(result.get("inSyncWithRemote")).isEqualTo(false);
    }

    @Test
    @DisplayName("Deve tratar graciosamente a ausência de estado compartilhado no Redis")
    void breaker_handlesEmptyRemoteStateGracefully() {
        when(coordinator.getManagedBreakerNames()).thenReturn(Set.of("orderService"));
        when(coordinator.getInstanceId()).thenReturn("pod-123");
        when(registry.circuitBreaker("orderService")).thenReturn(circuitBreaker);
        when(circuitBreaker.getState()).thenReturn(CircuitBreaker.State.CLOSED);
        when(coordinator.peekRemoteState("orderService")).thenReturn(Mono.empty());

        Map<String, Object> result = endpoint.breaker("orderService").block();

        assertThat(result).isNotNull();
        assertThat(result.get("localState")).isEqualTo("CLOSED");
        assertThat(result.get("remoteState")).isNull();
        assertThat(result.get("note")).isNotNull();
    }

    @Test
    @DisplayName("Deve retornar todos os breakers gerenciados com o identificador local da instância")
    @SuppressWarnings("unchecked")
    void allBreakers_returnsLocalInstanceIdAndAllManagedBreakers() {
        when(coordinator.getInstanceId()).thenReturn("pod-123");
        when(coordinator.getManagedBreakerNames()).thenReturn(Set.of("orderService"));
        when(registry.circuitBreaker("orderService")).thenReturn(circuitBreaker);
        when(circuitBreaker.getState()).thenReturn(CircuitBreaker.State.CLOSED);
        when(coordinator.peekRemoteState("orderService")).thenReturn(Mono.just("pod-999|CLOSED"));

        Map<String, Object> result = endpoint.allBreakers().block();

        assertThat(result).isNotNull();
        assertThat(result.get("localInstanceId")).isEqualTo("pod-123");
        assertThat(result.get("breakers")).isInstanceOf(Map.class);

        Map<String, Object> breakers = (Map<String, Object>) result.get("breakers");
        assertThat(breakers).containsKey("orderService");
    }

    @Test
    @DisplayName("Deve retornar mapa vazio de breakers quando nenhum circuit breaker for gerenciado")
    void allBreakers_whenNoBreakersManaged_returnsEmptyBreakersMap() {
        when(coordinator.getInstanceId()).thenReturn("pod-123");
        when(coordinator.getManagedBreakerNames()).thenReturn(Collections.emptySet());

        Map<String, Object> result = endpoint.allBreakers().block();

        assertThat(result).isNotNull();
        assertThat(result.get("localInstanceId")).isEqualTo("pod-123");
        assertThat(result.get("breakers")).isEqualTo(Collections.emptyMap());
    }
}
