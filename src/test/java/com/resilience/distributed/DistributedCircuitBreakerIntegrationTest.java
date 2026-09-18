package com.resilience.distributed;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.DisplayName;
import com.resilience.distributed.autoconfigure.DistributedCircuitBreakerProperties;
import com.resilience.distributed.core.DistributedCircuitBreakerCoordinator;

@Testcontainers
@DisplayName("Testes de Integração Ponta a Ponta com Redis Real (Testcontainers)")
class DistributedCircuitBreakerIntegrationTest {

    @Container
    private static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
        .withExposedPorts(6379);

    private static LettuceConnectionFactory connectionFactory;
    private static ReactiveStringRedisTemplate redisTemplate;
    private static ReactiveRedisMessageListenerContainer listenerContainer1;
    private static ReactiveRedisMessageListenerContainer listenerContainer2;

    @BeforeAll
    static void initRedis() {
        REDIS.start();

        connectionFactory = new LettuceConnectionFactory(REDIS.getHost(), REDIS.getFirstMappedPort());
        connectionFactory.afterPropertiesSet();

        redisTemplate = new ReactiveStringRedisTemplate(connectionFactory);

        listenerContainer1 = new ReactiveRedisMessageListenerContainer(connectionFactory);
        listenerContainer2 = new ReactiveRedisMessageListenerContainer(connectionFactory);
    }

    @AfterAll
    static void tearDown() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
        if (REDIS != null && REDIS.isRunning()) {
            REDIS.stop();
        }
    }

    @Test
    @DisplayName("Deve propagar transições de estado OPEN e CLOSED entre dois pods distintos via Redis real")
    void stateTransitionsArePropagatedBetweenTwoIndependentPodsViaRealRedis() throws Exception {
        DistributedCircuitBreakerProperties props = new DistributedCircuitBreakerProperties();
        props.setNamespace("integration-test");
        props.setMaxJitter(Duration.ofMillis(50));
        props.setStateTtl(Duration.ofMinutes(1));

        CircuitBreakerRegistry registry1 = CircuitBreakerRegistry.ofDefaults();
        CircuitBreakerRegistry registry2 = CircuitBreakerRegistry.ofDefaults();

        DistributedCircuitBreakerCoordinator pod1 = new DistributedCircuitBreakerCoordinator(
            redisTemplate, listenerContainer1, registry1, props);
        DistributedCircuitBreakerCoordinator pod2 = new DistributedCircuitBreakerCoordinator(
            redisTemplate, listenerContainer2, registry2, props);

        pod1.manage("paymentService");
        pod2.manage("paymentService");

        CircuitBreaker cb1 = registry1.circuitBreaker("paymentService");
        CircuitBreaker cb2 = registry2.circuitBreaker("paymentService");

        assertThat(cb1.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(cb2.getState()).isEqualTo(CircuitBreaker.State.CLOSED);

        // Aguarda estabelecimento das subscrições pub/sub do Redis
        Thread.sleep(500);

        // Pod 1 sofre falhas no serviço externo e abre o circuito
        cb1.transitionToOpenState();
        assertThat(cb1.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        // Pod 2 deve receber o sinal remoto de OPEN e forçar seu circuito local para OPEN
        long deadline = System.currentTimeMillis() + 5000;
        while (cb2.getState() != CircuitBreaker.State.OPEN && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }

        assertThat(cb2.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        // Pod 1 transiciona para CLOSED (recuperação detectada)
        cb1.transitionToClosedState();

        // Pod 2 deve seguir para HALF_OPEN aplicando o jitter aleatório
        deadline = System.currentTimeMillis() + 5000;
        while (cb2.getState() != CircuitBreaker.State.HALF_OPEN && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }

        assertThat(cb2.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);

        pod1.destroy();
        pod2.destroy();
    }
}
