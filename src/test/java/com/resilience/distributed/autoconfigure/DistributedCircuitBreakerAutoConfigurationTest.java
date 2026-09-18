package com.resilience.distributed.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;

import com.resilience.distributed.actuator.DistributedCircuitBreakerEndpoint;
import com.resilience.distributed.core.DistributedCircuitBreakerCoordinator;
import com.resilience.distributed.core.DistributedCircuitBreakerRegistrar;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

@DisplayName("Testes de Autoconfiguração do Spring Boot (DistributedCircuitBreakerAutoConfiguration)")
class DistributedCircuitBreakerAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(DistributedCircuitBreakerAutoConfiguration.class))
        .withBean(ReactiveRedisConnectionFactory.class, () -> mock(ReactiveRedisConnectionFactory.class))
        .withBean(CircuitBreakerRegistry.class, CircuitBreakerRegistry::ofDefaults);

    @Test
    @DisplayName("Deve registrar todos os beans do starter por padrão no contexto do Spring")
    void autoConfigurationIsActiveByDefault() {
        contextRunner
            .withPropertyValues("management.endpoints.web.exposure.include=distributedCircuitBreakers")
            .run(context -> {
                assertThat(context).hasSingleBean(DistributedCircuitBreakerProperties.class);
                assertThat(context).hasSingleBean(DistributedCircuitBreakerCoordinator.class);
                assertThat(context).hasSingleBean(DistributedCircuitBreakerRegistrar.class);
                assertThat(context).hasSingleBean(DistributedCircuitBreakerEndpoint.class);
            });
    }

    @Test
    @DisplayName("Não deve criar os beans quando resilience4j.distributed.enabled for false")
    void autoConfigurationBacksOffWhenDisabled() {
        contextRunner
            .withPropertyValues("resilience4j.distributed.enabled=false")
            .run(context -> {
                assertThat(context).doesNotHaveBean(DistributedCircuitBreakerCoordinator.class);
                assertThat(context).doesNotHaveBean(DistributedCircuitBreakerRegistrar.class);
            });
    }

    @Test
    @DisplayName("Não deve carregar o endpoint do Actuator caso a biblioteca do Actuator não esteja no classpath")
    void endpointBacksOffWhenActuatorIsNotOnClasspath() {
        contextRunner
            .withClassLoader(new FilteredClassLoader("org.springframework.boot.actuate.endpoint.annotation.Endpoint"))
            .run(context -> {
                assertThat(context).hasSingleBean(DistributedCircuitBreakerCoordinator.class);
                assertThat(context).doesNotHaveBean(DistributedCircuitBreakerEndpoint.class);
            });
    }
}
