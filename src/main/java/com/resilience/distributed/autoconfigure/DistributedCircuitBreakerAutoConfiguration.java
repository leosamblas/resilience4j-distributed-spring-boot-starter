package com.resilience.distributed.autoconfigure;

import org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnAvailableEndpoint;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;

import com.resilience.distributed.actuator.DistributedCircuitBreakerEndpoint;
import com.resilience.distributed.core.DistributedCircuitBreakerCoordinator;
import com.resilience.distributed.core.DistributedCircuitBreakerRegistrar;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

/**
 * Configuração automática para compartilhamento distribuído de estado do Resilience4j CircuitBreaker.
 * <p>
 * Ativada automaticamente quando {@code CircuitBreakerRegistry} do Resilience4j e
 * o Spring Data Redis Reactive estiverem presentes no classpath, podendo ser desativada
 * integralmente via {@code resilience4j.distributed.enabled=false}.
 * <p>
 * Se o Redis estiver inacessível em tempo de execução, as chamadas reativas falham
 * silenciosamente por operação (apenas emitindo logs de aviso) — o sistema adota
 * fail-open, degradando graciosamente para o comportamento local do Resilience4j
 * em vez de bloquear o startup ou requisições da aplicação.
 */
@AutoConfiguration
@EnableConfigurationProperties(DistributedCircuitBreakerProperties.class)
@ConditionalOnClass({CircuitBreakerRegistry.class, ReactiveStringRedisTemplate.class})
@ConditionalOnProperty(prefix = "resilience4j.distributed", name = "enabled",
    havingValue = "true", matchIfMissing = true)
public class DistributedCircuitBreakerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ReactiveRedisMessageListenerContainer distributedCircuitBreakerListenerContainer(
            ReactiveRedisConnectionFactory factory) {
        return new ReactiveRedisMessageListenerContainer(factory);
    }

    @Bean
    @ConditionalOnMissingBean
    public ReactiveStringRedisTemplate distributedCircuitBreakerRedisTemplate(
            ReactiveRedisConnectionFactory factory) {
        return new ReactiveStringRedisTemplate(factory);
    }

    @Bean
    public DistributedCircuitBreakerCoordinator distributedCircuitBreakerCoordinator(
            ReactiveStringRedisTemplate redis,
            ReactiveRedisMessageListenerContainer listenerContainer,
            CircuitBreakerRegistry registry,
            DistributedCircuitBreakerProperties properties) {
        return new DistributedCircuitBreakerCoordinator(redis, listenerContainer, registry, properties);
    }

    @Bean
    public DistributedCircuitBreakerRegistrar distributedCircuitBreakerRegistrar(
            CircuitBreakerRegistry registry,
            DistributedCircuitBreakerCoordinator coordinator,
            DistributedCircuitBreakerProperties properties) {
        return new DistributedCircuitBreakerRegistrar(registry, coordinator, properties);
    }

    /**
     * Configuração aninhada e isolada condicionalmente para o endpoint de diagnóstico do Actuator.
     * <p>
     * DEVE ser uma classe estática separada (e não apenas um método @Bean na classe externa)
     * porque {@code @ConditionalOnClass} previne apenas a *instanciação* da classe — a JVM ainda
     * valida referências de bytecode (tipos de retorno, membros de anotações) ao carregar a classe
     * hospedeira. Se o bean do endpoint estivesse diretamente em {@link DistributedCircuitBreakerAutoConfiguration},
     * um serviço consumidor sem spring-boot-starter-actuator no classpath falharia ao carregar toda a
     * autoconfiguração com {@link NoClassDefFoundError}.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.springframework.boot.actuate.endpoint.annotation.Endpoint")
    public static class DistributedCircuitBreakerEndpointConfiguration {

        @Bean
        @ConditionalOnAvailableEndpoint(endpoint = DistributedCircuitBreakerEndpoint.class)
        public DistributedCircuitBreakerEndpoint distributedCircuitBreakerEndpoint(
                CircuitBreakerRegistry registry,
                DistributedCircuitBreakerCoordinator coordinator) {
            return new DistributedCircuitBreakerEndpoint(registry, coordinator);
        }
    }
}
