package com.resilience.distributed;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Testes das Propriedades de Configuração (DistributedCircuitBreakerProperties)")
class DistributedCircuitBreakerPropertiesTest {

    @Test
    @DisplayName("Deve inicializar com valores padrão seguros para ambiente de produção")
    void defaultsAreSafeForProductionUse() {
        DistributedCircuitBreakerProperties props = new DistributedCircuitBreakerProperties();

        assertThat(props.isEnabled()).isTrue();
        assertThat(props.getNamespace()).isEqualTo("default");
        assertThat(props.getBreakerNames()).isEmpty();

        // O TTL deve exceder confortavelmente os valores típicos de waitDurationInOpenState
        // (geralmente 10-60s), já que não há job de atualização contínua em segundo plano.
        assertThat(props.getStateTtl()).isGreaterThanOrEqualTo(Duration.ofMinutes(1));

        // O jitter deve ser positivo porém contido, para não retardar excessivamente a recuperação
        // e simultaneamente evitar sondagens em manada (thundering herd).
        assertThat(props.getMaxJitter()).isPositive();
        assertThat(props.getMaxJitter()).isLessThanOrEqualTo(Duration.ofSeconds(10));

        // Intervalo de throttling de renovação de TTL tem como padrão 5s
        assertThat(props.getTtlRefreshInterval()).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("Deve permitir a mutação e correta atribuição de todos os campos via setters")
    void settersAreMutable() {
        DistributedCircuitBreakerProperties props = new DistributedCircuitBreakerProperties();

        props.setEnabled(false);
        props.setNamespace("staging");
        props.setStateTtl(Duration.ofMinutes(10));
        props.setTtlRefreshInterval(Duration.ofSeconds(15));
        props.setMaxJitter(Duration.ofSeconds(1));
        props.setBreakerNames(java.util.List.of("orderService"));

        assertThat(props.isEnabled()).isFalse();
        assertThat(props.getNamespace()).isEqualTo("staging");
        assertThat(props.getStateTtl()).isEqualTo(Duration.ofMinutes(10));
        assertThat(props.getTtlRefreshInterval()).isEqualTo(Duration.ofSeconds(15));
        assertThat(props.getMaxJitter()).isEqualTo(Duration.ofSeconds(1));
        assertThat(props.getBreakerNames()).containsExactly("orderService");
    }
}
