package com.resilience.distributed;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Endpoint reativo não-bloqueante do Actuator para inspeção do estado dos circuit breakers distribuídos,
 * exposto em {@code /actuator/distributedCircuitBreakers} (e
 * {@code /actuator/distributedCircuitBreakers/{name}} para um circuit breaker individual).
 * <p>
 * Completamente não-bloqueante: retorna {@link Mono} para garantir compatibilidade
 * com o Spring WebFlux (threads de I/O do Netty) e Spring MVC.
 * <p>
 * Deliberadamente somente leitura — forçar abertura/fechamento de circuit breaker
 * via HTTP em ambiente produtivo não é suportado por segurança.
 */
@Endpoint(id = "distributedCircuitBreakers")
public class DistributedCircuitBreakerEndpoint {

    public static final String KEY_LOCAL_INSTANCE_ID = "localInstanceId";
    public static final String KEY_BREAKERS = "breakers";
    public static final String KEY_BREAKER_NAME = "breakerName";
    public static final String KEY_MANAGED = "managed";
    public static final String KEY_NOTE = "note";
    public static final String KEY_LOCAL_STATE = "localState";
    public static final String KEY_REMOTE_STATE = "remoteState";
    public static final String KEY_REMOTE_INSTANCE_ID = "remoteInstanceId";
    public static final String KEY_IN_SYNC_WITH_REMOTE = "inSyncWithRemote";

    private final CircuitBreakerRegistry registry;
    private final DistributedCircuitBreakerCoordinator coordinator;

    public DistributedCircuitBreakerEndpoint(CircuitBreakerRegistry registry,
                                              DistributedCircuitBreakerCoordinator coordinator) {
        this.registry = registry;
        this.coordinator = coordinator;
    }

    /** Lista todos os circuit breakers distribuídos comparando seus estados locais versus remotos (Redis). */
    @ReadOperation
    public Mono<Map<String, Object>> allBreakers() {
        Set<String> names = coordinator.getManagedBreakerNames();
        if (names.isEmpty()) {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put(KEY_LOCAL_INSTANCE_ID, coordinator.getInstanceId());
            result.put(KEY_BREAKERS, Collections.emptyMap());
            return Mono.just(result);
        }

        return Flux.fromIterable(names)
            .flatMap(this::describe)
            .collectList()
            .map(list -> {
                Map<String, Object> result = new LinkedHashMap<>();
                result.put(KEY_LOCAL_INSTANCE_ID, coordinator.getInstanceId());

                Map<String, Object> breakers = new LinkedHashMap<>();
                for (Map<String, Object> item : list) {
                    breakers.put((String) item.get(KEY_BREAKER_NAME), item);
                }
                result.put(KEY_BREAKERS, breakers);
                return result;
            });
    }

    /** Detalhes de um único circuit breaker pelo nome. */
    @ReadOperation
    public Mono<Map<String, Object>> breaker(@Selector String name) {
        if (!coordinator.getManagedBreakerNames().contains(name)) {
            Map<String, Object> notManaged = new LinkedHashMap<>();
            notManaged.put(KEY_BREAKER_NAME, name);
            notManaged.put(KEY_MANAGED, false);
            notManaged.put(KEY_NOTE, "Este circuit breaker não está sob distribuição. "
                + "Verifique resilience4j.distributed.breaker-names, ou confirme se o "
                + "nome está grafado exatamente como em resilience4j.circuitbreaker.instances.");
            return Mono.just(notManaged);
        }
        return describe(name);
    }

    private Mono<Map<String, Object>> describe(String name) {
        CircuitBreaker.State localState = registry.circuitBreaker(name).getState();
        String localInstanceId = coordinator.getInstanceId();

        return coordinator.peekRemoteState(name)
            .map(remotePayload -> buildDetails(name, localState.name(), localInstanceId, remotePayload))
            .defaultIfEmpty(buildDetails(name, localState.name(), localInstanceId, null));
    }

    private Map<String, Object> buildDetails(String name, String localState, String localInstanceId, String remotePayload) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put(KEY_BREAKER_NAME, name);
        details.put(KEY_LOCAL_STATE, localState);
        details.put(KEY_LOCAL_INSTANCE_ID, localInstanceId);

        if (remotePayload == null) {
            details.put(KEY_REMOTE_STATE, null);
            details.put(KEY_REMOTE_INSTANCE_ID, null);
            details.put(KEY_NOTE, "Nenhum estado compartilhado encontrado no Redis para este breaker "
                + "(expirado, nunca gravado ou Redis indisponível).");
            return details;
        }

        String[] parts = remotePayload.split("\\|", 2);
        String remoteInstanceId = parts.length > 0 ? parts[0] : null;
        String remoteState = parts.length > 1 ? parts[1] : null;

        details.put(KEY_REMOTE_STATE, remoteState);
        details.put(KEY_REMOTE_INSTANCE_ID, remoteInstanceId);
        details.put(KEY_IN_SYNC_WITH_REMOTE, remoteState != null && remoteState.equals(localState));

        return details;
    }
}
