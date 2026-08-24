package org.pangrova.worker.property.exchange.remote;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.shuffle.ShuffleServerEndpoint;
import org.pangrova.worker.exchange.remote.RemoteShuffleClientPool;
import org.pangrova.worker.exchange.remote.RemoteShuffleExchangeManager;

/**
 * Property-based tests for shuffle replication endpoint selection consistency.
 *
 * <p><b>Validates: Requirements 12.2</b></p>
 */
class ShuffleReplicationConsistencyPropertyTest {

    /**
     * Replication requires at least 2 endpoints. Construction with fewer
     * endpoints must fail fast.
     *
     * <p><b>Validates: Requirements 12.2</b></p>
     */
    @Property(tries = 10)
    void replicationRequiresAtLeastTwoEndpoints(
            @ForAll @IntRange(min = 1, max = 1) int portBase) {

        var pool = new RemoteShuffleClientPool(5000, 5000, 1);
        try {
            List<ShuffleServerEndpoint> singleEndpoint = List.of(
                    new ShuffleServerEndpoint("host-a", 9400 + portBase));

            assertThatThrownBy(() ->
                    new RemoteShuffleExchangeManager(pool, null, singleEndpoint, true))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("at least 2");
        } finally {
            pool.close();
        }
    }

    /**
     * When replication is enabled with N >= 2 endpoints, construction succeeds
     * and the manager is usable.
     *
     * <p><b>Validates: Requirements 12.2</b></p>
     */
    @Property(tries = 50)
    void replicationWithSufficientEndpointsSucceeds(
            @ForAll @IntRange(min = 2, max = 5) int endpointCount) {

        var pool = new RemoteShuffleClientPool(5000, 5000, 1);
        try {
            List<ShuffleServerEndpoint> endpoints = new ArrayList<>();
            for (int i = 0; i < endpointCount; i++) {
                endpoints.add(new ShuffleServerEndpoint("host-" + i, 9400 + i));
            }

            var manager = new RemoteShuffleExchangeManager(
                    pool, null, endpoints, true);
            assertThat(manager).isNotNull();
        } finally {
            pool.close();
        }
    }

    /**
     * When replication is disabled, single endpoint is sufficient.
     *
     * <p><b>Validates: Requirements 12.2</b></p>
     */
    @Property(tries = 10)
    void noReplicationAllowsSingleEndpoint(
            @ForAll @IntRange(min = 1, max = 1) int portBase) {

        var pool = new RemoteShuffleClientPool(5000, 5000, 1);
        try {
            List<ShuffleServerEndpoint> singleEndpoint = List.of(
                    new ShuffleServerEndpoint("host-a", 9400 + portBase));

            var manager = new RemoteShuffleExchangeManager(
                    pool, null, singleEndpoint, false);
            assertThat(manager).isNotNull();
        } finally {
            pool.close();
        }
    }
}
