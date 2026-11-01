/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.ai.store.jdbc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.ai.control.AiInfrastructureStores;
import org.pangrova.ai.api.FeatureCatalogStore;
import org.pangrova.ai.api.FeatureCatalogStore.FeatureViewId;
import org.pangrova.ai.api.FeatureCatalogStore.FeatureViewVersion;
import org.pangrova.ai.api.FeatureCatalogStore.FeaturePublication;
import org.pangrova.ai.testinfra.SharedPostgresContainer;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcFeatureCatalogStoreTest {

    private static final Instant NOW = Instant.parse("2026-09-19T01:00:00Z");
    private static final String SHA_A = "a".repeat(64);
    private static final String SHA_B = "b".repeat(64);

    private FeatureCatalogStore store;

    @BeforeEach
    void setUp() throws Exception {
        store = AiInfrastructureStores.jdbc(
                SharedPostgresContainer.migratedDataSource("feature_catalog")).features();
    }

    @Test
    void isolatesTenantsAndPublishesWithCompareAndSet() {
        FeatureViewId tenantA = new FeatureViewId(
                "tenant-a", "lake", "ml", "customer_features");
        FeatureViewId tenantB = new FeatureViewId(
                "tenant-b", "lake", "ml", "customer_features");
        store.createFeatureView(feature(tenantA, 1, SHA_A));
        store.createFeatureView(feature(tenantA, 2, SHA_B));
        store.createFeatureView(feature(tenantB, 1, SHA_A));

        assertThat(store.publishFeatureView(
                tenantA, 1, OptionalLong.empty(), "owner-a", NOW.plusSeconds(1)))
                .hasValueSatisfying(publication -> {
                    assertThat(publication.activeVersion()).isEqualTo(1);
                    assertThat(publication.publicationEpoch()).isEqualTo(1);
                });
        assertThat(store.publishFeatureView(
                tenantA, 2, OptionalLong.of(99), "owner-a", NOW.plusSeconds(2)))
                .isEmpty();
        assertThat(store.publishFeatureView(
                tenantA, 2, OptionalLong.of(1), "owner-a", NOW.plusSeconds(3)))
                .hasValueSatisfying(publication ->
                        assertThat(publication.publicationEpoch()).isEqualTo(2));

        assertThat(store.findFeatureView(tenantA, 2)).isPresent();
        assertThat(store.findFeatureView(tenantB, 2)).isEmpty();
        assertThat(store.publishFeatureView(
                tenantB, 1, OptionalLong.empty(), "owner-b", NOW.plusSeconds(4)))
                .hasValueSatisfying(publication ->
                        assertThat(publication.publicationEpoch()).isEqualTo(1));
    }

    @Test
    void publicationEpochPreventsAbaAndFencesConcurrentPublishers() throws Exception {
        FeatureViewId id = new FeatureViewId(
                "tenant-a", "lake", "ml", "aba_features");
        store.createFeatureView(feature(id, 1, SHA_A));
        store.createFeatureView(feature(id, 2, SHA_B));
        store.createFeatureView(feature(id, 3, "c".repeat(64)));
        assertThat(store.publishFeatureView(
                id, 1, OptionalLong.empty(), "owner", NOW)).isPresent();
        assertThat(store.publishFeatureView(
                id, 2, OptionalLong.of(1), "owner", NOW.plusSeconds(1)))
                .get().extracting(FeaturePublication::publicationEpoch).isEqualTo(2L);
        assertThat(store.publishFeatureView(
                id, 1, OptionalLong.of(2), "owner", NOW.plusSeconds(2)))
                .get().extracting(FeaturePublication::publicationEpoch).isEqualTo(3L);
        assertThat(store.publishFeatureView(
                id, 3, OptionalLong.of(1), "stale", NOW.plusSeconds(3))).isEmpty();

        List<Optional<FeaturePublication>> results = raceFeaturePublications(id, 3);
        assertThat(results).filteredOn(Optional::isPresent).hasSize(1);
        assertThat(results.stream().flatMap(Optional::stream))
                .singleElement()
                .extracting(FeaturePublication::publicationEpoch)
                .isEqualTo(4L);
    }

    private List<Optional<FeaturePublication>> raceFeaturePublications(
            FeatureViewId id, long expectedEpoch) throws Exception {
        var ready = new CountDownLatch(2);
        var start = new CountDownLatch(1);
        var executor = ManagedExecutorFactory.newFixedThreadPool(
                "feature-publication-test", 2);
        try {
            var first = executor.submit(() -> publishAfterBarrier(
                    id, 2, expectedEpoch, ready, start));
            var second = executor.submit(() -> publishAfterBarrier(
                    id, 3, expectedEpoch, ready, start));
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            return List.of(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    private Optional<FeaturePublication> publishAfterBarrier(
            FeatureViewId id, long version, long expectedEpoch,
            CountDownLatch ready, CountDownLatch start) throws Exception {
        ready.countDown();
        assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
        return store.publishFeatureView(
                id, version, OptionalLong.of(expectedEpoch),
                "publisher-" + version, NOW.plusSeconds(version));
    }

    private static FeatureViewVersion feature(
            FeatureViewId id, long version, String definitionSha) {
        String definition = "{\"entity_keys\":[\"customer_id\"],"
                + "\"event_time\":\"event_ts\","
                + "\"source_table\":\"lake.raw.customers\","
                + "\"source_query\":\"SELECT * FROM lake.raw.customers\","
                + "\"transform_version\":\"v1\",\"schema\":{\"customer_id\":\"BIGINT\"},"
                + "\"freshness_target_ms\":60000,\"lineage\":[\"lake.raw.customers\"]}";
        return new FeatureViewVersion(
                id, version, "feature-owner", List.of("customer_id"), "event_ts",
                "lake.raw.customers", "SELECT * FROM lake.raw.customers",
                new byte[]{1, 2, 3}, SHA_A, "v1", "{\"customer_id\":\"BIGINT\"}",
                60_000, "[\"lake.raw.customers\"]", definition, definitionSha,
                NOW.plusSeconds(version));
    }
}
