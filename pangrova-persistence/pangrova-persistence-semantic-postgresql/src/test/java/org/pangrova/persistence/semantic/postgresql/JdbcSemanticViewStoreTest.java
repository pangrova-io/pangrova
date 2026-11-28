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
package org.pangrova.persistence.semantic.postgresql;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.concurrency.ManagedExecutorFactory;
import org.pangrova.semantic.api.exception.SemanticStoreException;
import org.pangrova.semantic.api.identity.SemanticViewId;
import org.pangrova.semantic.api.publication.SemanticViewPublication;
import org.pangrova.semantic.api.version.SemanticViewStore;
import org.pangrova.semantic.api.enums.SemanticViewValidationState;
import org.pangrova.semantic.api.version.SemanticViewVersion;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcSemanticViewStoreTest {

    private static final Instant NOW = Instant.parse("2026-09-19T00:00:00Z");
    private static final SemanticViewId VIEW =
            new SemanticViewId("tenant-a", "lake", "sales", "revenue");

    private SemanticViewStore store;

    @BeforeEach
    void setUp() throws Exception {
        var dataSource = SemanticPostgresContainer.migratedDataSource("semantic_view");
        store = new JdbcSemanticViewStore(dataSource);
    }

    @Test
    void storesImmutableVersionsAndPublishesWithCompareAndSet() {
        store.createVersion(version(1, SemanticViewValidationState.VALIDATED, null));
        store.createVersion(version(2, SemanticViewValidationState.INVALID, "unknown measure"));
        store.createVersion(version(3, SemanticViewValidationState.VALIDATED, null));

        assertThat(store.findVersion(VIEW, 1).orElseThrow())
                .satisfies(stored -> {
                    assertThat(stored.id()).isEqualTo(VIEW);
                    assertThat(stored.version()).isEqualTo(1);
                    assertThat(stored.definitionJson()).contains("logicalTables", "orders");
                    assertThat(stored.validationState())
                            .isEqualTo(SemanticViewValidationState.VALIDATED);
                });
        assertThat(store.listVersions(VIEW))
                .extracting(SemanticViewVersion::version)
                .containsExactly(1L, 2L, 3L);

        assertThat(store.publishVersion(
                VIEW, 1, OptionalLong.empty(), "release-owner", NOW.plusSeconds(1)))
                .hasValueSatisfying(publication -> {
                    assertThat(publication.activeVersion()).isEqualTo(1);
                    assertThat(publication.publicationEpoch()).isEqualTo(1);
                });
        assertThat(store.publishVersion(
                VIEW, 3, OptionalLong.empty(), "racing-owner", NOW.plusSeconds(2)))
                .isEmpty();
        assertThat(store.publishVersion(
                VIEW, 3, OptionalLong.of(99), "stale-owner", NOW.plusSeconds(2)))
                .isEmpty();

        assertThatThrownBy(() -> store.publishVersion(
                VIEW, 2, OptionalLong.of(1), "release-owner", NOW.plusSeconds(2)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid Semantic View");

        assertThat(store.publishVersion(
                VIEW, 3, OptionalLong.of(1), "release-owner", NOW.plusSeconds(3)))
                .hasValueSatisfying(publication ->
                        assertThat(publication.publicationEpoch()).isEqualTo(2));
        assertThat(store.findActiveVersion(VIEW).orElseThrow())
                .satisfies(active -> {
                    assertThat(active.version()).isEqualTo(3);
                    assertThat(active.validationState())
                            .isEqualTo(SemanticViewValidationState.VALIDATED);
                });
        assertThat(store.findPublication(VIEW).orElseThrow())
                .satisfies(publication -> {
                    assertThat(publication.activeVersion()).isEqualTo(3);
                    assertThat(publication.publicationEpoch()).isEqualTo(2);
                    assertThat(publication.publishedBy()).isEqualTo("release-owner");
                });
        assertThat(store.listVersionStatuses("tenant-a", "lake", "sales", 10))
                .extracting(status -> List.of(
                        status.version().id().viewName(),
                        status.version().version(),
                        status.active(),
                        status.activeVersion().orElseThrow(),
                        status.publicationEpoch()))
                .containsExactly(
                        List.of("revenue", 3L, true, 3L, 2L),
                        List.of("revenue", 2L, false, 3L, 2L),
                        List.of("revenue", 1L, false, 3L, 2L));
    }

    @Test
    void publicationEpochPreventsAbaAndFencesConcurrentPublishers() throws Exception {
        store.createVersion(version(1, SemanticViewValidationState.VALIDATED, null));
        store.createVersion(version(3, SemanticViewValidationState.VALIDATED, null));
        store.createVersion(version(4, SemanticViewValidationState.VALIDATED, null));
        assertThat(store.publishVersion(
                VIEW, 1, OptionalLong.empty(), "owner", NOW)).isPresent();
        assertThat(store.publishVersion(
                VIEW, 3, OptionalLong.of(1), "owner", NOW.plusSeconds(1)))
                .get().extracting(SemanticViewPublication::publicationEpoch).isEqualTo(2L);
        assertThat(store.publishVersion(
                VIEW, 1, OptionalLong.of(2), "owner", NOW.plusSeconds(2)))
                .get().extracting(SemanticViewPublication::publicationEpoch).isEqualTo(3L);
        assertThat(store.publishVersion(
                VIEW, 4, OptionalLong.of(1), "stale", NOW.plusSeconds(3))).isEmpty();

        List<Optional<SemanticViewPublication>> results = racePublications(3);
        assertThat(results).filteredOn(Optional::isPresent).hasSize(1);
        assertThat(results.stream().flatMap(Optional::stream))
                .singleElement()
                .extracting(SemanticViewPublication::publicationEpoch)
                .isEqualTo(4L);
    }

    private List<Optional<SemanticViewPublication>> racePublications(
            long expectedEpoch) throws Exception {
        var ready = new CountDownLatch(2);
        var start = new CountDownLatch(1);
        var executor = ManagedExecutorFactory.newFixedThreadPool(
                "semantic-publication-test", 2);
        try {
            var first = executor.submit(() -> publishAfterBarrier(
                    3, expectedEpoch, ready, start));
            var second = executor.submit(() -> publishAfterBarrier(
                    4, expectedEpoch, ready, start));
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            return List.of(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    private Optional<SemanticViewPublication> publishAfterBarrier(
            long version, long expectedEpoch,
            CountDownLatch ready, CountDownLatch start) throws Exception {
        ready.countDown();
        assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
        return store.publishVersion(
                VIEW, version, OptionalLong.of(expectedEpoch),
                "publisher-" + version, NOW.plusSeconds(version));
    }

    @Test
    void duplicateVersionsFailAndTenantIdentityDoesNotLeak() {
        SemanticViewVersion version = version(
                1, SemanticViewValidationState.VALIDATED, null);
        store.createVersion(version);

        assertThatThrownBy(() -> store.createVersion(version))
                .isInstanceOf(SemanticStoreException.class);
        assertThat(store.findVersion(
                new SemanticViewId("tenant-b", "lake", "sales", "revenue"), 1))
                .isEmpty();
    }

    private static SemanticViewVersion version(
            long version,
            SemanticViewValidationState state,
            String validationMessage) {
        return new SemanticViewVersion(
                VIEW,
                version,
                "semantic-owner",
                "{\"logicalTables\":[\"orders\"],\"version\":" + version + "}",
                Long.toHexString(version).repeat(64).substring(0, 64),
                state,
                validationMessage,
                NOW.plusSeconds(version));
    }
}
