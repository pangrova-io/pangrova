package org.pangrova.coordinator.query.execution;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.security.authz.Privilege;
import org.pangrova.spi.security.UserIdentity;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual SDK allocation, deterministic scheduling gates; no Native output or compensation is fabricated. */
class QueryWriteReservationConcurrencyTest {
    @TempDir Path warehouse;

    @Test
    void concurrentRegistrationsObserveAuthorityTwiceAndAllocateOnlyOnce() throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var authorities = new CountDownLatch(2);
            fixture.metadata.beforeInsert = () -> waitAtSdkBoundary(entered, release);
            fixture.engine.setGeneratedWriteAuthoritySupplier(request -> {
                fixture.observations.add(request);
                authorities.countDown();
                return fixture.receipt(request);
            });
            var executor = Executors.newFixedThreadPool(2);
            try {
                var context = context(fixture, "INSERT", Map.of());
                var first = executor.submit(() -> fixture.engine.ensureWriteContextRegistered(actor(), context));
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                var second = executor.submit(() -> fixture.engine.ensureWriteContextRegistered(actor(), context));
                assertThat(authorities.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(fixture.tracker.hasWriteContext("reservation-query")).isFalse();
                release.countDown();
                first.get(10, TimeUnit.SECONDS);
                second.get(10, TimeUnit.SECONDS);
                assertThat(fixture.metadata.inserts).isEqualTo(1);
                assertThat(fixture.observations).hasSize(2);
                assertThat(fixture.tracker.hasWriteContext("reservation-query")).isTrue();
            } finally {
                release.countDown();
                executor.shutdownNow();
                assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @Test
    void cancelledSdkWinnerCannotPublishOrSilentlyReallocateItsKnownHandle() throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            fixture.installOrdinaryAuthority();
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            fixture.metadata.beforeInsert = () -> waitAtSdkBoundary(entered, release);
            var executor = Executors.newSingleThreadExecutor();
            try {
                var context = context(fixture, "INSERT", Map.of());
                var winner = executor.submit(() -> fixture.engine.ensureWriteContextRegistered(actor(), context));
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                fixture.tracker.clearWriteState("reservation-query");
                release.countDown();
                assertThatThrownBy(() -> winner.get(10, TimeUnit.SECONDS))
                        .rootCause().isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("no longer active");
                assertThat(fixture.tracker.hasWriteContext("reservation-query")).isFalse();
                assertThat(fixture.tracker.unpublishedAllocation("reservation-query")).isPresent();
                assertThatThrownBy(() -> fixture.engine.ensureWriteContextRegistered(actor(), context))
                        .isInstanceOf(IllegalStateException.class);
                assertThat(fixture.metadata.inserts).isEqualTo(1);
            } finally {
                release.countDown();
                executor.shutdownNow();
                assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @Test
    void failedBeginWithNoOutputReleasesReservationForAnActualRetry() throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            fixture.installOrdinaryAuthority();
            fixture.metadata.failOverwrite = true;
            var context = context(fixture, "OVERWRITE", Map.of());
            assertThatThrownBy(() -> fixture.engine.ensureWriteContextRegistered(actor(), context))
                    .rootCause().hasMessageContaining("allocation boundary rejected overwrite");
            fixture.metadata.failOverwrite = false;

            fixture.engine.ensureWriteContextRegistered(actor(), context);

            assertThat(fixture.metadata.overwrites).isEqualTo(2);
            assertThat(fixture.tracker.hasWriteContext("reservation-query")).isTrue();
            assertThat(fixture.tracker.unpublishedAllocation("reservation-query")).isEmpty();
        }
    }

    @Test
    void sameAuthorityCannotChangeAppendToOverwriteOrStaticPartitionValues() throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            fixture.installOrdinaryAuthority();
            fixture.engine.ensureWriteContextRegistered(actor(), context(fixture, "INSERT", Map.of()));
            assertThatThrownBy(() -> fixture.engine.ensureWriteContextRegistered(actor(), context(fixture, "OVERWRITE", Map.of())))
                    .rootCause().hasMessageContaining("different authority or declaration");
            assertThat(fixture.metadata.inserts).isEqualTo(1);
            assertThat(fixture.metadata.overwrites).isZero();
            fixture.tracker.clearWriteState("reservation-query");
            fixture.engine.ensureWriteContextRegistered(actor(), context(fixture, "OVERWRITE", Map.of("day", "a")));
            assertThatThrownBy(() -> fixture.engine.ensureWriteContextRegistered(actor(), context(fixture, "OVERWRITE", Map.of("day", "b"))))
                    .rootCause().hasMessageContaining("different authority or declaration");
            assertThat(fixture.metadata.overwrites).isEqualTo(1);
        }
    }

    @Test
    void partitionOrderDoesNotChangeTheCanonicalIntentOrReallocate() throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            fixture.installOrdinaryAuthority();
            var proof = Optional.of(new WritePolicyObservation(List.of(), Set.of(Privilege.INSERT), fixture.declaration()));
            fixture.engine.ensureWriteContextRegistered(actor(), new DmlWriteContext("catalog.schema.documents", "OVERWRITE",
                    List.of("day=a", "region=b"), proof));

            fixture.engine.ensureWriteContextRegistered(actor(), new DmlWriteContext("catalog.schema.documents", "OVERWRITE",
                    List.of("region=b", "day=a"), proof));

            assertThat(fixture.metadata.overwrites).isEqualTo(1);
            assertThat(fixture.observations).hasSize(2);
        }
    }

    private static DmlWriteContext context(SchedulingWriteFixture fixture, String mode, Map<String, String> partitions) {
        return new DmlWriteContext("catalog.schema.documents", mode,
                partitions.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue()).toList(),
                Optional.of(new WritePolicyObservation(List.of(), Set.of(Privilege.INSERT), fixture.declaration())));
    }

    private static CoordinatorSessionContext actor() {
        return new CoordinatorSessionContext("reservation-query", Map.of(),
                new UserIdentity("alice", Set.of(), Map.of(), Instant.EPOCH), "tenant-1");
    }

    private static void waitAtSdkBoundary(CountDownLatch entered, CountDownLatch release) {
        entered.countDown();
        try {
            if (!release.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("SDK fixture gate timed out");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("SDK fixture gate interrupted", interrupted);
        }
    }
}
