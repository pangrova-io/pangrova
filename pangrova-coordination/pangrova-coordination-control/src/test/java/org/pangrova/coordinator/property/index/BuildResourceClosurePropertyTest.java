package org.pangrova.coordinator.property.index;

import org.pangrova.indexing.enums.IndexBuildOutcome;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.connector.api.index.SnapshotPinProvider;
import org.pangrova.connector.api.index.SnapshotPinOwner;
import org.pangrova.connector.api.index.SnapshotPinReference;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.build.IndexBuildContext;
import org.pangrova.indexing.build.IndexBuildCoordinator;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.persistence.indexing.api.build.IndexBuildClaimRepository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Checks the actual builder's claim cleanup and snapshot retention, including failed pinning. */
class BuildResourceClosurePropertyTest {
    @Property(tries = 30)
    void claimReleasedAndSuccessfulSnapshotRetained(
            @ForAll("indexIds") String indexId, @ForAll("generations") int generation) throws Exception {
        var claims = new CountingClaims();
        var pins = new Pins(false);
        try (var builder = builder(claims, pins, true)) {
            assertThat(builder.submitBuildTask(descriptor(indexId, generation), List.of(), null, null)
                    .get(5, TimeUnit.SECONDS).isFullySuccessful()).isTrue();
        }
        assertReleased(claims);
        assertThat(pins.pins.get()).isEqualTo(1);
        // Pins protect the published index and are released by retention/GC, not build completion.
        assertThat(pins.unpins.get()).isZero();
    }

    @Property(tries = 30)
    void claimReleasedAfterPinFailure(
            @ForAll("indexIds") String indexId, @ForAll("generations") int generation) {
        var claims = new CountingClaims();
        var pins = new Pins(true);
        try (var builder = builder(claims, pins, true)) {
            assertThatThrownBy(() -> builder.submitBuildTask(descriptor(indexId, generation), List.of(), null, null)
                    .get(5, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                    .hasRootCauseMessage("pin failed");
        }
        assertReleased(claims);
        assertThat(pins.pins.get()).isEqualTo(1);
        assertThat(pins.unpins.get()).isZero();
    }

    @Property(tries = 30)
    void staleGenerationReleasesClaimWithoutPinning(
            @ForAll("indexIds") String indexId, @ForAll("generations") int generation) throws Exception {
        var claims = new CountingClaims();
        var pins = new Pins(false);
        try (var builder = builder(claims, pins, false)) {
            assertThat(builder.submitBuildTask(descriptor(indexId, generation), List.of(), null, null)
                    .get(5, TimeUnit.SECONDS).outcome()).isEqualTo(IndexBuildOutcome.STALE_GENERATION);
        }
        assertReleased(claims);
        assertThat(pins.pins.get()).isZero();
        assertThat(pins.unpins.get()).isZero();
    }

    private static void assertReleased(CountingClaims claims) {
        assertThat(claims.claims.get()).isEqualTo(1);
        assertThat(claims.releases.get()).isEqualTo(1);
        assertThat(claims.releasedOwner).isEqualTo(claims.owner);
    }

    private static IndexBuildCoordinator builder(CountingClaims claims, Pins pins, boolean current) {
        return new IndexBuildCoordinator(new IndexBuildContext(
                new IndexingConfig("unused-no-file-build", false, null, null), null, null, null,
                claims, (index, generation) -> current, pins, null), null);
    }

    private static IndexDescriptor descriptor(String index, int generation) {
        Instant now = Instant.now();
        return new IndexDescriptor(index, "idx", "cat", "sch", "tbl", List.of("col"), IndexType.HASH,
                Map.of(), IndexState.BUILDING, null, generation, "42", "42", now, now, "tenant");
    }

    @Provide Arbitrary<String> indexIds() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20);
    }

    @Provide Arbitrary<Integer> generations() { return Arbitraries.integers().between(0, 100); }

    private static final class CountingClaims implements IndexBuildClaimRepository {
        private final AtomicInteger claims = new AtomicInteger();
        private final AtomicInteger releases = new AtomicInteger();
        private String owner;
        private String releasedOwner;
        @Override public boolean tryClaim(String index, int generation, String attempt, Instant now) {
            owner = attempt;
            claims.incrementAndGet();
            return true;
        }
        @Override public boolean release(String index, int generation, String attempt) {
            releasedOwner = attempt;
            releases.incrementAndGet();
            return owner.equals(attempt);
        }
        @Override public boolean heartbeat(String index, int generation, String attempt, Instant now) {
            return owner.equals(attempt);
        }
    }

    private static final class Pins implements SnapshotPinProvider {
        private final AtomicInteger pins = new AtomicInteger();
        private final AtomicInteger unpins = new AtomicInteger();
        private final boolean fail;
        private Pins(boolean fail) { this.fail = fail; }
        @Override public void pinSnapshot(String catalog, String schema, String table, String snapshot,
                                          SnapshotPinOwner owner) {
            pins.incrementAndGet();
            if (fail) throw new IllegalStateException("pin failed");
        }
        @Override public void unpinSnapshot(String catalog, String schema, String table, String snapshot,
                                            SnapshotPinOwner owner) {
            unpins.incrementAndGet();
        }
        @Override public void validateSnapshotPin(String catalog, String schema, String table, String snapshot,
                                                  SnapshotPinOwner owner) {}
        @Override public List<String> getPinnedSnapshots(String catalog, String schema, String table) {
            return List.of();
        }
        @Override public List<SnapshotPinReference> getSnapshotPinsForIndex(
                String catalog, String schema, String table, String index) {
            return List.of();
        }
    }
}
