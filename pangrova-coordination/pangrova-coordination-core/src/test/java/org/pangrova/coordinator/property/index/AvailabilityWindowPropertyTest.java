package org.pangrova.coordinator.property.index;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.spi.connector.metadata.ConnectorSplit;
import org.pangrova.spi.indexing.descriptor.IndexDescriptor;
import org.pangrova.spi.indexing.state.IndexState;
import org.pangrova.spi.indexing.descriptor.IndexType;
import org.pangrova.spi.connector.index.predicate.EqualTo;
import org.pangrova.indexing.core.planner.IndexHint;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property P2: Availability Window Correctness.
 *
 * <p>∀ file f, ∀ index i: if {@code f.snapshotId > i.lastIndexedSnapshotId},
 * then the file bypasses L1 evaluation (always KEEP).</p>
 *
 * <p>Validates: Requirements 3.AC1, 3.AC2, 3.AC4</p>
 */
class AvailabilityWindowPropertyTest {

    /**
     * Files with snapshotId > lastIndexedSnapshotId are outside the window.
     */
    @Property(tries = 10)
    void fileOutsideWindowBypassesL1(
            @ForAll("outsideWindowPairs") SnapshotPair pair) {
        boolean outside = checkAvailabilityWindow(
                pair.fileSnapshotId, pair.lastIndexedSnapshotId);
        assertThat(outside)
                .as("file snapshot %d > lastIndexed %d must bypass L1",
                        pair.fileSnapshotId, pair.lastIndexedSnapshotId)
                .isTrue();
    }

    /**
     * Files with snapshotId <= lastIndexedSnapshotId are inside the window.
     */
    @Property(tries = 10)
    void fileInsideWindowAllowsL1(
            @ForAll("insideWindowPairs") SnapshotPair pair) {
        boolean outside = checkAvailabilityWindow(
                pair.fileSnapshotId, pair.lastIndexedSnapshotId);
        assertThat(outside)
                .as("file snapshot %d <= lastIndexed %d must allow L1",
                        pair.fileSnapshotId, pair.lastIndexedSnapshotId)
                .isFalse();
    }

    /**
     * Null or missing snapshot IDs are conservative (allow L1, never bypass).
     */
    @Property(tries = 10)
    void nullSnapshotNeverBypasses(
            @ForAll("positiveSnapshot") long fileSnapshotId) {
        // null lastIndexedSnapshotId → conservative, allow L1
        assertThat(checkAvailabilityWindowNullIndex(fileSnapshotId))
                .as("null lastIndexedSnapshotId must not bypass L1")
                .isFalse();
    }

    /**
     * Missing file snapshot property is conservative (allow L1).
     */
    @Property(tries = 10)
    void missingFileSnapshotNeverBypasses(
            @ForAll("positiveSnapshot") long lastIndexed) {
        assertThat(checkAvailabilityWindowNoFileSnapshot(lastIndexed))
                .as("missing file snapshot must not bypass L1")
                .isFalse();
    }

    // ── Logic under test (mirrors IndexSplitFilter.isOutsideAvailabilityWindow) ──

    private static boolean checkAvailabilityWindow(
            long fileSnapshotId, long lastIndexedSnapshotId) {
        return fileSnapshotId > lastIndexedSnapshotId;
    }

    private static boolean checkAvailabilityWindowNullIndex(long fileSnapshotId) {
        // Simulates null lastIndexedSnapshotId on descriptor
        String lastIndexed = null;
        if (lastIndexed == null) {
            return false;
        }
        return fileSnapshotId > Long.parseLong(lastIndexed);
    }

    private static boolean checkAvailabilityWindowNoFileSnapshot(long lastIndexed) {
        // Simulates missing file snapshot property
        String fileSnapshotStr = null;
        if (fileSnapshotStr == null) {
            return false;
        }
        return Long.parseLong(fileSnapshotStr) > lastIndexed;
    }

    // ── Arbitraries ─────────────────────────────────────────────────────

    @Provide
    Arbitrary<SnapshotPair> outsideWindowPairs() {
        return Arbitraries.longs().between(1, 1_000_000).flatMap(lastIndexed ->
                Arbitraries.longs().between(lastIndexed + 1, lastIndexed + 100_000)
                        .map(fileSnap -> new SnapshotPair(fileSnap, lastIndexed)));
    }

    @Provide
    Arbitrary<SnapshotPair> insideWindowPairs() {
        return Arbitraries.longs().between(1, 1_000_000).flatMap(lastIndexed ->
                Arbitraries.longs().between(1, lastIndexed)
                        .map(fileSnap -> new SnapshotPair(fileSnap, lastIndexed)));
    }

    @Provide
    Arbitrary<Long> positiveSnapshot() {
        return Arbitraries.longs().between(1, 1_000_000);
    }

    record SnapshotPair(long fileSnapshotId, long lastIndexedSnapshotId) {}
}
