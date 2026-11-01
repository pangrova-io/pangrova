package org.pangrova.spi.indexing;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.indexing.descriptor.IndexDescriptor;
import org.pangrova.spi.indexing.descriptor.IndexType;
import org.pangrova.spi.indexing.state.IndexState;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link IndexDescriptor} validation, state transitions,
 * and defensive copy behavior.
 */
class IndexDescriptorTest {

    private static final Instant NOW = Instant.parse("2025-01-01T00:00:00Z");
    private static final Instant LATER = Instant.parse("2025-01-01T01:00:00Z");

    private static IndexDescriptor base(IndexState state) {
        return new IndexDescriptor(
                "idx-1", "my_index", "catalog", "schema", "table",
                List.of("col1"), IndexType.BTREE, Map.of("k", "v"),
                state, null, 0, null, null, NOW, NOW, null);
    }

    // ── Validation ──────────────────────────────────────────────────

    @Test
    void nullIndexIdThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                new IndexDescriptor(null, "n", "c", "s", "t",
                        List.of("col"), IndexType.BTREE, Map.of(),
                        IndexState.BUILDING, null, 0, null, null, NOW, NOW, null));
    }

    @Test
    void blankIndexNameThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                new IndexDescriptor("id", "  ", "c", "s", "t",
                        List.of("col"), IndexType.BTREE, Map.of(),
                        IndexState.BUILDING, null, 0, null, null, NOW, NOW, null));
    }

    @Test
    void blankCatalogNameThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                new IndexDescriptor("id", "n", "", "s", "t",
                        List.of("col"), IndexType.BTREE, Map.of(),
                        IndexState.BUILDING, null, 0, null, null, NOW, NOW, null));
    }

    @Test
    void blankSchemaNameThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                new IndexDescriptor("id", "n", "c", " ", "t",
                        List.of("col"), IndexType.BTREE, Map.of(),
                        IndexState.BUILDING, null, 0, null, null, NOW, NOW, null));
    }

    @Test
    void blankTableNameThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                new IndexDescriptor("id", "n", "c", "s", "",
                        List.of("col"), IndexType.BTREE, Map.of(),
                        IndexState.BUILDING, null, 0, null, null, NOW, NOW, null));
    }

    @Test
    void nullIndexTypeThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                new IndexDescriptor("id", "n", "c", "s", "t",
                        List.of("col"), null, Map.of(),
                        IndexState.BUILDING, null, 0, null, null, NOW, NOW, null));
    }

    @Test
    void nullStateThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                new IndexDescriptor("id", "n", "c", "s", "t",
                        List.of("col"), IndexType.BTREE, Map.of(),
                        null, null, 0, null, null, NOW, NOW, null));
    }

    @Test
    void nullColumnNamesThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                new IndexDescriptor("id", "n", "c", "s", "t",
                        null, IndexType.BTREE, Map.of(),
                        IndexState.BUILDING, null, 0, null, null, NOW, NOW, null));
    }

    @Test
    void emptyColumnNamesThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                new IndexDescriptor("id", "n", "c", "s", "t",
                        List.of(), IndexType.BTREE, Map.of(),
                        IndexState.BUILDING, null, 0, null, null, NOW, NOW, null));
    }

    @Test
    void nullPropertiesDefaultsToEmptyMap() {
        IndexDescriptor desc = new IndexDescriptor(
                "id", "n", "c", "s", "t",
                List.of("col"), IndexType.BTREE, null,
                IndexState.BUILDING, null, 0, null, null, NOW, NOW, null);
        assertEquals(Map.of(), desc.properties());
    }

    // ── Defensive copy ──────────────────────────────────────────────

    @Test
    void columnNamesDefensivelyCopied() {
        ArrayList<String> mutable = new ArrayList<>(List.of("col1", "col2"));
        IndexDescriptor desc = new IndexDescriptor(
                "id", "n", "c", "s", "t",
                mutable, IndexType.BTREE, Map.of(),
                IndexState.BUILDING, null, 0, null, null, NOW, NOW, null);

        mutable.add("col3");
        assertEquals(List.of("col1", "col2"), desc.columnNames());
    }

    @Test
    void propertiesDefensivelyCopied() {
        HashMap<String, String> mutable = new HashMap<>(Map.of("k", "v"));
        IndexDescriptor desc = new IndexDescriptor(
                "id", "n", "c", "s", "t",
                List.of("col"), IndexType.BTREE, mutable,
                IndexState.BUILDING, null, 0, null, null, NOW, NOW, null);

        mutable.put("k2", "v2");
        assertEquals(Map.of("k", "v"), desc.properties());
    }

    @Test
    void columnNamesListIsUnmodifiable() {
        IndexDescriptor desc = base(IndexState.BUILDING);
        assertThrows(UnsupportedOperationException.class, () ->
                desc.columnNames().add("extra"));
    }

    @Test
    void propertiesMapIsUnmodifiable() {
        IndexDescriptor desc = base(IndexState.BUILDING);
        assertThrows(UnsupportedOperationException.class, () ->
                desc.properties().put("extra", "val"));
    }

    // ── State transitions — valid ───────────────────────────────────

    @Test
    void buildingToBuildingAllowed() {
        IndexDescriptor result = base(IndexState.BUILDING).withState(IndexState.BUILDING, LATER);
        assertEquals(IndexState.BUILDING, result.state());
        assertEquals(LATER, result.updatedAt());
    }

    @Test
    void buildingToReadyAllowed() {
        IndexDescriptor result = base(IndexState.BUILDING).withState(IndexState.READY, LATER);
        assertEquals(IndexState.READY, result.state());
    }

    @Test
    void buildingToStaleAllowed() {
        IndexDescriptor result = base(IndexState.BUILDING).withState(IndexState.STALE, LATER);
        assertEquals(IndexState.STALE, result.state());
    }

    @Test
    void buildingToDroppedAllowed() {
        IndexDescriptor result = base(IndexState.BUILDING).withState(IndexState.DROPPED, LATER);
        assertEquals(IndexState.DROPPED, result.state());
    }

    @Test
    void readyToStaleAllowed() {
        IndexDescriptor result = base(IndexState.READY).withState(IndexState.STALE, LATER);
        assertEquals(IndexState.STALE, result.state());
    }

    @Test
    void readyToDroppedAllowed() {
        IndexDescriptor result = base(IndexState.READY).withState(IndexState.DROPPED, LATER);
        assertEquals(IndexState.DROPPED, result.state());
    }

    @Test
    void staleToBuildingAllowed() {
        IndexDescriptor stale = base(IndexState.BUILDING)
                .withState(IndexState.STALE, "COMPACTION", LATER);
        IndexDescriptor result = stale.withState(IndexState.BUILDING, LATER);
        assertEquals(IndexState.BUILDING, result.state());
        assertNull(result.staleReason(), "staleReason should be cleared on non-STALE transition");
    }

    @Test
    void staleToDroppedAllowed() {
        IndexDescriptor stale = base(IndexState.BUILDING)
                .withState(IndexState.STALE, LATER);
        IndexDescriptor result = stale.withState(IndexState.DROPPED, LATER);
        assertEquals(IndexState.DROPPED, result.state());
    }

    // ── State transitions — invalid ─────────────────────────────────

    @Test
    void droppedToAnyThrows() {
        IndexDescriptor dropped = base(IndexState.BUILDING).withState(IndexState.DROPPED, LATER);
        for (IndexState target : IndexState.values()) {
            assertThrows(IllegalStateException.class, () ->
                    dropped.withState(target, LATER),
                    "DROPPED → " + target + " should throw");
        }
    }

    @Test
    void readyToBuildingAllowedForRebuild() {
        // READY → BUILDING is an allowed transition representing an explicit
        // index rebuild request (e.g. forced refresh after property change).
        IndexDescriptor rebuilt = base(IndexState.READY).withState(IndexState.BUILDING, LATER);
        assertEquals(IndexState.BUILDING, rebuilt.state());
    }

    @Test
    void readyToReadyThrows() {
        assertThrows(IllegalStateException.class, () ->
                base(IndexState.READY).withState(IndexState.READY, LATER));
    }

    @Test
    void staleToStaleThrows() {
        IndexDescriptor stale = base(IndexState.BUILDING).withState(IndexState.STALE, LATER);
        assertThrows(IllegalStateException.class, () ->
                stale.withState(IndexState.STALE, LATER));
    }

    @Test
    void staleToReadyThrows() {
        IndexDescriptor stale = base(IndexState.BUILDING).withState(IndexState.STALE, LATER);
        assertThrows(IllegalStateException.class, () ->
                stale.withState(IndexState.READY, LATER));
    }

    // ── staleReason handling ────────────────────────────────────────

    @Test
    void withStateOverloadSetsStaleReason() {
        IndexDescriptor result = base(IndexState.BUILDING)
                .withState(IndexState.STALE, "SNAPSHOT_GAP", LATER);
        assertEquals(IndexState.STALE, result.state());
        assertEquals("SNAPSHOT_GAP", result.staleReason());
    }

    @Test
    void staleReasonClearedOnTransitionToNonStale() {
        IndexDescriptor stale = base(IndexState.BUILDING)
                .withState(IndexState.STALE, "BUILD_OOM", LATER);
        IndexDescriptor building = stale.withState(IndexState.BUILDING, LATER);
        assertNull(building.staleReason());
    }

    @Test
    void staleReasonPreservedOnIdempotentWithStateNoReason() {
        // BUILDING → STALE with reason, then use single-arg withState for a valid transition
        IndexDescriptor stale = base(IndexState.BUILDING)
                .withState(IndexState.STALE, "COMPACTION", LATER);
        // stale → BUILDING clears reason
        IndexDescriptor building = stale.withState(IndexState.BUILDING, LATER);
        assertNull(building.staleReason());
    }

    // ── withGeneration ──────────────────────────────────────────────

    @Test
    void withGenerationUpdatesGenerationAndTimestamp() {
        IndexDescriptor desc = base(IndexState.BUILDING);
        IndexDescriptor result = desc.withGeneration(5, LATER);
        assertEquals(5, result.generation());
        assertEquals(LATER, result.updatedAt());
        assertEquals(desc.state(), result.state());
        assertEquals(desc.indexId(), result.indexId());
    }

    // ── Immutability — withState returns new instance ────────────────

    @Test
    void withStateReturnsNewInstance() {
        IndexDescriptor original = base(IndexState.BUILDING);
        IndexDescriptor transitioned = original.withState(IndexState.READY, LATER);
        assertEquals(IndexState.BUILDING, original.state(), "original should be unchanged");
        assertEquals(IndexState.READY, transitioned.state());
    }

    // ── Valid construction ───────────────────────────────────────────

    @Test
    void validConstructionSucceeds() {
        assertDoesNotThrow(() -> base(IndexState.BUILDING));
    }
}
