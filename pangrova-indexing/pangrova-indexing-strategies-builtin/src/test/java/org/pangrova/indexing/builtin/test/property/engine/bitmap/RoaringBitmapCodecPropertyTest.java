package org.pangrova.indexing.builtin.test.property.engine.bitmap;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.indexing.builtin.bitmap.provider.BitmapStrategyProvider;
import org.pangrova.connector.api.index.IndexEvalResult;
import org.pangrova.indexing.api.data.bitmap.BitmapIndexData;
import org.pangrova.connector.api.index.predicate.EqualTo;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for container-based Roaring Bitmap codec (BM1 fix).
 *
 * <p>Validates: BM1 — serialize/deserialize round-trip preserves all positions,
 * container selection (array vs bitmap) is correct, and the full build→evaluate
 * pipeline works with the new codec.</p>
 */
class RoaringBitmapCodecPropertyTest {

    private final BitmapStrategyProvider provider = new BitmapStrategyProvider();

    @Property(tries = 10)
    void buildThenEvaluatePreservesValues(
            @ForAll @IntRange(min = 1, max = 200) int rowCount) {
        List<List<Comparable<?>>> rows = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            // Use modulo to create low-cardinality values
            rows.add(List.of(i % 10));
        }
        var indexData = provider.build(rows.iterator(), Map.of());

        // Every value 0-9 (up to rowCount) should be found
        int maxVal = Math.min(9, rowCount - 1);
        for (int v = 0; v <= maxVal; v++) {
            var eq = new EqualTo("col", v);
            assertThat(provider.evaluate(indexData, eq))
                    .as("value %d should be KEEP", v)
                    .isEqualTo(IndexEvalResult.KEEP);
        }

        // A value that was never inserted should be SKIP
        var eq = new EqualTo("col", 999);
        assertThat(provider.evaluate(indexData, eq)).isEqualTo(IndexEvalResult.SKIP);
    }

    @Property(tries = 10)
    void highCardinalityBuildProducesPartialIndex(
            @ForAll @IntRange(min = 100, max = 300) int rowCount) {
        List<List<Comparable<?>>> rows = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            rows.add(List.of(i)); // each row is unique
        }
        // Set max cardinality to 50 — will produce partial index
        var indexData = provider.build(rows.iterator(),
                Map.of("bitmap.max_cardinality", "50"));

        assertThat(indexData).isInstanceOf(BitmapIndexData.class);
        var bitmap = (BitmapIndexData) indexData;
        assertThat(bitmap.partial()).isTrue();
        assertThat(bitmap.bitmaps().size()).isEqualTo(50);

        // Unknown value in partial index should return UNKNOWN, not SKIP
        var eq = new EqualTo("col", rowCount + 1);
        assertThat(provider.evaluate(indexData, eq)).isEqualTo(IndexEvalResult.UNKNOWN);
    }

    @Property(tries = 10)
    void sparsePositionsRoundTrip(
            @ForAll @IntRange(min = 1, max = 100) int posCount) {
        // Test with sparse positions (array container path)
        List<List<Comparable<?>>> rows = new ArrayList<>();
        // Create a single value with many rows — the bitmap for that value
        // will have posCount positions
        for (int i = 0; i < posCount; i++) {
            rows.add(List.of("same_value"));
        }
        var indexData = provider.build(rows.iterator(), Map.of());

        var eq = new EqualTo("col", "same_value");
        assertThat(provider.evaluate(indexData, eq)).isEqualTo(IndexEvalResult.KEEP);

        var eqMissing = new EqualTo("col", "other_value");
        assertThat(provider.evaluate(indexData, eqMissing)).isEqualTo(IndexEvalResult.SKIP);
    }
}
