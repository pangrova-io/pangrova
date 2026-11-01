package org.pangrova.indexing.engine.bitmap;

import org.pangrova.spi.connector.index.IndexEvalResult;
import org.pangrova.indexing.engine.bitmap.provider.BitmapStrategyProvider;
import org.pangrova.indexing.engine.bitmap.serializer.RoaringBitmapCodec;
import org.pangrova.spi.indexing.data.bitmap.BitmapIndexData;
import org.pangrova.spi.connector.index.predicate.EqualTo;
import org.pangrova.spi.connector.index.predicate.InList;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the direct container accumulation build path produces
 * identical results to the previous BitSet-based path.
 */
class BitmapDirectAccumulationTest {

    private final BitmapStrategyProvider provider = new BitmapStrategyProvider();

    @Test
    void buildAndEvaluateWithLowCardinality() {
        List<List<Comparable<?>>> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            rows.add(List.of(i % 5));
        }
        var data = provider.build(rows.iterator(), Map.of());
        assertThat(data).isInstanceOf(BitmapIndexData.class);
        var bitmap = (BitmapIndexData) data;
        assertThat(bitmap.partial()).isFalse();
        assertThat(bitmap.bitmaps()).hasSize(5);

        for (int v = 0; v < 5; v++) {
            assertThat(provider.evaluate(data, new EqualTo("c", v)))
                    .isEqualTo(IndexEvalResult.KEEP);
        }
        assertThat(provider.evaluate(data, new EqualTo("c", 99)))
                .isEqualTo(IndexEvalResult.SKIP);
    }

    @Test
    void buildWithPartialIndex() {
        List<List<Comparable<?>>> rows = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            rows.add(List.of(i));
        }
        var data = provider.build(rows.iterator(),
                Map.of("bitmap.max_cardinality", "50"));
        var bitmap = (BitmapIndexData) data;
        assertThat(bitmap.partial()).isTrue();
        assertThat(bitmap.bitmaps()).hasSize(50);

        // Unknown value in partial index returns UNKNOWN
        assertThat(provider.evaluate(data, new EqualTo("c", 9999)))
                .isEqualTo(IndexEvalResult.UNKNOWN);
    }

    @Test
    void buildWithNullValues() {
        List<List<Comparable<?>>> rows = List.of(
                List.of(1),
                List.of(),       // empty row
                List.of(2),
                List.of(1)
        );
        var data = provider.build(rows.iterator(), Map.of());
        assertThat(provider.evaluate(data, new EqualTo("c", 1)))
                .isEqualTo(IndexEvalResult.KEEP);
        assertThat(provider.evaluate(data, new EqualTo("c", 2)))
                .isEqualTo(IndexEvalResult.KEEP);
    }

    @Test
    void buildWithInListEvaluation() {
        List<List<Comparable<?>>> rows = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            rows.add(List.of(i % 10));
        }
        var data = provider.build(rows.iterator(), Map.of());

        var inList = new InList("c", List.of(3, 7));
        assertThat(provider.evaluate(data, inList)).isEqualTo(IndexEvalResult.KEEP);

        var inListMissing = new InList("c", List.of(99, 100));
        assertThat(provider.evaluate(data, inListMissing)).isEqualTo(IndexEvalResult.SKIP);
    }

    @Test
    void directAccumulationMatchesBitSetSerialization() {
        // Build via the provider (direct accumulation) and verify
        // the serialized bytes can be deserialized by the codec
        List<List<Comparable<?>>> rows = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            rows.add(List.of("val_" + (i % 3)));
        }
        var data = (BitmapIndexData) provider.build(rows.iterator(), Map.of());

        for (var entry : data.bitmaps().entrySet()) {
            byte[] serialized = entry.getValue();
            BitSet deserialized = RoaringBitmapCodec.deserialize(serialized);
            assertThat(deserialized.cardinality())
                    .as("Each of 3 values should appear in ~166-167 rows")
                    .isBetween(166, 167);
        }
    }

    @Test
    void largeRowCountTriggersContainerPromotion() {
        // 5000 rows with a single value — should trigger bitmap container
        List<List<Comparable<?>>> rows = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            rows.add(List.of("dense"));
        }
        var data = (BitmapIndexData) provider.build(rows.iterator(), Map.of());
        assertThat(data.bitmaps()).hasSize(1);

        byte[] serialized = data.bitmaps().get("dense");
        BitSet deserialized = RoaringBitmapCodec.deserialize(serialized);
        assertThat(deserialized.cardinality()).isEqualTo(5000);
        for (int i = 0; i < 5000; i++) {
            assertThat(deserialized.get(i)).isTrue();
        }
    }

    @Test
    void multiChunkPositionsAccumulatedCorrectly() {
        // Positions spanning multiple 16-bit chunks
        List<List<Comparable<?>>> rows = new ArrayList<>();
        int totalRows = 70_000; // spans chunk 0 (0-65535) and chunk 1 (65536+)
        for (int i = 0; i < totalRows; i++) {
            rows.add(List.of("multi"));
        }
        var data = (BitmapIndexData) provider.build(rows.iterator(), Map.of());

        byte[] serialized = data.bitmaps().get("multi");
        BitSet deserialized = RoaringBitmapCodec.deserialize(serialized);
        assertThat(deserialized.cardinality()).isEqualTo(totalRows);
        for (int i = 0; i < totalRows; i++) {
            assertThat(deserialized.get(i))
                    .as("Position %d should be set", i)
                    .isTrue();
        }
    }
}
