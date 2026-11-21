package org.pangrova.indexing.api.data;

import org.junit.jupiter.api.Test;
import org.pangrova.indexing.api.data.bitmap.BitmapIndexData;
import org.pangrova.indexing.api.data.text.GinIndexData;
import org.pangrova.indexing.api.data.text.NGramBloomIndexData;
import org.pangrova.indexing.api.statistics.DynamicFilterSummary.ColumnRangeSummary;
import org.pangrova.indexing.api.statistics.RowGroupSkipInfo;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexBufferOwnershipTest {

    @Test
    void bloomViewsHaveIndependentCursorsAndNoWritableAliases() {
        byte[] original = {1, 2, 3};
        var bloom = new BloomFilterIndexData(original, 8, 24);
        var ngram = new NGramBloomIndexData(original, 8, 24, 3);
        original[0] = 99;
        bloom.filterData()[0] = 88;
        ngram.filterData()[0] = 77;
        for (ByteBuffer view : List.of(bloom.filterDataView(), ngram.filterDataView())) {
            assertThat(view.get(0)).isEqualTo((byte) 1);
            assertThat(view.hasArray()).isFalse();
            assertThatThrownBy(view::array).isInstanceOf(ReadOnlyBufferException.class);
            assertThatThrownBy(() -> view.put(0, (byte) 5)).isInstanceOf(ReadOnlyBufferException.class);
            assertThatThrownBy(() -> view.slice().put((byte) 5)).isInstanceOf(ReadOnlyBufferException.class);
            view.position(2);
            view.limit(2);
        }
        assertThat(bloom.filterDataView().remaining()).isEqualTo(3);
        assertThat(ngram.filterDataView().remaining()).isEqualTo(3);
    }

    @Test
    void bitmapSnapshotsCannotChangeEncodedPositionsOrMembership() {
        byte[] encoded = {1, 2};
        Map<Comparable<?>, byte[]> input = new HashMap<>();
        input.put("a", encoded);
        var bitmap = new BitmapIndexData(input, true);
        encoded[0] = 8;
        input.clear();
        var snapshot = bitmap.bitmaps();
        snapshot.get("a")[0] = 9;
        assertThatThrownBy(() -> snapshot.put("b", new byte[0]))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(bitmap.containsKey("a")).isTrue();
        assertThat(bitmap.containsKey("b")).isFalse();
        assertThat(bitmap.bitmaps().get("a")).containsExactly(1, 2);
        assertThat(bitmap.partial()).isTrue();
    }

    @Test
    void postingsAndRowPositionsAreDeeplyImmutable() {
        var positions = new ArrayList<>(List.of(1, 2));
        var rows = new ArrayList<List<Integer>>();
        rows.add(positions);
        var page = new BTreePage(List.of("a"), rows, "a", "a");
        var gin = new GinIndexData(Map.of("a", positions), true);
        positions.clear();
        rows.clear();
        assertThat(page.rowPositions()).containsExactly(List.of(1, 2));
        assertThat(gin.elementPostings().get("a")).containsExactly(1, 2);
        assertThatThrownBy(() -> page.rowPositions().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> page.rowPositions().get(0).clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> gin.elementPostings().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> gin.elementPostings().get("a").clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rowGroupHintsCannotBeMutatedAndCannotContainOutOfRangeSkips() {
        BitSet skipped = new BitSet();
        skipped.set(1);
        var hint = new RowGroupSkipInfo("data", 3, skipped);
        skipped.clear();
        hint.skippedGroups().clear();
        assertThat(hint.shouldRead(1)).isFalse();
        assertThat(hint.retainedIndices()).containsExactly(0, 2);
        assertThat(hint.retainedCount()).isEqualTo(2);
        RowGroupSkipInfo.NONE.skippedGroups().set(1);
        assertThat(RowGroupSkipInfo.NONE.shouldRead(1)).isTrue();
        assertThat(RowGroupSkipInfo.NONE.hasSkips()).isFalse();
        assertThatThrownBy(() -> new RowGroupSkipInfo("data", -1, new BitSet()))
                .isInstanceOf(IllegalArgumentException.class);
        skipped.set(3);
        assertThatThrownBy(() -> new RowGroupSkipInfo("data", 3, skipped))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void dynamicFilterDigestIsIsolatedOnInputAndOutput() {
        byte[] digest = {1, 2};
        var range = new ColumnRangeSummary("key", 1, 10, digest, 2);
        digest[0] = 9;
        range.bloomFilter()[1] = 8;
        assertThat(range.bloomFilter()).containsExactly(1, 2);
        assertThat(new ColumnRangeSummary("key", null, null, null, 0).bloomFilter()).isEmpty();
    }
}
