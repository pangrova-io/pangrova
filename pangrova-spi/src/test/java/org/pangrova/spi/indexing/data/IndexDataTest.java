package org.pangrova.spi.indexing.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pangrova.spi.indexing.descriptor.IndexType;
import org.pangrova.spi.indexing.data.bitmap.BitmapIndexData;
import org.pangrova.spi.indexing.data.spatial.BoundingBox;
import org.pangrova.spi.indexing.data.spatial.RTreeIndexData;
import org.pangrova.spi.indexing.data.spatial.RTreeNode;
import org.pangrova.spi.indexing.data.spatial.SparseEntry;
import org.pangrova.spi.indexing.data.spatial.SparseIndexData;
import org.pangrova.spi.indexing.data.text.GinIndexData;
import org.pangrova.spi.indexing.data.text.NGramBloomIndexData;
import org.pangrova.spi.indexing.data.text.PrefixEntry;
import org.pangrova.spi.indexing.data.text.PrefixIndexData;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link IndexData} implementations,
 * auxiliary records, and {@link BoundingBox} spatial operations.
 */
class IndexDataTest {

    // ── IndexType correctness ───────────────────────────────────────

    @Test
    void btreeReturnsCorrectType() {
        var data = new BTreeIndexData(List.of(), false);
        assertEquals(IndexType.BTREE, data.indexType());
    }

    @Test
    void bloomFilterReturnsCorrectType() {
        var data = new BloomFilterIndexData(new byte[8], 3, 64);
        assertEquals(IndexType.BLOOM_FILTER, data.indexType());
    }

    @Test
    void prefixReturnsCorrectType() {
        var data = new PrefixIndexData(List.of(), 3);
        assertEquals(IndexType.PREFIX, data.indexType());
    }

    @Test
    void ngramBloomReturnsCorrectType() {
        var data = new NGramBloomIndexData(new byte[8], 3, 64, 3);
        assertEquals(IndexType.NGRAM_BLOOM_FILTER, data.indexType());
    }

    @Test
    void bitmapReturnsCorrectType() {
        var data = new BitmapIndexData(Map.of(), false);
        assertEquals(IndexType.BITMAP, data.indexType());
    }

    @Test
    void hashReturnsCorrectType() {
        var data = new HashIndexData(new byte[16], 0, 16);
        assertEquals(IndexType.HASH, data.indexType());
    }

    @Test
    void sparseReturnsCorrectType() {
        var data = new SparseIndexData(List.of(), 1024);
        assertEquals(IndexType.SPARSE, data.indexType());
    }

    @Test
    void rtreeReturnsCorrectType() {
        var data = new RTreeIndexData(List.of(), 0);
        assertEquals(IndexType.R_TREE, data.indexType());
    }

    @Test
    void ginReturnsCorrectType() {
        var data = new GinIndexData(Map.of(), false);
        assertEquals(IndexType.GIN, data.indexType());
    }

    // ── Defensive copy — byte[] fields ──────────────────────────────

    @Test
    void bloomFilterDefensiveCopyOnConstruction() {
        byte[] original = {1, 2, 3, 4};
        var data = new BloomFilterIndexData(original, 2, 32);
        original[0] = 99;
        assertEquals(1, data.filterData()[0]);
    }

    @Test
    void bloomFilterAccessorReturnsInternalArray() {
        var data = new BloomFilterIndexData(new byte[]{1, 2, 3}, 2, 24);
        byte[] first = data.filterData();
        byte[] second = data.filterData();
        assertSame(first, second, "accessor must return same internal array (zero-copy)");
    }

    @Test
    void ngramBloomDefensiveCopyOnConstruction() {
        byte[] original = {10, 20, 30};
        var data = new NGramBloomIndexData(original, 3, 24, 3);
        original[0] = 99;
        assertEquals(10, data.filterData()[0]);
    }

    @Test
    void hashDefensiveCopyOnConstruction() {
        byte[] original = {5, 6, 7, 8};
        var data = new HashIndexData(original, 1, 4);
        original[0] = 99;
        assertEquals(5, data.table()[0]);
    }

    @Test
    void hashDefensiveCopyOnAccess() {
        var data = new HashIndexData(new byte[]{5, 6, 7}, 1, 3);
        byte[] accessed = data.table();
        accessed[0] = 99;
        assertEquals(5, data.table()[0]);
    }

    // ── Defensive copy — List fields ────────────────────────────────

    @Test
    void btreeDefensiveCopyOfLeafPages() {
        var page = new BTreePage(List.of("a"), null, "a", "a");
        var mutableList = new ArrayList<>(List.of(page));
        var data = new BTreeIndexData(mutableList, false);
        mutableList.add(page);
        assertEquals(1, data.leafPages().size());
    }

    @Test
    void prefixDefensiveCopyOfEntries() {
        var entry = new PrefixEntry("abc", 0L);
        var mutableList = new ArrayList<>(List.of(entry));
        var data = new PrefixIndexData(mutableList, 3);
        mutableList.add(entry);
        assertEquals(1, data.entries().size());
    }

    @Test
    void sparseDefensiveCopyOfEntries() {
        var entry = new SparseEntry("key", 0L);
        var mutableList = new ArrayList<>(List.of(entry));
        var data = new SparseIndexData(mutableList, 1024);
        mutableList.add(entry);
        assertEquals(1, data.entries().size());
    }

    @Test
    void rtreeDefensiveCopyOfNodes() {
        var box = new BoundingBox(new double[]{0}, new double[]{1});
        var node = new RTreeNode(box, List.of(), true);
        var mutableList = new ArrayList<>(List.of(node));
        var data = new RTreeIndexData(mutableList, 1);
        mutableList.add(node);
        assertEquals(1, data.nodes().size());
    }

    // ── Defensive copy — Map fields ─────────────────────────────────

    @Test
    void ginDefensiveDeepCopy() {
        var mutablePostings = new HashMap<String, List<Integer>>();
        var mutablePositions = new ArrayList<>(List.of(10, 20));
        mutablePostings.put("elem", mutablePositions);

        var data = new GinIndexData(mutablePostings, true);

        mutablePostings.put("new_elem", List.of(30));
        mutablePositions.add(99);

        assertEquals(1, data.elementPostings().size());
        assertEquals(List.of(10, 20), data.elementPostings().get("elem"));
    }

    @Test
    void bitmapDefensiveDeepCopy() {
        byte[] bitmapBytes = {1, 2, 3};
        var mutableBitmaps = new HashMap<Comparable<?>, byte[]>();
        mutableBitmaps.put("val", bitmapBytes);

        var data = new BitmapIndexData(mutableBitmaps, false);

        // Mutate original byte array
        bitmapBytes[0] = 99;
        // Mutate original map
        mutableBitmaps.put("val2", new byte[]{4});

        assertEquals(1, data.bitmaps().size());
        assertArrayEquals(new byte[]{1, 2, 3}, data.bitmaps().get("val"));
    }

    // ── Defensive copy — BTreePage ──────────────────────────────────

    @Test
    void btreePageDefensiveCopyOfKeys() {
        var mutableKeys = new ArrayList<Comparable<?>>(List.of("a", "b"));
        var page = new BTreePage(mutableKeys, null, "a", "b");
        mutableKeys.add("c");
        assertEquals(2, page.keys().size());
    }

    @Test
    void btreePageDefensiveCopyOfRowPositions() {
        var innerList = new ArrayList<Integer>(List.of(1, 2));
        List<List<Integer>> rowPositions = new ArrayList<>(List.of((List<Integer>) innerList));
        var page = new BTreePage(List.of("a"), rowPositions, "a", "a");
        innerList.add(99);
        rowPositions.add(List.of(3));
        assertEquals(1, page.rowPositions().size());
        assertEquals(List.of(1, 2), page.rowPositions().get(0));
    }

    // ── Defensive copy — RTreeNode ──────────────────────────────────

    @Test
    void rtreeNodeDefensiveCopyOfChildIndices() {
        var box = new BoundingBox(new double[]{0}, new double[]{1});
        var mutableIndices = new ArrayList<>(List.of(0, 1));
        var node = new RTreeNode(box, mutableIndices, false);
        mutableIndices.add(2);
        assertEquals(2, node.childIndices().size());
    }

    // ── BoundingBox — defensive copy ────────────────────────────────

    @Test
    void boundingBoxDefensiveCopyOnConstruction() {
        double[] min = {0.0, 0.0};
        double[] max = {1.0, 1.0};
        var box = new BoundingBox(min, max);
        min[0] = 99.0;
        max[0] = 99.0;
        assertEquals(0.0, box.minValues()[0]);
        assertEquals(1.0, box.maxValues()[0]);
    }

    @Test
    void boundingBoxZeroCopyOnAccess() {
        var box = new BoundingBox(new double[]{0.0}, new double[]{1.0});
        assertSame(box.minValues(), box.minValues(),
                "minValues() must return the same internal array (zero-copy)");
        assertSame(box.maxValues(), box.maxValues(),
                "maxValues() must return the same internal array (zero-copy)");
    }

    // ── BoundingBox — mbrIntersects ─────────────────────────────────

    @Test
    void mbrIntersectsOverlappingBoxes() {
        var a = new BoundingBox(new double[]{0, 0}, new double[]{2, 2});
        var b = new BoundingBox(new double[]{1, 1}, new double[]{3, 3});
        assertTrue(a.mbrIntersects(b));
        assertTrue(b.mbrIntersects(a));
    }

    @Test
    void mbrIntersectsDisjointBoxes() {
        var a = new BoundingBox(new double[]{0, 0}, new double[]{1, 1});
        var b = new BoundingBox(new double[]{2, 2}, new double[]{3, 3});
        assertFalse(a.mbrIntersects(b));
        assertFalse(b.mbrIntersects(a));
    }

    @Test
    void mbrIntersectsTouchingEdge() {
        var a = new BoundingBox(new double[]{0, 0}, new double[]{1, 1});
        var b = new BoundingBox(new double[]{1, 0}, new double[]{2, 1});
        assertTrue(a.mbrIntersects(b));
    }

    @Test
    void mbrIntersectsContainedBox() {
        var outer = new BoundingBox(new double[]{0, 0}, new double[]{10, 10});
        var inner = new BoundingBox(new double[]{2, 2}, new double[]{5, 5});
        assertTrue(outer.mbrIntersects(inner));
        assertTrue(inner.mbrIntersects(outer));
    }

    @Test
    void mbrIntersectsNullReturnsFalse() {
        var box = new BoundingBox(new double[]{0}, new double[]{1});
        assertFalse(box.mbrIntersects(null));
    }

    @Test
    void mbrIntersectsDimensionMismatchReturnsFalse() {
        var a = new BoundingBox(new double[]{0, 0}, new double[]{1, 1});
        var b = new BoundingBox(new double[]{0, 0, 0}, new double[]{1, 1, 1});
        assertFalse(a.mbrIntersects(b));
    }

    // ── BoundingBox — mbrContains ───────────────────────────────────

    @Test
    void mbrContainsInnerBox() {
        var outer = new BoundingBox(new double[]{0, 0}, new double[]{10, 10});
        var inner = new BoundingBox(new double[]{2, 2}, new double[]{5, 5});
        assertTrue(outer.mbrContains(inner));
        assertFalse(inner.mbrContains(outer));
    }

    @Test
    void mbrContainsSameBox() {
        var box = new BoundingBox(new double[]{1, 1}, new double[]{3, 3});
        assertTrue(box.mbrContains(box));
    }

    @Test
    void mbrContainsPartialOverlapReturnsFalse() {
        var a = new BoundingBox(new double[]{0, 0}, new double[]{2, 2});
        var b = new BoundingBox(new double[]{1, 1}, new double[]{3, 3});
        assertFalse(a.mbrContains(b));
    }

    @Test
    void mbrContainsNullReturnsFalse() {
        var box = new BoundingBox(new double[]{0}, new double[]{1});
        assertFalse(box.mbrContains(null));
    }

    @Test
    void mbrContainsDimensionMismatchReturnsFalse() {
        var a = new BoundingBox(new double[]{0, 0}, new double[]{1, 1});
        var b = new BoundingBox(new double[]{0}, new double[]{1});
        assertFalse(a.mbrContains(b));
    }

    // ── estimatedSizeInBytes sanity ─────────────────────────────────

    @Test
    void estimatedSizeIsNonNegative() {
        List<IndexData> allTypes = List.of(
                new BTreeIndexData(List.of(), false),
                new BloomFilterIndexData(new byte[8], 3, 64),
                new PrefixIndexData(List.of(), 3),
                new NGramBloomIndexData(new byte[8], 3, 64, 3),
                new BitmapIndexData(Map.of(), false),
                new HashIndexData(new byte[16], 0, 16),
                new SparseIndexData(List.of(), 1024),
                new RTreeIndexData(List.of(), 0),
                new GinIndexData(Map.of(), false)
        );
        for (IndexData data : allTypes) {
            assertTrue(data.estimatedSizeInBytes() >= 0,
                    data.indexType() + " should have non-negative size");
        }
    }
}
