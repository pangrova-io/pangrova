package org.pangrova.indexing.strategies.bitmap.provider;

import org.pangrova.indexing.api.codec.TypeSerializer;
import org.pangrova.indexing.strategies.bitmap.serializer.BitmapSerializer;

import org.pangrova.indexing.strategies.normalization.IndexValueNormalizer;
import org.pangrova.indexing.strategies.bitmap.serializer.ContainerAccumulator;
import org.pangrova.indexing.strategies.bitmap.serializer.RoaringBitmapCodec;
import org.pangrova.connector.api.enums.IndexEvalResult;
import org.pangrova.indexing.api.strategy.IndexStrategyProvider;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.api.data.bitmap.BitmapIndexData;
import org.pangrova.indexing.api.data.IndexData;
import org.pangrova.connector.api.index.predicate.EqualTo;
import org.pangrova.connector.api.index.predicate.InList;
import org.pangrova.connector.api.index.predicate.IndexPredicate;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Bitmap index strategy for low-cardinality columns.
 *
 * <p>Tracks distinct values up to a configurable cardinality limit.
 * When exceeded, continues tracking existing values but drops new ones,
 * producing a partial index. Evaluation for unknown values in a partial
 * index returns UNKNOWN (never SKIP).</p>
 *
 * <p>Properties:
 * <ul>
 *   <li>{@code bitmap.max_cardinality} — max distinct values (default 10000)</li>
 * </ul>
 */
public final class BitmapStrategyProvider implements IndexStrategyProvider {

    private static final String PROP_MAX_CARDINALITY = "bitmap.max_cardinality";
    private static final int DEFAULT_MAX_CARDINALITY = 10_000;

    @Override
    public TypeSerializer<?> payloadSerializer() {
        return new BitmapSerializer();
    }

    @Override
    public IndexType supportedType() {
        return IndexType.BITMAP;
    }

    @Override
    public IndexData build(Iterator<List<Comparable<?>>> values, Map<String, String> properties) {
        int maxCardinality = intProp(properties, PROP_MAX_CARDINALITY, DEFAULT_MAX_CARDINALITY);
        AccumulationResult acc = accumulateValues(values, maxCardinality);
        Map<Comparable<?>, byte[]> serialized = serializeAccumulators(acc.accumulators);
        return new BitmapIndexData(serialized, acc.partial);
    }

    private record AccumulationResult(Map<Comparable<?>, ContainerAccumulator> accumulators, boolean partial) {}

    private static AccumulationResult accumulateValues(Iterator<List<Comparable<?>>> values, int maxCardinality) {
        Map<Comparable<?>, ContainerAccumulator> accumulators = new HashMap<>();
        boolean partial = false;
        int rowIndex = 0;
        while (values.hasNext()) {
            List<Comparable<?>> row = values.next();
            if (!row.isEmpty() && row.get(0) != null) {
                partial |= accumulateRow(accumulators, row.get(0), rowIndex, maxCardinality);
            }
            rowIndex++;
        }
        return new AccumulationResult(accumulators, partial);
    }

    private static boolean accumulateRow(Map<Comparable<?>, ContainerAccumulator> accumulators,
                                         Comparable<?> rawValue, int rowIndex, int maxCardinality) {
        Comparable<?> value = IndexValueNormalizer.toCanonicalKey(rawValue);
        ContainerAccumulator acc = accumulators.get(value);
        if (acc != null) {
            acc.add(rowIndex);
            return false;
        }
        if (accumulators.size() >= maxCardinality) {
            return true;
        }
        acc = new ContainerAccumulator();
        acc.add(rowIndex);
        accumulators.put(value, acc);
        return false;
    }

    private static Map<Comparable<?>, byte[]> serializeAccumulators(
            Map<Comparable<?>, ContainerAccumulator> accumulators) {
        Map<Comparable<?>, byte[]> serialized = new HashMap<>(accumulators.size());
        for (var entry : accumulators.entrySet()) {
            ContainerAccumulator acc = entry.getValue();
            serialized.put(entry.getKey(), RoaringBitmapCodec.serialize(acc.toBitmap()));
        }
        return serialized;
    }

    @Override
    public IndexEvalResult evaluate(IndexData indexData, IndexPredicate predicate) {
        if (!(indexData instanceof BitmapIndexData bitmap)) {
            return IndexEvalResult.UNKNOWN;
        }
        if (predicate instanceof EqualTo eq) {
            return evaluateEqual(bitmap, eq.value());
        }
        if (predicate instanceof InList inList) {
            return evaluateInList(bitmap, inList.values());
        }
        return IndexEvalResult.UNKNOWN;
    }

    private static IndexEvalResult evaluateEqual(BitmapIndexData bitmap, Comparable<?> value) {
        Comparable<?> key = IndexValueNormalizer.toCanonicalKey(value);
        if (bitmap.containsKey(key)) {
            return IndexEvalResult.KEEP;
        }
        return bitmap.partial() ? IndexEvalResult.UNKNOWN : IndexEvalResult.SKIP;
    }

    private static IndexEvalResult evaluateInList(BitmapIndexData bitmap, List<Comparable<?>> values) {
        if (containsAnyKey(bitmap, values)) {
            return IndexEvalResult.KEEP;
        }
        return bitmap.partial() ? IndexEvalResult.UNKNOWN : IndexEvalResult.SKIP;
    }

    private static boolean containsAnyKey(BitmapIndexData bitmap, List<Comparable<?>> values) {
        for (Comparable<?> val : values) {
            Comparable<?> key = IndexValueNormalizer.toCanonicalKey(val);
            if (bitmap.containsKey(key)) {
                return true;
            }
        }
        return false;
    }

    private static int intProp(Map<String, String> props, String key, int defaultVal) {
        String val = props.get(key);
        return val == null ? defaultVal : Integer.parseInt(val);
    }
}
