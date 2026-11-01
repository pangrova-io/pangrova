package org.pangrova.coordinator.indexing.ddl;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.spi.indexing.descriptor.IndexType;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Normalizes SQL index options before they become immutable index metadata. */
final class IndexBuildProperties {

    private IndexBuildProperties() {}

    static Map<String, String> canonicalize(IndexType type, Map<String, String> properties) {
        if (type != IndexType.VECTOR) {
            return Map.copyOf(properties);
        }
        Map<String, String> canonical = new LinkedHashMap<>(properties);
        String algorithm = properties.getOrDefault("algorithm", "HNSW")
                .trim().toUpperCase(Locale.ROOT);
        if (algorithm.equals("IVFPQ")) {
            algorithm = "IVF_PQ";
        }
        canonical.put("algorithm", algorithm);
        canonical.put("metric", DurableIndexBuildService.requireMetric(properties, algorithm));
        if (algorithm.equals("IVF_PQ")) {
            rename(canonical, "ivfpq.nlist", "coarse_clusters");
            rename(canonical, "ivfpq.m", "pq_subvectors");
            rename(canonical, "ivfpq.nbits", "pq_bits_per_code");
        }
        return Map.copyOf(canonical);
    }

    private static void rename(Map<String, String> properties, String sqlName, String nativeName) {
        String value = properties.remove(sqlName);
        if (value == null) {
            return;
        }
        String existing = properties.putIfAbsent(nativeName, value);
        if (existing != null && !existing.equals(value)) {
            throw new PangrovaException(PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR,
                    "Conflicting index properties: " + sqlName + " and " + nativeName);
        }
    }
}
