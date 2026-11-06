package org.pangrova.coordinator.indexing.ddl;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.indexing.api.descriptor.IndexType;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexBuildPropertiesTest {

    @Test
    void sqlIvfpqOptionsBecomeCanonicalBuildAndRetrievalMetadata() {
        Map<String, String> options = IndexBuildProperties.canonicalize(IndexType.VECTOR,
                Map.of("algorithm", "ivfpq", "ivfpq.nlist", "2",
                        "ivfpq.m", "2", "ivfpq.nbits", "8", "metric", "L2"));
        assertThat(options).containsExactlyInAnyOrderEntriesOf(Map.of(
                "algorithm", "IVF_PQ", "coarse_clusters", "2", "pq_subvectors", "2",
                "pq_bits_per_code", "8", "metric", "L2"));
        assertThat(BuiltInCapabilityCatalog.vectorArtifactBuildPlanRef(options.get("algorithm")))
                .isEqualTo(BuiltInCapabilityCatalog.ivfPqArtifactBuildPlanRef());
        assertThat(BuiltInCapabilityCatalog.vectorRetrievalPlanRef(options.get("algorithm")))
                .isEqualTo(BuiltInCapabilityCatalog.ivfPqRetrievalPlanRef());
    }

    @Test
    void conflictingSqlAndCanonicalOptionsFailBeforeIndexRegistration() {
        assertThatThrownBy(() -> IndexBuildProperties.canonicalize(IndexType.VECTOR,
                Map.of("algorithm", "IVFPQ", "ivfpq.nlist", "2", "coarse_clusters", "3")))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("Conflicting index properties");
    }

    @Test
    void canonicalOptionsAndOtherIndexTypesArePreserved() {
        Map<String, String> properties = Map.of("algorithm", "IVF_PQ", "coarse_clusters", "4", "metric", "L2");
        assertThat(IndexBuildProperties.canonicalize(IndexType.VECTOR, properties))
                .containsExactlyInAnyOrderEntriesOf(properties);
        assertThat(IndexBuildProperties.canonicalize(IndexType.BLOOM_FILTER, Map.of("fpp", "0.01")))
                .containsExactlyInAnyOrderEntriesOf(Map.of("fpp", "0.01"));
    }

    @Test
    void persistsDefaultAndCanonicalMetricBeforePlannerBinding() {
        assertThat(IndexBuildProperties.canonicalize(IndexType.VECTOR, Map.of()))
                .containsEntry("algorithm", "HNSW").containsEntry("metric", "L2");
        for (String algorithm : new String[]{"HNSW", "IVF_PQ"}) {
            for (String metric : new String[]{"L2", "COSINE", "INNER_PRODUCT"}) {
                assertThat(IndexBuildProperties.canonicalize(IndexType.VECTOR,
                        Map.of("algorithm", algorithm, "metric", metric.toLowerCase(java.util.Locale.ROOT))))
                        .containsEntry("metric", metric);
            }
        }
        assertThat(IndexBuildProperties.canonicalize(IndexType.VECTOR,
                Map.of("metric", "EUCLIDEAN"))).containsEntry("metric", "L2");
    }

    @Test
    void rejectsUnsupportedMetricsBeforeRegistration() {
        assertThatThrownBy(() -> IndexBuildProperties.canonicalize(IndexType.VECTOR,
                Map.of("algorithm", "HNSW", "metric", "MANHATTAN")))
                .isInstanceOf(PangrovaException.class);
        assertThatThrownBy(() -> IndexBuildProperties.canonicalize(IndexType.VECTOR,
                Map.of("algorithm", "DISK_ANN", "metric", "COSINE")))
                .isInstanceOf(PangrovaException.class);
    }
}
