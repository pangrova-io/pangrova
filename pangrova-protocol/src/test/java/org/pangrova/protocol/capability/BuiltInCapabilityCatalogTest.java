package org.pangrova.protocol.capability;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.RowLocatorKind;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BuiltInCapabilityCatalogTest {

    @Test
    void planAndNativeReferencesShareSemanticIdentity() {
        var plan = BuiltInCapabilityCatalog.exactTopKPlanRef();
        var nativeRef = BuiltInCapabilityCatalog.exactTopKNativeRef();

        assertThat(plan.getCapabilityId()).isEqualTo(nativeRef.getCapabilityId());
        assertThat(plan.getSemanticVersion()).isEqualTo(nativeRef.getSemanticVersion());
        assertThat(plan.getSignatureSha256()).isEqualTo(nativeRef.getSignatureSha256());
        assertThat(plan.getSignatureSha256().size()).isEqualTo(32);
        assertThat(plan.getImplementationBuildId()).isEmpty();
        assertThat(nativeRef.getImplementationBuildId()).isNotBlank();
    }

    @Test
    void diskAnnV2BindsFormatAndPhysicalRowLocator() {
        var capability = BuiltInCapabilityCatalog.diskAnnArtifactBuildPlanRef();
        var format = BuiltInCapabilityCatalog.diskAnnArtifactFormat();
        var locator = BuiltInCapabilityCatalog.physicalRowLocatorContract();

        assertThat(capability.getSemanticVersion()).isEqualTo("2.0.0");
        assertThat(capability.getSignatureSha256()).hasSize(32);
        assertThat(format.getMajorVersion()).isEqualTo(2);
        assertThat(format.getSchemaSha256()).hasSize(32);
        assertThat(locator.getKind())
                .isEqualTo(RowLocatorKind.ROW_LOCATOR_KIND_PHYSICAL_ROW_POSITION);
        assertThat(locator.getSourceObjectScoped()).isTrue();
        assertThat(locator.getKeyTypesCount()).isZero();
        assertThat(locator.getSchemaSha256()).hasSize(32);
    }

    @Test
    void bm25BindsCurrentFormatAndSharedPhysicalLocator() {
        var build = BuiltInCapabilityCatalog.bm25ArtifactBuildPlanRef();
        var retrieval = BuiltInCapabilityCatalog.bm25RetrievalPlanRef();
        var format = BuiltInCapabilityCatalog.bm25ArtifactFormat();

        assertThat(build.getCapabilityId())
                .isEqualTo("pangrova.artifact.build.bm25");
        assertThat(retrieval.getCapabilityId())
                .isEqualTo("pangrova.retrieval.text.bm25");
        assertThat(build.getSignatureSha256()).hasSize(32);
        assertThat(retrieval.getSignatureSha256()).hasSize(32);
        assertThat(format.getFormatId()).isEqualTo("pangrova.bm25.segment");
        assertThat(format.getMajorVersion()).isEqualTo(1);
        assertThat(format.getSchemaSha256()).hasSize(32);
        assertThat(BuiltInCapabilityCatalog.physicalRowLocatorContract()
                .getSourceObjectScoped()).isTrue();
    }

    @Test
    void generatedValuesUsesGenericArtifactIdentity() {
        var build = BuiltInCapabilityCatalog.generatedValuesArtifactBuildPlanRef();
        var overlay = BuiltInCapabilityCatalog.generatedValuesOverlayPlanRef();
        var format = BuiltInCapabilityCatalog.generatedValuesArtifactFormat();

        assertThat(build.getCapabilityId())
                .isEqualTo("pangrova.artifact.build.generated_values");
        assertThat(build.getSemanticVersion()).isEqualTo("2.0.0");
        assertThat(build.getSignatureSha256()).hasSize(32);
        assertThat(overlay.getCapabilityId())
                .isEqualTo("pangrova.generated-values.overlay");
        assertThat(overlay.getSemanticVersion()).isEqualTo("2.0.0");
        assertThat(overlay.getSignatureSha256()).hasSize(32);
        assertThat(format.getFormatId())
                .isEqualTo("pangrova.generated-values.segment");
        assertThat(format.getMajorVersion()).isEqualTo(2);
        assertThat(format.getSchemaSha256()).hasSize(32);
    }

    @Test
    void generatedNullContractsHaveReproducibleExactFingerprints() throws Exception {
        assertThat(BuiltInCapabilityCatalog.generatedValuesArtifactBuildPlanRef()
                .getSignatureSha256().toByteArray()).isEqualTo(digest(
                "pangrova.artifact.build.generated_values@2.0.0:"
                + "SOURCE_MAP(BIGINT,nullable VECTOR(FP32))->GENERATED_VALUES_V2[0],"
                + "ROW_LOCATORS[1];retain_null_locator;report_v2"));
        assertThat(BuiltInCapabilityCatalog.generatedValuesArtifactFormat()
                .getSchemaSha256().toByteArray()).isEqualTo(digest(
                "pangrova.generated-values.segment@2:PGGENV02;"
                + "u32le(schema=2,kind=1,dimension,validity_encoding=1);u64le(rows);"
                + "lsb_first_validity_bitmap_zero_padding;dense_fp32le;null_payload_positive_zero"));
        assertThat(BuiltInCapabilityCatalog.generatedValuesOverlayPlanRef()
                .getSignatureSha256().toByteArray()).isEqualTo(digest(
                "pangrova.generated-values.overlay@2.0.0:physical_source_object_row_position_exact_join;"
                + "GENERATED_VALUES_V2_nullable_vector;retain_null_rows;fail_closed_missing_duplicate"));
    }

    private static byte[] digest(String contract) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(contract.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void everyNativeAnnCapabilityHasAnExactBuildRetrievalAndFormatIdentity() {
        assertAnnIdentity(
                BuiltInCapabilityCatalog.hnswArtifactBuildPlanRef(),
                BuiltInCapabilityCatalog.hnswRetrievalPlanRef(),
                BuiltInCapabilityCatalog.hnswArtifactFormat(),
                "pangrova.artifact.build.hnsw",
                "pangrova.retrieval.vector.hnsw",
                "pangrova.hnsw.segment");
        assertAnnIdentity(
                BuiltInCapabilityCatalog.ivfPqArtifactBuildPlanRef(),
                BuiltInCapabilityCatalog.ivfPqRetrievalPlanRef(),
                BuiltInCapabilityCatalog.ivfPqArtifactFormat(),
                "pangrova.artifact.build.ivf_pq",
                "pangrova.retrieval.vector.ivf_pq",
                "pangrova.ivfpq.segment");
    }

    @Test
    void indexMetadataAlgorithmsResolveToCanonicalNativeIdentities() {
        assertThat(BuiltInCapabilityCatalog.vectorArtifactBuildPlanRef("IVF_PQ"))
                .isEqualTo(BuiltInCapabilityCatalog.ivfPqArtifactBuildPlanRef());
        assertThat(BuiltInCapabilityCatalog.vectorRetrievalPlanRef("IVF_PQ"))
                .isEqualTo(BuiltInCapabilityCatalog.ivfPqRetrievalPlanRef());
        assertThat(BuiltInCapabilityCatalog.vectorArtifactFormat("DISK_ANN"))
                .isEqualTo(BuiltInCapabilityCatalog.diskAnnArtifactFormat());
        assertThatThrownBy(() -> BuiltInCapabilityCatalog
                .vectorArtifactBuildPlanRef("ivfpq"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported Native vector index algorithm");
        assertThatThrownBy(() -> BuiltInCapabilityCatalog
                .vectorRetrievalPlanRef("IVF-PQ"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported Native vector index algorithm");
        assertThatThrownBy(() -> BuiltInCapabilityCatalog
                .vectorRetrievalPlanRef("unknown"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported Native vector index algorithm");
    }

    private static void assertAnnIdentity(
            org.pangrova.protocol.v2.CapabilityRef build,
            org.pangrova.protocol.v2.CapabilityRef retrieval,
            org.pangrova.protocol.v2.ArtifactFormatRef format,
            String buildId, String retrievalId, String formatId) {
        assertThat(build.getCapabilityId()).isEqualTo(buildId);
        assertThat(retrieval.getCapabilityId()).isEqualTo(retrievalId);
        assertThat(build.getSemanticVersion()).isEqualTo("2.0.0");
        assertThat(retrieval.getSemanticVersion()).isEqualTo("2.0.0");
        assertThat(format.getFormatId()).isEqualTo(formatId);
        assertThat(build.getSignatureSha256()).hasSize(32);
        assertThat(retrieval.getSignatureSha256()).hasSize(32);
        assertThat(format.getSchemaSha256()).hasSize(32);
        assertThat(format.getMajorVersion()).isEqualTo(2);
    }
}
