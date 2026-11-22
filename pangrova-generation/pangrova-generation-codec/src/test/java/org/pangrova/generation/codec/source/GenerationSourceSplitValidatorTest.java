package org.pangrova.generation.codec.source;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.DataSplit;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GenerationSourceSplitValidatorTest {
    @Test
    void preservesExactSourceIdentityAndScanDetail() {
        assertThatCode(() -> GenerationSourceSplitValidator.validateSplit(canonical())).doesNotThrowAnyException();
    }

    @Test
    void rejectsUnknownAndIncompleteSourceEvidence() {
        for (DataSplit invalid : List.of(DataSplit.getDefaultInstance(),
                canonical().toBuilder().clearSourceSnapshot().build(),
                canonical().toBuilder().clearDataObjectId().build(),
                canonical().toBuilder().setUnknownFields(com.google.protobuf.UnknownFieldSet.newBuilder()
                        .addField(10000, com.google.protobuf.UnknownFieldSet.Field.newBuilder().addVarint(1).build())
                        .build()).build())) {
            assertThatThrownBy(() -> GenerationSourceSplitValidator.validateSplit(invalid))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("canonical split identity");
        }
    }

    @Test
    void codecLoadsWithoutDomainCoreOrProviders() {
        for (String absent : List.of("org.pangrova.generation.submission.GenerationDispatchSpecFactory",
                "org.pangrova.generation.publication.read.PublishedGenerationBindingResolver",
                "org.pangrova.persistence.generation.api.job.GenerationJobStore",
                "org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchEngine",
                "org.postgresql.Driver")) {
            assertThatThrownBy(() -> Class.forName(absent, false, getClass().getClassLoader()))
                    .isInstanceOf(ClassNotFoundException.class);
        }
    }

    private static DataSplit canonical() {
        return DataSplit.newBuilder().setSplitId("split").setSourceId("lake.raw.source")
                .setSourceSnapshot("3").setDataObjectId("object").setOpaqueLocation("s3://bucket/object")
                .setConnectorId("iceberg").build();
    }

}
