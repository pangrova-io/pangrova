package org.pangrova.generation.codec.source;

import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.DataSplit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GenerationSourceSplitCodecTest {
    @Test
    void preservesExactSourceIdentityAndScanDetail() {
        DataSplit split = canonical();
        assertThat(GenerationSourceSplitCodec.decode(List.of(encoded(split))))
                .containsExactly(split);
        assertThat(GenerationSourceSplitCodec.decode(List.of())).isEmpty();
    }

    @Test
    void rejectsUnparseableAndIncompleteSourceEvidence() {
        for (String invalid : List.of("not base64", "Cg==", encoded(DataSplit.getDefaultInstance()),
                encoded(canonical().toBuilder().clearSourceSnapshot().build()),
                encoded(canonical().toBuilder().clearDataObjectId().build()),
                encoded(canonical().toBuilder().setUnknownFields(com.google.protobuf.UnknownFieldSet.newBuilder()
                        .addField(10000, com.google.protobuf.UnknownFieldSet.Field.newBuilder().addVarint(1).build())
                        .build()).build()))) {
            assertThatThrownBy(() -> GenerationSourceSplitCodec.decode(List.of(invalid)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cannot parse split");
        }
    }

    @Test
    void codecLoadsWithoutDomainCoreOrProviders() {
        for (String absent : List.of("org.pangrova.generation.core.submission.GenerationDispatchSpecFactory",
                "org.pangrova.generation.core.publication.read.PublishedGenerationBindingResolver",
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

    private static String encoded(DataSplit split) {
        return Base64.getEncoder().encodeToString(split.toByteArray());
    }
}
