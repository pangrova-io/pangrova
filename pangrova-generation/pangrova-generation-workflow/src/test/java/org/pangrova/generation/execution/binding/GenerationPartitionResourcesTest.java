package org.pangrova.generation.execution.binding;


import org.junit.jupiter.api.Test;
import org.pangrova.protocol.generation.PublishedGenerationBindingChecksum;
import org.pangrova.protocol.v2.ArtifactBinding;
import org.pangrova.protocol.v2.ArtifactObject;
import org.pangrova.protocol.v2.ArtifactObjectScope;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorResourceRequirement;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GenerationPartitionResourcesTest {
    @Test
    void countsPrimarySourceRowsOnceAndEveryRoleByte() {
        var first = operator(100, 1000, object("a0", "a", 0, 7, 20),
                object("a1", "a", 1, 7, 30));
        var second = operator(100, 1000, object("b0", "b", 0, 11, 40),
                object("b1", "b", 1, 11, 50));
        var result = GenerationPartitionResources.bind(List.of(first, second));
        assertThat(result).extracting(value -> value.getResources().getFillKeys()).containsExactly(7L, 11L);
        assertThat(result).extracting(value -> value.getResources().getObjectStoreReadBytes())
                .containsExactly(50L, 90L);
        assertThat(result.get(0).getArtifactsList()).isEqualTo(first.getArtifactsList());
        assertThat(first.getResources().getFillKeys()).isEqualTo(100);
    }

    @Test
    void rejectsRowAndByteTemplateOverrun() {
        assertThatThrownBy(() -> bind(operator(6, 100, object("a", "a", 0, 7, 20))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("exceed");
        assertThatThrownBy(() -> bind(operator(7, 19, object("a", "a", 0, 7, 20))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("exceed");
    }

    @Test
    void rejectsMissingOrDuplicatePrimaryEvidence() {
        assertThatThrownBy(() -> bind(operator(100, 1000, object("a1", "a", 1, 7, 20))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("primary");
        assertThatThrownBy(() -> bind(operator(100, 1000, object("a0", "a", 0, 7, 20),
                object("a2", "a", 0, 7, 20))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("repeats");
    }

    @Test
    void rejectsRoleRowDriftAndDuplicateObject() {
        var primary = object("a0", "a", 0, 7, 20);
        assertThatThrownBy(() -> bind(operator(100, 1000, primary, object("a1", "a", 1, 8, 20))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("row-count");
        assertThatThrownBy(() -> bind(operator(100, 1000, primary, primary)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("repeated");
    }

    @Test
    void rejectsUnsignedOrTamperedBinding() {
        var original = operator(100, 1000, object("a0", "a", 0, 7, 20));
        var unsigned = original.toBuilder().setArtifacts(0,
                original.getArtifacts(0).toBuilder().clearBindingChecksum()).build();
        assertThatThrownBy(() -> bind(unsigned)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("checksum");
        var changed = original.toBuilder().setArtifacts(0,
                original.getArtifacts(0).toBuilder().setGeneration("different")).build();
        assertThatThrownBy(() -> bind(changed)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("checksum mismatch");
    }

    @Test
    void rejectsUnsignedIntegerOverflowAndInvalidScope() {
        assertThatThrownBy(() -> bind(operator(Long.MAX_VALUE, Long.MAX_VALUE,
                object("a0", "a", 0, 1, Long.MAX_VALUE), object("a1", "a", 1, 1, 1))))
                .isInstanceOf(ArithmeticException.class);
        var invalid = object("a", "a", 0, 1, 1).toBuilder().setRowCount(-1).build();
        assertThatThrownBy(() -> bind(operator(100, 1000, invalid)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("invalid");
        var global = object("a", "a", 0, 1, 1).toBuilder()
                .setScope(ArtifactObjectScope.ARTIFACT_OBJECT_SCOPE_GENERATION).build();
        assertThatThrownBy(() -> bind(operator(100, 1000, global)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("invalid");
    }

    @Test
    void preservesProviderAndCandidateEnvelopes() {
        var mixed = operator(100, 1000, object("a", "a", 0, 7, 20)).toBuilder()
                .setResources(OperatorResourceRequirement.newBuilder().setFillKeys(100)
                        .setObjectStoreReadBytes(1000).setCandidateRows(10).setProviderItems(2)).build();
        assertThat(bind(mixed)).isSameAs(mixed);
        assertThat(bind(OperatorBinding.getDefaultInstance())).isEqualTo(OperatorBinding.getDefaultInstance());
    }

    private static OperatorBinding bind(OperatorBinding binding) {
        return GenerationPartitionResources.bind(List.of(binding)).get(0);
    }

    private static OperatorBinding operator(long rows, long bytes, ArtifactObject... objects) {
        var artifact = PublishedGenerationBindingChecksum.sign(ArtifactBinding.newBuilder()
                .setArtifactId("artifact").setGeneration("generation").addAllSplitObjects(List.of(objects)).build());
        return OperatorBinding.newBuilder().addArtifacts(artifact)
                .setResources(OperatorResourceRequirement.newBuilder()
                        .setFillKeys(rows).setObjectStoreReadBytes(bytes)).build();
    }

    private static ArtifactObject object(String id, String source, int ordinal, long rows, long bytes) {
        return ArtifactObject.newBuilder().setObjectId(id).setSourceObjectId(source).setOrdinal(ordinal)
                .setRowCount(rows).setSizeBytes(bytes)
                .setScope(ArtifactObjectScope.ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT).build();
    }
}
