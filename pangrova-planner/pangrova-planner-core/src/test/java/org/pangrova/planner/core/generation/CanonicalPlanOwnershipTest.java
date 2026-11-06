package org.pangrova.planner.core.generation;

import org.junit.jupiter.api.Test;
import org.pangrova.planner.core.substrait.NativeDatasetSchemaCompiler;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.RowLocatorContract;
import org.pangrova.types.PangrovaType;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CanonicalPlanOwnershipTest {
    @Test
    void artifactOwnsBytesAndRetainsImmutableCanonicalContracts() {
        byte[] bytes = {1, 2};
        var capability = BuiltInCapabilityCatalog.generatedValuesArtifactBuildPlanRef();
        var format = BuiltInCapabilityCatalog.generatedValuesArtifactFormat();
        var locatorBuilder = RowLocatorContract.newBuilder().setSchemaVersion(2);
        var locator = locatorBuilder.build();
        var artifact = new GenerationPlan(bytes, "sink", capability, format, locator);
        bytes[0] = 9;
        artifact.planBytes()[1] = 8;
        locatorBuilder.setSchemaVersion(99);
        artifact.capability().toBuilder().setSemanticVersion("changed").build();
        artifact.format().toBuilder().setMajorVersion(99).build();
        artifact.rowLocator().toBuilder().setSchemaVersion(99).build();
        assertThat(artifact.planBytes()).containsExactly(1, 2);
        assertThat(artifact.capability()).isSameAs(capability);
        assertThat(artifact.format()).isSameAs(format);
        assertThat(artifact.rowLocator()).isSameAs(locator);
        assertThat(artifact.rowLocator().getSchemaVersion()).isEqualTo(2);
    }

    @Test
    void compiledSchemaCannotBeMutatedThroughItsProtobufOrLogicalConstraintAccessors() {
        var compiled = NativeDatasetSchemaCompiler.compile(List.of(
                new ConnectorTableMetadata.Column("id", PangrovaType.INTEGER)));
        byte[] original = compiled.schema().toByteArray();
        compiled.schema().toBuilder().setNames(0, "changed").build();
        assertThat(compiled.schema().getNamesList()).containsExactly("id");
        assertThat(compiled.schema().toByteArray()).isEqualTo(original);
        assertThatThrownBy(() -> compiled.columnTypes().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
