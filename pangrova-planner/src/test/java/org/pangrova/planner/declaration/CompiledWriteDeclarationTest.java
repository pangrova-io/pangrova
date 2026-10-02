package org.pangrova.planner.declaration;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.junit.jupiter.api.Test;
import org.pangrova.parser.SqlParser;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.type.ConnectorColumnTypeMapper;
import org.pangrova.protocol.GeneratedColumnMetadataCodec;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.coordination.WriteDeclarationSnapshot;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompiledWriteDeclarationTest {
    @Test
    void capturesTheDeclarationFromTheActualCompiledTarget() {
        var metadata = metadata("documents", Map.of());
        var compiled = compile(new DeclarationTable(metadata, false));
        var definitions = GeneratedColumnMetadataCodec.decode(metadata.properties());

        assertThat(CompiledWriteDeclaration.capture(compiled)).contains(WriteDeclarationSnapshot.capture(
                new QualifiedTableName("catalog", "schema", "documents"), metadata, definitions.toByteArray(), false));
    }

    @Test
    void unbackedPlannerTableIsUnknownNotAnEmptyConnectorDeclaration() {
        assertThat(CompiledWriteDeclaration.capture(compile(new DeclarationTable(null, false)))).isEmpty();
    }

    @Test
    void rejectsConnectorIdentityDifferentFromTheCompiledTarget() {
        assertThatThrownBy(() -> CompiledWriteDeclaration.capture(
                compile(new DeclarationTable(metadata("different", Map.of()), false))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("actual connector schema");
    }

    @Test
    void rejectsPlannerTypeDriftRatherThanCountingOnlyColumnNames() {
        assertThatThrownBy(() -> CompiledWriteDeclaration.capture(
                compile(new DeclarationTable(metadata("documents", Map.of()), true))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("exact connector schema identity");
    }

    @Test
    void rejectsPartialGeneratedDeclarationAndMissingMutation() {
        var partial = metadata("documents", Map.of(GeneratedColumnMetadataCodec.PAYLOAD_PROPERTY, "bad"));
        assertThatThrownBy(() -> compile(new DeclarationTable(partial, false)))
                .isInstanceOf(org.pangrova.planner.PlannerException.class)
                .hasRootCauseInstanceOf(IllegalArgumentException.class).hasMessageContaining("present together");
        assertThatThrownBy(() -> CompiledWriteDeclaration.capture(null)).isInstanceOf(NullPointerException.class);
    }

    private static ConnectorTableMetadata metadata(String name, Map<String, String> properties) {
        return new ConnectorTableMetadata(new SchemaTableName("schema", name), List.of("id"),
                List.of(new ConnectorTableMetadata.Column("id", PangrovaType.BIGINT)), properties);
    }

    private static TableModify compile(AbstractTable table) {
        var root = CalciteSchema.createRootSchema(false);
        var catalog = root.add("catalog", new AbstractSchema());
        catalog.add("schema", new AbstractSchema()).add("documents", table);
        return (TableModify) new PangrovaPlanner(root, List.of("catalog", "schema"))
                .planFromAst(new SqlParser().parse("INSERT INTO catalog.schema.documents VALUES (1)")).plan();
    }

    private static final class DeclarationTable extends AbstractTable {
        private final ConnectorTableMetadata metadata;
        private final boolean drift;

        DeclarationTable(ConnectorTableMetadata metadata, boolean drift) {
            this.metadata = metadata;
            this.drift = drift;
        }

        @Override public RelDataType getRowType(RelDataTypeFactory factory) {
            var columns = drift ? List.of(new ConnectorTableMetadata.Column("id", PangrovaType.DOUBLE))
                    : CompiledWriteDeclarationTest.metadata("documents", Map.of()).typedColumns();
            return ConnectorColumnTypeMapper.rowType(columns, factory);
        }

        @Override public <T> T unwrap(Class<T> type) {
            return type.isInstance(metadata) ? type.cast(metadata) : super.unwrap(type);
        }
    }
}
