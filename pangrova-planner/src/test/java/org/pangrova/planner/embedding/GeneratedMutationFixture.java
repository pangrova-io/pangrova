package org.pangrova.planner.embedding;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.pangrova.parser.parse.AntlrSqlParser;
import org.pangrova.planner.analysis.PangrovaPlanner;
import org.pangrova.planner.type.ConnectorColumnTypeMapper;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.generation.codec.column.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.GeneratedColumnDefinition;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.types.VectorType;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

/** Actual parser/validator/SqlToRel fixtures, not authorization or Native execution substitutes. */
public final class GeneratedMutationFixture {
    private GeneratedMutationFixture() {}

    public static GeneratedColumnDefinition definition() {
        return GeneratedColumnDefinition.newBuilder().setTargetColumn("vector")
                .setCapability(BuiltInCapabilityCatalog.aiEmbeddingPlanRef()).setLogicalModelName("embedding_model")
                .addSourceColumns("title").addSourceColumns("body").setExecutionMode(ExecutionMode.EXECUTION_MODE_SYNC)
                .setOutputType(VectorLogicalType.create(3)).build();
    }

    public static LogicalTableModify plan(String sql) { return plan(sql, definition(), false); }

    public static LogicalTableModify plan(String sql, GeneratedColumnDefinition definition, boolean ast) {
        var root = CalciteSchema.createRootSchema(false);
        var schema = root.add("catalog", new AbstractSchema()).add("schema", new AbstractSchema());
        schema.add("documents", new DeclaredTable(metadata(definition)));
        schema.add("source_rows", new DeclaredTable(new ConnectorTableMetadata(new SchemaTableName("schema", "source_rows"),
                List.of("id", "tenant", "title", "body"), columns().subList(0, 4), Map.of())));
        var planner = new PangrovaPlanner(root, List.of("catalog", "schema"));
        return (LogicalTableModify) (ast ? planner.planFromAst(new AntlrSqlParser().parse(sql)) : planner.plan(sql)).plan();
    }

    public static ConnectorTableMetadata metadata(GeneratedColumnDefinition definition) {
        var definitions = GeneratedColumnSet.newBuilder().setSchemaVersion(GeneratedColumnMetadataCodec.SCHEMA_VERSION)
                .addDefinitions(definition).build();
        return new ConnectorTableMetadata(new SchemaTableName("schema", "documents"),
                List.of("id", "tenant", "title", "body", "vector"), columns(), GeneratedColumnMetadataCodec.encode(definitions));
    }

    /** Checks a literal through Calcite's storage-type casts, not arbitrary expressions. */
    public static String textLiteral(RexNode expression) {
        while (expression instanceof RexCall call && call.getKind() == SqlKind.CAST) {
            expression = call.getOperands().get(0);
        }
        return RexLiteral.stringValue(expression);
    }

    private static List<ConnectorTableMetadata.Column> columns() {
        return List.of(new ConnectorTableMetadata.Column("id", PangrovaType.BIGINT),
                new ConnectorTableMetadata.Column("tenant", PangrovaType.BIGINT),
                new ConnectorTableMetadata.Column("title", PangrovaType.VARCHAR),
                new ConnectorTableMetadata.Column("body", PangrovaType.VARCHAR),
                new ConnectorTableMetadata.Column("vector", PangrovaType.VECTOR, OptionalInt.of(5),
                        new VectorType(3, VectorType.ElementType.FLOAT)));
    }

    private static final class DeclaredTable extends AbstractTable {
        private final ConnectorTableMetadata metadata;

        DeclaredTable(ConnectorTableMetadata metadata) { this.metadata = metadata; }
        @Override public RelDataType getRowType(RelDataTypeFactory factory) {
            return ConnectorColumnTypeMapper.rowType(metadata.typedColumns(), factory);
        }
        @Override public <T> T unwrap(Class<T> type) {
            return type.isInstance(metadata) ? type.cast(metadata) : super.unwrap(type);
        }
    }
}
