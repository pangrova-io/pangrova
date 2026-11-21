package org.pangrova.planner.metadata.catalog;

import org.pangrova.types.enums.VectorElementType;

import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.pangrova.planner.analysis.PangrovaPlanner;
import org.pangrova.planner.analysis.PlanFragment;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.planner.type.StorageVectorRelDataType;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.types.VectorType;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real SQL planning over the production metadata table: unresolved storage cannot become executable SQL. */
class MetadataStorageTypeAdmissionTest {
    @ParameterizedTest
    @EnumSource(VectorElementType.class)
    void realSqlProjectionAliasAndOuterJoinRetainStorageVectorIdentity(VectorElementType element) {
        var columns = List.of(new ConnectorTableMetadata.Column("embedding", PangrovaType.VECTOR,
                OptionalInt.empty(), new VectorType(3, element)));
        var metadata = new ConnectorTableMetadata(new SchemaTableName("default", "vectors"),
                List.of("embedding"), columns, Map.of());
        var schema = CalciteSchema.createRootSchema(false);
        schema.add("vectors", new MetadataResolvedTable(metadata));
        var compiler = new SqlCompiler(new PangrovaPlanner(schema), new SubstraitConverter(),
                new SubstraitPlanSerializer());

        for (String sql : List.of("SELECT \"embedding\" AS \"result\" FROM \"vectors\"",
                "SELECT b.\"embedding\" AS \"result\" FROM \"vectors\" a "
                        + "LEFT JOIN \"vectors\" b ON TRUE",
                "SELECT \"embedding\" FROM \"vectors\" UNION ALL SELECT \"embedding\" FROM \"vectors\"",
                "SELECT CASE WHEN TRUE THEN \"embedding\" ELSE NULL END FROM \"vectors\"")) {
            var rel = compiler.compileToRelNode(sql);
            var fragment = new PlanFragment(rel, -1, -1);
            assertThat(fragment.resultColumnTypes()).hasSize(1);
            assertThat(fragment.resultColumnTypes().get(0).getColumnIndex()).isZero();
            var result = rel.getRowType().getFieldList().get(0).getType();
            assertThat(result).isInstanceOf(StorageVectorRelDataType.class);
            assertThat(((StorageVectorRelDataType) result).logicalType()).isEqualTo(
                    VectorLogicalType.create(3, element == VectorElementType.FLOAT
                            ? VectorLogicalType.FLOAT32 : VectorLogicalType.FLOAT64, true, false));
            assertThat(fragment.resultColumnTypes().get(0).getLogicalType())
                    .isEqualTo(((StorageVectorRelDataType) result).logicalType());
        }
        // SQL explicitly requests an ordinary ARRAY; no storage-vector declaration may survive this cast.
        var cast = compiler.compileToRelNode("SELECT CAST(\"embedding\" AS ARRAY(DOUBLE)) FROM \"vectors\"");
        assertThat(cast.getRowType().getFieldList().get(0).getType()).isNotInstanceOf(StorageVectorRelDataType.class);
        assertThat(new PlanFragment(cast, -1, -1).resultColumnTypes()).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(value = PangrovaType.class, names = {"UNKNOWN", "ARRAY", "VECTOR", "MAP", "STRUCT", "DECIMAL"})
    void incompleteDeclarationIsRejectedDuringSqlPlanning(PangrovaType type) {
        var metadata = new ConnectorTableMetadata(new SchemaTableName("default", "broken"), List.of("value"),
                List.of(new ConnectorTableMetadata.Column("value", type)), Map.of());
        var schema = CalciteSchema.createRootSchema(false);
        schema.add("broken", new MetadataResolvedTable(metadata));
        var compiler = new SqlCompiler(new PangrovaPlanner(schema), new SubstraitConverter(),
                new SubstraitPlanSerializer());

        assertThatThrownBy(() -> compiler.compile("SELECT \"value\" FROM \"broken\""))
                .hasStackTraceContaining(type + " requires a concrete matching storage type declaration");
    }
}
