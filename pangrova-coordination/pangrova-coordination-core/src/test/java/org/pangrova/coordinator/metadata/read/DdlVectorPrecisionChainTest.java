package org.pangrova.coordinator.metadata.read;

import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.execution.dispatch.task.CtasTask;
import org.pangrova.coordinator.query.statement.DdlExecutor;
import org.pangrova.parser.AstPrinter;
import org.pangrova.parser.ast.statement.CreateTableAsSelectStatement;
import org.pangrova.parser.ast.statement.CreateTableStatement;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.PlanFragment;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;
import org.pangrova.protocol.metadata.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.spi.connector.data.type.ArrayType;
import org.pangrova.spi.connector.data.type.VectorType;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual SQL, CREATE/CTAS declaration builders and metadata planner, without a substitute connector. */
class DdlVectorPrecisionChainTest {
    @ParameterizedTest
    @EnumSource(VectorType.ElementType.class)
    void createAndCtasPreserveCoordinatePrecisionThroughFinalLogicalDeclaration(VectorType.ElementType element) {
        var schema = CalciteSchema.createRootSchema(false);
        var compiler = compiler(schema);
        var create = (CreateTableStatement) compiler.parseToAst(
                "CREATE TABLE vectors (embedding VECTOR(3," + element.name() + "))");
        var declaration = DdlExecutor.declaredTableMetadata(new SchemaTableName("default", "vectors"), create);
        assertThat(declaration.typedColumns().get(0).parameterizedType()).isEqualTo(new VectorType(3, element));
        assertThat(declaration.properties()).containsEntry("pangrova.vector.dimension.embedding", "3");
        schema.add("vectors", new MetadataResolvedTable(declaration));

        var ctas = (CreateTableAsSelectStatement) compiler.parseToAst(
                "CREATE TABLE copied AS SELECT embedding AS \"result\" FROM vectors");
        // This is the real CTAS compile/derive path, retaining its existing single parser.
        var plan = compiler.compileToResult(new AstPrinter().print(ctas.query()));
        var columns = CtasTask.deriveColumnDefinitions(plan.toPlannedQuery());
        assertThat(plan.preOptimizationColumnNames()).containsExactly("result");
        assertThat(columns).singleElement().satisfies(column -> assertThat(column.name()).isEqualTo("result"));
        var copiedStatement = new CreateTableStatement(ctas.tableName(), columns, ctas.ifNotExists(),
                List.of(), List.of(), Optional.empty(), Map.of());
        var copied = DdlExecutor.declaredTableMetadata(new SchemaTableName("default", "copied"), copiedStatement);
        assertThat(copied.typedColumns().get(0).parameterizedType()).isEqualTo(new VectorType(3, element));
        schema.add("copied", new MetadataResolvedTable(copied));

        var selected = compiler.compileToRelNode("SELECT \"result\" FROM copied");
        var outputs = new PlanFragment(selected, -1, -1).resultColumnTypes();
        assertThat(outputs).singleElement().satisfies(output -> {
            assertThat(output.getColumnIndex()).isZero();
            assertThat(output.getLogicalType()).isEqualTo(VectorLogicalType.create(3,
                    element == VectorType.ElementType.FLOAT ? VectorLogicalType.FLOAT32 : VectorLogicalType.FLOAT64,
                    true, false));
        });
    }

    @Test
    void generatedDeclarationUsesFullCanonicalFloat32AndRejectsFloat64AtTypeValidation() {
        var compiler = new SqlCompiler();
        String suffix = " GENERATED ALWAYS AS (AI_EMBED('catalog.embed', body)) EMBEDDING MODE ASYNC)";
        var create = (CreateTableStatement) compiler.parseToAst(
                "CREATE TABLE docs (body STRING, embedding VECTOR(3)" + suffix);
        ConnectorTableMetadata declared = DdlExecutor.declaredTableMetadata(
                new SchemaTableName("default", "docs"), create);
        assertThat(GeneratedColumnMetadataCodec.decode(declared.properties()).getDefinitionsList())
                .singleElement().satisfies(definition ->
                        assertThat(definition.getOutputType()).isEqualTo(VectorLogicalType.create(3)));

        var doubleCreate = (CreateTableStatement) compiler.parseToAst(
                "CREATE TABLE docs (body STRING, embedding VECTOR(3,DOUBLE)" + suffix);
        assertThatThrownBy(() -> DdlExecutor.declaredTableMetadata(
                new SchemaTableName("default", "docs"), doubleCreate))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE))
                .hasMessageContaining("FLOAT32 VECTOR storage");
    }

    @Test
    void nestedDdlDeclarationAndDefaultFloatPreserveExistingStorageContracts() {
        var create = (CreateTableStatement) new SqlCompiler().parseToAst(
                "CREATE TABLE nested (default_vec VECTOR(3), vectors ARRAY(VECTOR(3,DOUBLE)))");
        var declaration = DdlExecutor.declaredTableMetadata(new SchemaTableName("default", "nested"), create);
        assertThat(declaration.typedColumns().get(0).parameterizedType())
                .isEqualTo(new VectorType(3, VectorType.ElementType.FLOAT));
        assertThat(declaration.typedColumns().get(1).parameterizedType())
                .isEqualTo(new ArrayType(new VectorType(3, VectorType.ElementType.DOUBLE)));
    }

    private static SqlCompiler compiler(CalciteSchema schema) {
        return new SqlCompiler(new PangrovaPlanner(schema), new SubstraitConverter(), new SubstraitPlanSerializer());
    }
}
