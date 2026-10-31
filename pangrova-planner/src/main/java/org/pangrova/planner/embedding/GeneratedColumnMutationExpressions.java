package org.pangrova.planner.embedding;

import org.pangrova.planner.PlanTable;

import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.planner.validation.PangrovaSqlOperatorTable;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.GeneratedColumnDefinition;
import org.pangrova.connector.spi.data.type.PangrovaType;
import org.pangrova.connector.spi.data.type.VectorType;
import org.pangrova.connector.spi.metadata.table.ConnectorTableMetadata;
import org.pangrova.ai.api.embedding.EmbeddingPurpose;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Builds typed AI expressions from the compiled declaration and SQL's simultaneous NEW environment. */
final class GeneratedColumnMutationExpressions {
    private final TableModify modify;
    private final ConnectorTableMetadata metadata;
    private final RexBuilder builder;
    private final SqlOperator embedding;
    private final SqlOperator concat;

    GeneratedColumnMutationExpressions(TableModify modify, ConnectorTableMetadata metadata,
            PangrovaSqlOperatorTable operators) {
        this.modify = modify;
        this.metadata = metadata;
        this.builder = modify.getCluster().getRexBuilder();
        this.embedding = operator(operators, "AI_EMBED");
        this.concat = operator(operators, "CONCAT");
    }

    RexNode embedding(GeneratedColumnDefinition definition, Map<String, RexNode> newValues) {
        int target = column(definition.getTargetColumn());
        requireEmbedding(definition, target);
        List<RexNode> sources = new ArrayList<>();
        for (String name : definition.getSourceColumnsList()) {
            int index = column(name);
            var sourceType = metadata.typedColumns().get(index).type();
            if (sourceType != PangrovaType.VARCHAR) {
                throw invalid("Generated embedding source must be text: " + name);
            }
            RexNode value = newValues.get(normalize(name));
            if (value == null) throw invalid("Generated embedding lacks a NEW source: " + name);
            sources.add(value);
        }
        return builder.makeCall(PlanTable.require(modify)
                .getRowType().getFieldList().get(target).getType(), embedding,
                List.of(builder.makeLiteral(definition.getLogicalModelName()), source(sources),
                        builder.makeLiteral(EmbeddingPurpose.DOCUMENT.name())));
    }

    int column(String name) {
        for (int index = 0; index < metadata.typedColumns().size(); index++) {
            if (metadata.typedColumns().get(index).name().equalsIgnoreCase(name)) return index;
        }
        throw invalid("Generated column is absent from the compiled declaration: " + name);
    }

    private void requireEmbedding(GeneratedColumnDefinition definition, int target) {
        if (!definition.getCapability().equals(BuiltInCapabilityCatalog.aiEmbeddingPlanRef())
                || definition.getLogicalModelName().isBlank() || definition.getOptionsCount() != 0) {
            throw invalid("SYNC generated expressions require the canonical text embedding capability/model");
        }
        int dimension = VectorLogicalType.requireDimension(definition.getOutputType());
        var column = metadata.typedColumns().get(target);
        if (column.type() != PangrovaType.VECTOR
                || !(column.parameterizedType() instanceof VectorType physical)
                || physical.elementType() != VectorType.ElementType.FLOAT
                || physical.dimension() != dimension) {
            throw invalid("Generated VECTOR output differs from its compiled storage declaration");
        }
    }

    private RexNode source(List<RexNode> sources) {
        if (sources.size() == 1) return sources.get(0);
        List<RexNode> arguments = new ArrayList<>();
        for (RexNode value : sources) {
            if (!arguments.isEmpty()) arguments.add(builder.makeLiteral(GeneratedColumnSourceEncoding.SEPARATOR));
            arguments.add(value);
        }
        var type = modify.getCluster().getTypeFactory().createTypeWithNullability(
                modify.getCluster().getTypeFactory().createSqlType(SqlTypeName.VARCHAR), true);
        // Strict concat propagates NULL; never substitute an empty string for a NULL source.
        return builder.makeCall(type, concat, arguments);
    }

    private static SqlOperator operator(PangrovaSqlOperatorTable operators, String name) {
        return operators.getOperatorList().stream().filter(value -> value.getName().equals(name))
                .findFirst().orElseThrow(() -> invalid("Generated expression operator is unavailable: " + name));
    }

    static String normalize(String name) { return name.toLowerCase(Locale.ROOT); }

    private static IllegalArgumentException invalid(String message) { return new IllegalArgumentException(message); }
}
