package org.pangrova.planner.embedding;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.generation.codec.column.GeneratedColumnMetadataCodec;
import org.pangrova.planner.type.ModelBoundVectorRelDataType;
import org.pangrova.planner.type.StorageVectorRelDataType;
import org.pangrova.planner.validation.PangrovaSqlOperatorTable;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.v2.GeneratedColumnDefinition;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.rel.metadata.RelColumnOrigin;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Rewrites a text literal used as the query vector of a vector distance/similarity
 * function into a governed AI_EMBED(...,'QUERY') expression when the referenced
 * column is an auto-embedding column.
 */
public final class QueryTextVectorRewrite {
    private static final Set<String> VECTOR_TEXT_FUNCTIONS = Set.of(
            "L2_DISTANCE", "COSINE_DISTANCE", "INNER_PRODUCT", "COSINE_SIMILARITY");
    private static final String EMBEDDING_PURPOSE = "QUERY";

    private QueryTextVectorRewrite() {}

    /** Rewrites text query vectors after conversion, while table identity is still attached. */
    public static RelNode rewrite(RelNode relation, PangrovaSqlOperatorTable operators) {
        if (relation == null) {
            return null;
        }
        return relation.accept(new Shuttle(operators));
    }

    private static final class Shuttle extends RelShuttleImpl {
        private final PangrovaSqlOperatorTable operators;

        Shuttle(PangrovaSqlOperatorTable operators) {
            this.operators = java.util.Objects.requireNonNull(operators, "operators");
        }

        @Override
        public RelNode visit(LogicalProject project) {
            RelNode input = project.getInput().accept(this);
            List<RexNode> projects = rewriteAll(project.getProjects(), input);
            return project.copy(project.getTraitSet(), input, projects, project.getRowType());
        }

        @Override
        public RelNode visit(LogicalFilter filter) {
            RelNode input = filter.getInput().accept(this);
            RexNode condition = rewrite(filter.getCondition(), input);
            return filter.copy(filter.getTraitSet(), input, condition);
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            RelNode left = join.getLeft().accept(this);
            RelNode right = join.getRight().accept(this);
            RexNode condition = rewrite(join.getCondition(), join);
            return join.copy(join.getTraitSet(), condition, left, right,
                    join.getJoinType(), join.isSemiJoinDone());
        }

        @Override
        public RelNode visit(LogicalSort sort) {
            RelNode input = sort.getInput().accept(this);
            return sort.copy(sort.getTraitSet(), input, sort.getCollation(),
                    sort.offset, sort.fetch);
        }

        private List<RexNode> rewriteAll(List<RexNode> expressions, RelNode context) {
            List<RexNode> rewritten = new ArrayList<>(expressions.size());
            for (RexNode expression : expressions) {
                rewritten.add(rewrite(expression, context));
            }
            return rewritten;
        }

        private RexNode rewrite(RexNode expression, RelNode context) {
            if (expression == null) {
                return null;
            }
            return expression.accept(new RexShuttle() {
                @Override
                public RexNode visitCall(RexCall call) {
                    RexNode rewritten = super.visitCall(call);
                    if (rewritten instanceof RexCall rewrittenCall) {
                        return rewriteTextVector(rewrittenCall, context, operators);
                    }
                    return rewritten;
                }
            });
        }
    }

    private static RexNode rewriteTextVector(RexCall call, RelNode context,
            PangrovaSqlOperatorTable operators) {
        if (!VECTOR_TEXT_FUNCTIONS.contains(call.getOperator().getName().toUpperCase(Locale.ROOT))
                || call.getOperands().size() != 2) {
            return call;
        }
        RexNode vectorOperand = call.getOperands().get(0);
        RexLiteral text = textLiteral(call.getOperands().get(1));
        if (text != null) {
            return embedTextQuery(call, vectorOperand, text, context, operators);
        }
        if (textLiteral(vectorOperand) != null) {
            throw invalid(call.getOperator().getName()
                    + " requires the vector column as its first argument and a text literal second");
        }
        return call;
    }

    private static RexNode embedTextQuery(RexCall call, RexNode vectorOperand, RexLiteral text,
            RelNode context, PangrovaSqlOperatorTable operators) {
        RexInputRef reference = inputReference(vectorOperand);
        String textValue = RexLiteral.stringValue(text);
        if (textValue == null || textValue.isBlank()) {
            throw invalid("Vector query text must not be blank");
        }
        RelColumnOrigin origin = singleOrigin(reference, context);
        RelOptTable table = origin.getOriginTable();
        ConnectorTableMetadata metadata = table.unwrap(ConnectorTableMetadata.class);
        if (metadata == null) {
            throw invalid("Text vector shorthand requires table metadata for "
                    + table.getQualifiedName());
        }
        String columnName = table.getRowType().getFieldNames()
                .get(origin.getOriginColumnOrdinal());
        GeneratedColumnDefinition definition = definition(metadata, columnName);
        int definitionDimension = VectorLogicalType.requireDimension(definition.getOutputType());
        int columnDimension = columnDimension(table, origin.getOriginColumnOrdinal(), columnName);
        if (definitionDimension != columnDimension) {
            throw invalid("Auto-embedding column " + columnName + " declares dimension "
                    + columnDimension + " but model " + definition.getLogicalModelName()
                    + " declares " + definitionDimension);
        }
        RexBuilder builder = context.getCluster().getRexBuilder();
        RexNode embedded = builder.makeCall(modelBoundVector(context.getCluster()),
                embeddingOperator(operators),
                List.of(builder.makeLiteral(definition.getLogicalModelName()), text,
                        builder.makeLiteral(EMBEDDING_PURPOSE)));
        List<RexNode> operands = new ArrayList<>(call.getOperands());
        operands.set(1, embedded);
        return builder.makeCall(call.getType(), call.getOperator(), operands);
    }

    private static GeneratedColumnDefinition definition(ConnectorTableMetadata metadata,
            String columnName) {
        for (GeneratedColumnDefinition definition : GeneratedColumnMetadataCodec
                .decode(metadata.properties()).getDefinitionsList()) {
            if (!definition.getTargetColumn().equalsIgnoreCase(columnName)) {
                continue;
            }
            if (!definition.getCapability().equals(BuiltInCapabilityCatalog.aiEmbeddingPlanRef())
                    || definition.getLogicalModelName().isBlank()) {
                throw invalid("Auto-embedding column " + columnName
                        + " does not use the canonical text embedding capability");
            }
            return definition;
        }
        throw invalid("Text vector shorthand requires an auto-embedding column: " + columnName);
    }

    private static int columnDimension(RelOptTable table, int ordinal, String columnName) {
        RelDataType type = table.getRowType().getFieldList().get(ordinal).getType();
        if (!(type instanceof StorageVectorRelDataType vector)) {
            throw invalid("Auto-embedding column " + columnName + " is not a stored VECTOR");
        }
        return VectorLogicalType.requireDimension(vector.logicalType());
    }

    private static RelColumnOrigin singleOrigin(RexInputRef reference, RelNode context) {
        var origins = context.getCluster().getMetadataQuery()
                .getColumnOrigins(context, reference.getIndex());
        if (origins == null || origins.size() != 1) {
            throw invalid("Text vector shorthand requires an unambiguous table column");
        }
        RelColumnOrigin origin = origins.iterator().next();
        if (origin == null || origin.isDerived()) {
            throw invalid("Text vector shorthand requires a direct table column");
        }
        return origin;
    }

    private static RexInputRef inputReference(RexNode operand) {
        RexNode current = operand;
        while (current instanceof RexCall call && call.getKind() == SqlKind.CAST
                && call.getOperands().size() == 1) {
            current = call.getOperands().get(0);
        }
        if (current instanceof RexInputRef reference) {
            return reference;
        }
        throw invalid("Text vector shorthand requires a table column as the first argument");
    }

    private static RexLiteral textLiteral(RexNode node) {
        RexNode current = node;
        while (current instanceof RexCall call && call.getKind() == SqlKind.CAST
                && call.getOperands().size() == 1) {
            current = call.getOperands().get(0);
        }
        if (current instanceof RexLiteral literal
                && SqlTypeUtil.inCharFamily(literal.getType())
                && literal.getValue() != null) {
            return literal;
        }
        return null;
    }

    private static SqlOperator embeddingOperator(PangrovaSqlOperatorTable operators) {
        return operators.getOperatorList().stream()
                .filter(operator -> "AI_EMBED".equals(operator.getName()))
                .findFirst()
                .orElseThrow(() -> invalid("AI_EMBED operator is unavailable"));
    }

    private static RelDataType modelBoundVector(RelOptCluster cluster) {
        return new ModelBoundVectorRelDataType(
                cluster.getTypeFactory().createSqlType(SqlTypeName.REAL), true);
    }

    private static PangrovaException invalid(String message) {
        return new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message);
    }
}
