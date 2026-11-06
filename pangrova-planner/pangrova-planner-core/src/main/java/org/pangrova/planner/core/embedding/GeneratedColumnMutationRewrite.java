package org.pangrova.planner.core.embedding;

import org.pangrova.planner.core.metadata.PlanTable;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.pangrova.planner.core.declaration.CompiledWriteDeclaration;
import org.pangrova.planner.core.validation.PangrovaSqlOperatorTable;
import org.pangrova.generation.codec.column.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.GeneratedColumnDefinition;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.pangrova.planner.core.embedding.GeneratedColumnMutationExpressions.normalize;

/** Injects generated NEW/action expressions exactly once, before mutation security policies and COW lowering. */
@Internal
public final class GeneratedColumnMutationRewrite {
    private GeneratedColumnMutationRewrite() {}

    /** Uses only metadata attached to the actual compiled target; no catalog re-read or executor directives. */
    public static RelNode rewrite(RelNode relation, SqlNode statement, PangrovaSqlOperatorTable operators) {
        if (!(relation instanceof LogicalTableModify modify) || modify.getOperation() == TableModify.Operation.DELETE) {
            return relation;
        }
        var retained = PlanTable.require(modify).unwrap(ConnectorTableMetadata.class);
        if (retained == null || !hasDeclaration(retained)) return relation;
        var metadata = CompiledWriteDeclaration.metadata(modify).orElseThrow();
        var definitions = GeneratedColumnMetadataCodec.decode(metadata.properties()).getDefinitionsList();
        requireManagedTargets(statement, definitions);
        List<GeneratedColumnDefinition> synchronous = definitions.stream()
                .filter(value -> value.getExecutionMode() == ExecutionMode.EXECUTION_MODE_SYNC).toList();
        if (synchronous.isEmpty()) return relation;
        var expressions = new GeneratedColumnMutationExpressions(modify, metadata, operators);
        return switch (modify.getOperation()) {
            case INSERT -> insert(modify, synchronous, expressions);
            case UPDATE -> update(modify, synchronous, expressions);
            case MERGE -> merge(modify, statement, synchronous, expressions);
            default -> throw invalid("Unsupported generated mutation action: " + modify.getOperation());
        };
    }

    private static boolean hasDeclaration(ConnectorTableMetadata metadata) {
        return metadata.properties().containsKey(GeneratedColumnMetadataCodec.PAYLOAD_PROPERTY)
                || metadata.properties().containsKey(GeneratedColumnMetadataCodec.SHA256_PROPERTY);
    }

    private static void requireManagedTargets(SqlNode statement, List<GeneratedColumnDefinition> definitions) {
        if (definitions.isEmpty()) return;
        Set<String> managed = definitions.stream().map(value -> normalize(value.getTargetColumn()))
                .collect(Collectors.toUnmodifiableSet());
        if (statement instanceof SqlInsert insert) {
            requireColumns(insert.getTargetColumnList(), managed, true);
        } else if (statement instanceof SqlUpdate update) {
            requireColumns(update.getTargetColumnList(), managed, false);
        } else if (statement instanceof SqlMerge merge) {
            var insert = merge.getInsertCall();
            var update = merge.getUpdateCall();
            if (insert != null) {
                requireColumns(insert.getTargetColumnList(), managed, true);
            }
            if (update != null) {
                requireColumns(update.getTargetColumnList(), managed, false);
            }
        } else throw invalid("Generated mutation lacks its validated SQL action");
    }

    private static void requireColumns(SqlNodeList columns, Set<String> managed, boolean insert) {
        if (columns == null) {
            if (insert) throw invalid("Generated-column INSERT requires an explicit non-generated target column list");
            throw invalid("Generated UPDATE lacks its target column list");
        }
        for (SqlNode node : columns) {
            if (!(node instanceof SqlIdentifier identifier)) throw invalid("Invalid generated mutation target");
            if (managed.contains(normalize(identifier.names.get(identifier.names.size() - 1)))) {
                throw invalid("Explicit writes to a generated column are forbidden: " + identifier);
            }
        }
    }

    private static RelNode insert(LogicalTableModify modify, List<GeneratedColumnDefinition> definitions,
            GeneratedColumnMutationExpressions expressions) {
        RelNode input = modify.getInput();
        List<RexNode> values = input instanceof LogicalProject project
                ? new ArrayList<>(project.getProjects()) : inputReferences(input);
        if (values.size() != PlanTable.require(modify).getRowType().getFieldCount()) {
            throw invalid("Generated INSERT lacks the complete target NEW row");
        }
        var targetFields = PlanTable.require(modify).getRowType().getFieldList();
        var builder = modify.getCluster().getRexBuilder();
        for (int index = 0; index < values.size(); index++) {
            var type = targetFields.get(index).getType();
            if (!values.get(index).getType().equals(type)) {
                // Keep the complete storage row exact, including nullability of VALUES literals.
                values.set(index, builder.makeAbstractCast(type, values.get(index)));
            }
        }
        Map<String, RexNode> environment = environment(modify, values, 0);
        for (var definition : definitions) {
            values.set(expressions.column(definition.getTargetColumn()),
                    expressions.embedding(definition, environment));
        }
        RelNode child = input instanceof LogicalProject project ? project.getInput() : input;
        var project = input instanceof LogicalProject retained
                ? retained.copy(retained.getTraitSet(), child, values, PlanTable.require(modify).getRowType())
                : LogicalProject.create(child, List.of(), values, PlanTable.require(modify).getRowType());
        return modify.copy(modify.getTraitSet(), List.of(project));
    }

    private static RelNode update(LogicalTableModify modify, List<GeneratedColumnDefinition> definitions,
            GeneratedColumnMutationExpressions expressions) {
        LogicalProject project = requireProject(modify);
        int count = PlanTable.require(modify).getRowType().getFieldCount();
        if (project.getProjects().size() != count + updateColumns(modify).size()) {
            throw invalid("Generated UPDATE requires physical target columns followed by original SET values");
        }
        return assignments(modify, project, definitions, expressions, count, 0);
    }

    private static RelNode merge(LogicalTableModify modify, SqlNode statement,
            List<GeneratedColumnDefinition> definitions, GeneratedColumnMutationExpressions expressions) {
        if (!(statement instanceof SqlMerge merge)) throw invalid("Generated MERGE lacks its validated actions");
        LogicalProject project = requireProject(modify);
        if (!(project.getInput() instanceof LogicalJoin)) throw invalid("Generated MERGE requires source/target join");
        int count = PlanTable.require(modify).getRowType().getFieldCount();
        int inserts = merge.getInsertCall() == null ? 0 : count;
        int targets = merge.getUpdateCall() == null ? 0 : count;
        if (project.getProjects().size() != inserts + targets + updateColumns(modify).size()) {
            throw invalid("Generated MERGE action layout differs from the validated target");
        }
        List<RexNode> values = new ArrayList<>(project.getProjects());
        if (inserts != 0) {
            var environment = environment(modify, values, 0);
            for (var definition : definitions) {
                values.set(expressions.column(definition.getTargetColumn()),
                        expressions.embedding(definition, environment));
            }
        }
        var rewritten = project.copy(project.getTraitSet(), project.getInput(), values, project.getRowType());
        if (updateColumns(modify).isEmpty()) return modify.copy(modify.getTraitSet(), List.of(rewritten));
        return assignments(modify, rewritten, definitions, expressions, inserts + targets, inserts);
    }

    private static RelNode assignments(LogicalTableModify modify, LogicalProject project,
            List<GeneratedColumnDefinition> definitions, GeneratedColumnMutationExpressions expressions,
            int setOffset, int targetOffset) {
        var columns = new ArrayList<>(updateColumns(modify));
        var values = new ArrayList<>(project.getProjects());
        var names = new ArrayList<>(project.getRowType().getFieldNames());
        Map<String, RexNode> environment = environment(modify, values, targetOffset);
        for (int index = 0; index < columns.size(); index++) {
            environment.put(normalize(columns.get(index)), values.get(setOffset + index));
        }
        Set<String> changed = columns.stream().map(GeneratedColumnMutationExpressions::normalize)
                .collect(Collectors.toUnmodifiableSet());
        for (var definition : definitions) {
            if (definition.getSourceColumnsList().stream().map(GeneratedColumnMutationExpressions::normalize)
                    .noneMatch(changed::contains)) continue;
            values.add(expressions.embedding(definition, environment));
            columns.add(PlanTable.require(modify).getRowType().getFieldNames()
                    .get(expressions.column(definition.getTargetColumn())));
            names.add("$generated$" + values.size());
        }
        if (columns.equals(updateColumns(modify))) return modify.copy(modify.getTraitSet(), List.of(project));
        var result = LogicalProject.create(project.getInput(), project.getHints(),
                values, names, project.getVariablesSet());
        List<RexNode> sourceExpressions = modify.getOperation() == TableModify.Operation.UPDATE
                ? inputReferences(result).subList(setOffset, values.size()) : modify.getSourceExpressionList();
        return new LogicalTableModify(modify.getCluster(), modify.getTraitSet(), PlanTable.require(modify),
                modify.getCatalogReader(), result, modify.getOperation(),
                columns, sourceExpressions, modify.isFlattened());
    }

    private static Map<String, RexNode> environment(LogicalTableModify modify, List<RexNode> values, int offset) {
        Map<String, RexNode> environment = new HashMap<>();
        var names = PlanTable.require(modify).getRowType().getFieldNames();
        for (int index = 0; index < names.size(); index++) {
            environment.put(normalize(names.get(index)), values.get(offset + index));
        }
        return environment;
    }

    private static List<RexNode> inputReferences(RelNode input) {
        List<RexNode> references = new ArrayList<>();
        for (int index = 0; index < input.getRowType().getFieldCount(); index++) {
            references.add(input.getCluster().getRexBuilder().makeInputRef(input, index));
        }
        return references;
    }

    private static LogicalProject requireProject(LogicalTableModify modify) {
        if (!(modify.getInput() instanceof LogicalProject project)) {
            throw invalid("Generated action requires a NEW project");
        }
        return project;
    }

    private static List<String> updateColumns(LogicalTableModify modify) {
        var columns = modify.getUpdateColumnList();
        if (columns == null) throw invalid("Generated UPDATE/MERGE lacks its compiled SET column list");
        return columns;
    }

    private static IllegalArgumentException invalid(String message) { return new IllegalArgumentException(message); }
}
