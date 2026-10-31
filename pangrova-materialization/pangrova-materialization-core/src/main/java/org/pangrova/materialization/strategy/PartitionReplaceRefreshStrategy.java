package org.pangrova.materialization.strategy;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.GenerationState;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.PartitionAlignment;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.domain.RefreshPlan;
import org.pangrova.materialization.domain.StrategyId;
import org.pangrova.materialization.extension.RefreshContext;
import org.pangrova.materialization.extension.RefreshStrategy;
import org.pangrova.materialization.extension.StrategyCompatibility;
import org.pangrova.connector.spi.materialization.ChangeProof;
import org.pangrova.connector.spi.materialization.ChangeSetMode;
import org.pangrova.connector.spi.materialization.ChangeSetPlan;
import org.pangrova.connector.spi.materialization.PartitionChange;
import org.pangrova.connector.spi.materialization.PartitionValue;
import org.pangrova.connector.spi.materialization.SourceRef;
import org.pangrova.connector.spi.materialization.SourceSnapshotRef;

/**
 * Recomputes only the source partitions the connector reported as changed.
 *
 * <p>The canonical statement keeps every base row outside the changed
 * partitions and replaces the changed partitions with the definition
 * evaluated over the current source. It is only valid when the definition is
 * a single-table projection or partition-local aggregate, the base generation
 * was produced by the same definition revision, and every changed partition
 * column is exposed by a directly derived output column. Partition predicates
 * are null-safe: a base row with a null partition value is never dropped from
 * the unchanged part.
 */
public final class PartitionReplaceRefreshStrategy implements RefreshStrategy {

    @Override
    public StrategyId id() {
        return StrategyId.PARTITION_REPLACE;
    }

    @Override
    public RefreshKind kind() {
        return RefreshKind.IncrementalPartitionReplace.INSTANCE;
    }

    @Override
    public StrategyCompatibility validate(RefreshContext context) {
        return baseProblem(context)
                .or(() -> alignmentProblem(context))
                .or(() -> partitionProblem(context))
                .map(StrategyCompatibility::incompatible)
                .orElseGet(StrategyCompatibility::supported);
    }

    private static Optional<String> baseProblem(RefreshContext context) {
        if (context.baseGeneration().isEmpty()) {
            return Optional.of("partition replacement requires a published base generation");
        }
        Generation base = context.baseGeneration().get();
        if (base.state() != GenerationState.Published.INSTANCE) {
            return Optional.of(
                    "base generation " + base.id() + " is " + base.state() + ", not published");
        }
        if (base.definitionRevision() != context.definition().revision()) {
            return Optional.of("base generation was computed from definition revision "
                    + base.definitionRevision() + ", not " + context.definition().revision());
        }
        if (!base.sourceVector().snapshots().keySet()
                .equals(context.sourceVector().snapshots().keySet())) {
            return Optional.of("base generation and planned sources differ");
        }
        if (context.sourceVector().snapshots().size() != 1) {
            return Optional.of("partition replacement requires exactly one source table");
        }
        return Optional.empty();
    }

    private static Optional<String> alignmentProblem(RefreshContext context) {
        if (context.partitionAlignment().shape() == PartitionAlignment.Shape.UNSUPPORTED) {
            return Optional.of("definition shape cannot be recomputed one partition at a time");
        }
        if (context.changeProof().isEmpty()) {
            return Optional.of("partition replacement requires a connector change proof");
        }
        ChangeProof proof = context.changeProof().get();
        if (!proof.complete()) {
            return Optional.of(
                    "change proof for " + proof.source().canonicalName() + " is incomplete");
        }
        if (proof.mode() != ChangeSetMode.PARTITION_REPLACE) {
            return Optional.of("change proof mode " + proof.mode() + " is not PARTITION_REPLACE");
        }
        SourceRef source = context.sourceVector().snapshots().keySet().iterator().next();
        if (!proof.source().equals(source)) {
            return Optional.of("change proof belongs to " + proof.source().canonicalName());
        }
        SourceSnapshotRef target = context.sourceVector().snapshotOf(source).orElseThrow();
        if (!proof.toToken().equals(target.snapshotToken())) {
            return Optional.of("change proof reaches snapshot " + proof.toToken()
                    + " instead of the planned " + target.snapshotToken());
        }
        ChangeSetPlan changeSet = context.changeSets().get(source);
        if (changeSet == null
                || changeSet.mode() != ChangeSetMode.PARTITION_REPLACE
                || !changeSet.fromSnapshot().snapshotToken().equals(proof.fromToken())
                || !changeSet.toSnapshot().snapshotToken().equals(proof.toToken())) {
            return Optional.of("change proof does not match the planned change set");
        }
        return Optional.empty();
    }

    private static Optional<String> partitionProblem(RefreshContext context) {
        SourceRef source = context.sourceVector().snapshots().keySet().iterator().next();
        List<String> columns = partitionColumns(
                context.changeSets().get(source).changedPartitions());
        if (columns == null) {
            return Optional.of("changed partitions do not share one partition column set");
        }
        PartitionAlignment alignment = context.partitionAlignment();
        for (String column : columns) {
            Optional<PartitionAlignment.Column> mapped = alignment.columnFor(column);
            if (mapped.isEmpty()) {
                return Optional.of("partition column " + column
                        + " is not exposed by a directly derived output column");
            }
            if (alignment.shape() == PartitionAlignment.Shape.AGGREGATE
                    && !mapped.get().groupingKey()) {
                return Optional.of("partition column " + column + " is not a grouping key");
            }
        }
        return Optional.empty();
    }

    @Override
    public RefreshPlan plan(RefreshContext context) {
        StrategyCompatibility compatibility = validate(context);
        if (!compatibility.compatible()) {
            throw MaterializationException.incrementalUnsupported(compatibility.reason());
        }
        Generation base = context.baseGeneration().orElseThrow();
        SourceRef source = context.sourceVector().snapshots().keySet().iterator().next();
        ChangeSetPlan changeSet = context.changeSets().get(source);
        List<PartitionChange> changes = changeSet.changedPartitions();
        PartitionAlignment alignment = context.partitionAlignment();
        String canonicalSql = changes.isEmpty()
                ? copyOf(base)
                : copyOf(base) + " WHERE " + unchangedPredicate(changes, alignment)
                        + " UNION ALL SELECT * FROM ("
                        + RefreshSql.definitionQuery(context.definition().sql())
                        + ") AS pg_partition_refresh WHERE "
                        + changedPredicate(changes, alignment);
        Map<SourceRef, ChangeSetPlan> changeSets = new LinkedHashMap<>();
        changeSets.put(source, changeSet);
        String fingerprint = RefreshSql.sha256("partition-replace:v1|"
                + context.definition().fingerprint() + '|'
                + base.physicalTable().qualifiedName() + '|'
                + base.sourceVector().canonicalHash() + '|'
                + context.sourceVector().canonicalHash() + '|'
                + changeSet.planFingerprint() + '|'
                + context.candidateTable().qualifiedName() + '|'
                + changedPredicate(changes, alignment));
        return new RefreshPlan(kind(), canonicalSql, changeSets, fingerprint);
    }

    @Override
    public void verify(RefreshPlan plan, ChangeProof proof) {
        if (!plan.kind().equals(kind())) {
            throw MaterializationException.requestRejected(
                    "partition replacement strategy received a " + plan.kind() + " plan");
        }
        if (!proof.complete()) {
            throw MaterializationException.incrementalUnsupported(
                    "incomplete change proof for " + proof.source().canonicalName()
                            + " from " + proof.fromToken() + " to " + proof.toToken());
        }
        if (proof.mode() != ChangeSetMode.PARTITION_REPLACE) {
            throw MaterializationException.incrementalUnsupported(
                    "change proof mode " + proof.mode() + " is not PARTITION_REPLACE");
        }
        ChangeSetPlan changeSet = plan.changeSets().get(proof.source());
        if (changeSet == null
                || changeSet.mode() != ChangeSetMode.PARTITION_REPLACE
                || !changeSet.fromSnapshot().snapshotToken().equals(proof.fromToken())
                || !changeSet.toSnapshot().snapshotToken().equals(proof.toToken())) {
            throw MaterializationException.requestRejected(
                    "change proof does not cover the planned change set for "
                            + proof.source().canonicalName());
        }
    }

    private static String copyOf(Generation base) {
        return "SELECT * FROM " + RefreshSql.qualified(base.physicalTable());
    }

    /**
     * Returns the shared partition column list, or null when the connector
     * reported inconsistent partition shapes inside one interval.
     */
    private static List<String> partitionColumns(List<PartitionChange> changes) {
        List<String> columns = null;
        for (PartitionChange change : changes) {
            List<String> current = change.values().stream()
                    .map(PartitionValue::column)
                    .toList();
            if (columns == null) {
                columns = current;
            } else if (!columns.equals(current)) {
                return null;
            }
        }
        return columns == null ? List.of() : columns;
    }

    /** Predicate selecting the rows of every changed partition. */
    private static String changedPredicate(
            List<PartitionChange> changes, PartitionAlignment alignment) {
        List<String> partitions = new ArrayList<>(changes.size());
        for (PartitionChange change : changes) {
            StringBuilder predicate = new StringBuilder();
            for (PartitionValue value : change.values()) {
                String column = quoteOutput(value.column(), alignment);
                if (!predicate.isEmpty()) {
                    predicate.append(" AND ");
                }
                predicate.append(value.literal() == null
                        ? column + " IS NULL" : column + " = " + value.literal());
            }
            partitions.add('(' + predicate.toString() + ')');
        }
        return String.join(" OR ", partitions);
    }

    /**
     * Predicate keeping the rows of every unchanged partition.
     *
     * <p>Comparing a null partition value with a literal yields NULL, and
     * {@code NOT NULL} would silently drop that row. Each column difference is
     * therefore written as {@code IS NULL OR <>}, which never evaluates to
     * NULL.
     */
    private static String unchangedPredicate(
            List<PartitionChange> changes, PartitionAlignment alignment) {
        List<String> partitions = new ArrayList<>(changes.size());
        for (PartitionChange change : changes) {
            StringBuilder predicate = new StringBuilder();
            for (PartitionValue value : change.values()) {
                String column = quoteOutput(value.column(), alignment);
                if (!predicate.isEmpty()) {
                    predicate.append(" OR ");
                }
                predicate.append(value.literal() == null
                        ? column + " IS NOT NULL"
                        : "(" + column + " IS NULL OR " + column + " <> " + value.literal() + ")");
            }
            partitions.add('(' + predicate.toString() + ')');
        }
        return String.join(" AND ", partitions);
    }

    private static String quoteOutput(String sourceColumn, PartitionAlignment alignment) {
        PartitionAlignment.Column mapped = alignment.columnFor(sourceColumn)
                .orElseThrow(() -> MaterializationException.incrementalUnsupported(
                        "partition column " + sourceColumn + " has no output column"));
        return RefreshSql.quote(mapped.outputColumn());
    }
}
