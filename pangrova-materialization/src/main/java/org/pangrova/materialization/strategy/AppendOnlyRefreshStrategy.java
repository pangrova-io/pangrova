package org.pangrova.materialization.strategy;

import java.util.LinkedHashMap;
import java.util.Map;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.GenerationState;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.domain.StrategyId;
import org.pangrova.materialization.extension.RefreshContext;
import org.pangrova.materialization.domain.RefreshPlan;
import org.pangrova.materialization.extension.RefreshStrategy;
import org.pangrova.materialization.extension.StrategyCompatibility;
import org.pangrova.spi.materialization.ChangeProof;
import org.pangrova.spi.materialization.ChangeSetMode;
import org.pangrova.spi.materialization.ChangeSetPlan;
import org.pangrova.spi.materialization.SourceRef;
import org.pangrova.spi.materialization.SourceSnapshotRef;

/**
 * Appends the rows of a proven append-only snapshot delta to the base result.
 *
 * <p>The canonical statement is the base generation's physical table followed
 * by the definition evaluated over the delta range. It is only valid when the
 * definition is a single-table projection/filter scan, the base generation was
 * produced by the same definition revision, and the connector proved that no
 * existing row changed or disappeared in the interval.
 */
public final class AppendOnlyRefreshStrategy implements RefreshStrategy {

    @Override
    public StrategyId id() {
        return StrategyId.APPEND_ONLY;
    }

    @Override
    public RefreshKind kind() {
        return RefreshKind.IncrementalAppendOnly.INSTANCE;
    }

    @Override
    public StrategyCompatibility validate(RefreshContext context) {
        if (context.baseGeneration().isEmpty()) {
            return StrategyCompatibility.incompatible(
                    "append-only refresh requires a published base generation");
        }
        Generation base = context.baseGeneration().get();
        if (base.state() != GenerationState.Published.INSTANCE) {
            return StrategyCompatibility.incompatible(
                    "base generation " + base.id() + " is " + base.state() + ", not published");
        }
        if (base.definitionRevision() != context.definition().revision()) {
            return StrategyCompatibility.incompatible(
                    "base generation was computed from definition revision "
                            + base.definitionRevision() + ", not " + context.definition().revision());
        }
        if (!base.sourceVector().snapshots().keySet()
                .equals(context.sourceVector().snapshots().keySet())) {
            return StrategyCompatibility.incompatible(
                    "base generation and planned sources differ");
        }
        if (context.sourceVector().snapshots().size() != 1) {
            return StrategyCompatibility.incompatible(
                    "append-only refresh requires exactly one source table");
        }
        if (context.changeProof().isEmpty()) {
            return StrategyCompatibility.incompatible(
                    "append-only refresh requires a connector change proof");
        }
        ChangeProof proof = context.changeProof().get();
        if (!proof.complete()) {
            return StrategyCompatibility.incompatible(
                    "change proof for " + proof.source().canonicalName() + " is incomplete");
        }
        if (proof.mode() != ChangeSetMode.APPEND_ONLY) {
            return StrategyCompatibility.incompatible(
                    "change proof mode " + proof.mode() + " is not APPEND_ONLY");
        }
        SourceRef source = context.sourceVector().snapshots().keySet().iterator().next();
        if (!proof.source().equals(source)) {
            return StrategyCompatibility.incompatible(
                    "change proof belongs to " + proof.source().canonicalName());
        }
        SourceSnapshotRef target = context.sourceVector().snapshotOf(source).orElseThrow();
        if (!proof.toToken().equals(target.snapshotToken())) {
            return StrategyCompatibility.incompatible(
                    "change proof reaches snapshot " + proof.toToken()
                            + " instead of the planned " + target.snapshotToken());
        }
        if (proof.fromToken().equals(proof.toToken())) {
            return StrategyCompatibility.incompatible(
                    "append-only refresh requires a non-empty snapshot interval");
        }
        ChangeSetPlan changeSet = context.changeSets().get(source);
        if (changeSet == null
                || changeSet.mode() != ChangeSetMode.APPEND_ONLY
                || !changeSet.fromSnapshot().snapshotToken().equals(proof.fromToken())
                || !changeSet.toSnapshot().snapshotToken().equals(proof.toToken())) {
            return StrategyCompatibility.incompatible(
                    "change proof does not match the planned change set");
        }
        return StrategyCompatibility.supported();
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
        String canonicalSql = "SELECT * FROM " + RefreshSql.qualified(base.physicalTable())
                + " UNION ALL " + RefreshSql.definitionQuery(context.definition().sql());
        Map<SourceRef, ChangeSetPlan> changeSets = new LinkedHashMap<>();
        changeSets.put(source, changeSet);
        String fingerprint = RefreshSql.sha256("append-only:v1|"
                + context.definition().fingerprint() + '|'
                + base.physicalTable().qualifiedName() + '|'
                + base.sourceVector().canonicalHash() + '|'
                + context.sourceVector().canonicalHash() + '|'
                + changeSet.planFingerprint() + '|'
                + context.candidateTable().qualifiedName());
        return new RefreshPlan(kind(), canonicalSql, changeSets, fingerprint);
    }

    @Override
    public void verify(RefreshPlan plan, ChangeProof proof) {
        if (!plan.kind().equals(kind())) {
            throw MaterializationException.requestRejected(
                    "append-only strategy received a " + plan.kind() + " plan");
        }
        if (!proof.complete()) {
            throw MaterializationException.incrementalUnsupported(
                    "incomplete change proof for " + proof.source().canonicalName()
                            + " from " + proof.fromToken() + " to " + proof.toToken());
        }
        if (proof.mode() != ChangeSetMode.APPEND_ONLY) {
            throw MaterializationException.incrementalUnsupported(
                    "change proof mode " + proof.mode() + " is not APPEND_ONLY");
        }
        ChangeSetPlan changeSet = plan.changeSets().get(proof.source());
        if (changeSet == null
                || changeSet.mode() != ChangeSetMode.APPEND_ONLY
                || !changeSet.fromSnapshot().snapshotToken().equals(proof.fromToken())
                || !changeSet.toSnapshot().snapshotToken().equals(proof.toToken())) {
            throw MaterializationException.requestRejected(
                    "change proof does not cover the planned change set for "
                            + proof.source().canonicalName());
        }
    }
}
