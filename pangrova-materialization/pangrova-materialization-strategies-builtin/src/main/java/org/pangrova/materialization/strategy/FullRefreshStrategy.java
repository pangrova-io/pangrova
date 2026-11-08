package org.pangrova.materialization.strategy;

import java.util.Map;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.domain.StrategyId;
import org.pangrova.materialization.extension.RefreshContext;
import org.pangrova.materialization.domain.RefreshPlan;
import org.pangrova.materialization.extension.RefreshStrategy;
import org.pangrova.materialization.extension.StrategyCompatibility;
import org.pangrova.connector.api.materialization.ChangeProof;

/**
 * Recomputes the whole result from the exact planned source snapshots.
 *
 * <p>Full refresh has no change set and never names a base generation, so it is
 * always applicable to a validated definition.
 */
public final class FullRefreshStrategy implements RefreshStrategy {

    @Override
    public StrategyId id() {
        return StrategyId.FULL;
    }

    @Override
    public RefreshKind kind() {
        return RefreshKind.Full.INSTANCE;
    }

    @Override
    public StrategyCompatibility validate(RefreshContext context) {
        if (context.baseGeneration().isPresent()) {
            return StrategyCompatibility.incompatible(
                    "full refresh must not name a base generation");
        }
        if (!context.changeSets().isEmpty()) {
            return StrategyCompatibility.incompatible("full refresh must not carry change sets");
        }
        return StrategyCompatibility.supported();
    }

    @Override
    public RefreshPlan plan(RefreshContext context) {
        StrategyCompatibility compatibility = validate(context);
        if (!compatibility.compatible()) {
            throw MaterializationException.incrementalUnsupported(compatibility.reason());
        }
        String canonicalSql = RefreshSql.definitionQuery(context.definition().sql());
        String fingerprint = RefreshSql.sha256("full:v1|"
                + context.definition().fingerprint() + '|'
                + context.sourceVector().canonicalHash() + '|'
                + context.candidateTable().qualifiedName());
        return new RefreshPlan(kind(), canonicalSql, Map.of(), fingerprint);
    }

    @Override
    public void verify(RefreshPlan plan, ChangeProof proof) {
        throw MaterializationException.requestRejected(
                "a full refresh plan has no change proof to verify");
    }
}
