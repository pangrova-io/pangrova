package org.pangrova.materialization.usecase;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationObject;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.domain.RefreshMode;
import org.pangrova.materialization.domain.StrategyId;
import org.pangrova.materialization.extension.MaterializationExtensionRegistry;
import org.pangrova.materialization.port.PlannedDefinition;

/**
 * Resolves the kind, base, and strategy of the next refresh.
 *
 * <p>INCREMENTAL never degrades to FULL: a configured strategy must be
 * supported by the planned definition, and AUTO only selects among registered
 * incremental strategies.
 */
final class RefreshResolutionPolicy {

    private static final List<RefreshKind> INCREMENTAL_KINDS = List.of(
            // Partition replacement covers every interval an append-only merge
            // can prove for a partitioned source, including deletes and file
            // rewrites. Append-only stays the fallback for sources that cannot
            // identify partitions; a workload that wants the cheaper append
            // path pins INCREMENTAL_STRATEGY = APPEND_ONLY.
            RefreshKind.IncrementalPartitionReplace.INSTANCE,
            RefreshKind.IncrementalAppendOnly.INSTANCE,
            RefreshKind.IncrementalAffectedKey.INSTANCE,
            RefreshKind.IncrementalCdc.INSTANCE);

    private final MaterializationExtensionRegistry extensions;

    RefreshResolutionPolicy(MaterializationExtensionRegistry extensions) {
        if (extensions == null) {
            throw new IllegalArgumentException("extensions must not be null");
        }
        this.extensions = extensions;
    }

    RefreshResolution resolve(MaterializationObject header, PlannedDefinition planned,
            Generation published) {
        if (published.definitionRevision() != header.definitionRevision()) {
            // A redefinition changed the output of the source query, so the
            // existing generation is not a valid incremental base. Bootstrap
            // the new definition with one full generation instead of failing
            // every incremental refresh until a manual full refresh.
            return full();
        }
        if (header.refreshMode() == RefreshMode.FULL) {
            return full();
        }
        RefreshKind kind = header.incrementalStrategy()
                .map(configured -> configuredKind(header, planned, configured))
                .orElseGet(() -> autoKind(header, planned));
        if (!header.refreshMode().accepts(kind)) {
            throw MaterializationException.incrementalUnsupported(
                    "refresh mode " + header.refreshMode() + " does not accept " + kind);
        }
        return new RefreshResolution(kind, Optional.of(published),
                extensions.strategy(kind.strategyId()));
    }

    RefreshResolution full() {
        return new RefreshResolution(RefreshKind.Full.INSTANCE, Optional.empty(),
                extensions.strategy(StrategyId.FULL));
    }

    /**
     * Validates the configured refresh mode against the definition at creation.
     *
     * <p>Creation always bootstraps a full generation, but an object configured
     * for incremental refresh must be able to resolve an incremental strategy
     * now; otherwise the configuration could never take effect.
     */
    void requireConfiguration(RefreshMode mode, Optional<StrategyId> strategy,
            Set<RefreshKind> supportedRefreshKinds) {
        if (mode == RefreshMode.FULL) {
            requireNoStrategy(strategy);
            return;
        }
        if (strategy.isPresent()) {
            requireSupported(strategy.get(), supportedRefreshKinds);
            return;
        }
        boolean available = INCREMENTAL_KINDS.stream()
                .anyMatch(kind -> supportedRefreshKinds.contains(kind)
                        && extensions.hasStrategy(kind.strategyId()));
        if (!available) {
            throw MaterializationException.incrementalUnsupported(
                    "definition has no compatible incremental strategy; "
                            + "create it with full refresh instead");
        }
    }

    private static void requireNoStrategy(Optional<StrategyId> strategy) {
        if (strategy.isPresent()) {
            throw MaterializationException.requestRejected(
                    "FULL refresh mode cannot carry an incremental strategy");
        }
    }

    private void requireSupported(StrategyId configured,
            Set<RefreshKind> supportedRefreshKinds) {
        RefreshKind kind = incrementalKind(configured);
        if (!supportedRefreshKinds.contains(kind)) {
            throw MaterializationException.incrementalUnsupported(
                    "definition does not support configured strategy " + configured.value());
        }
        extensions.strategy(configured);
    }

    private RefreshKind configuredKind(MaterializationObject header, PlannedDefinition planned,
            StrategyId configured) {
        RefreshKind kind = incrementalKind(configured);
        if (!planned.supportedRefreshKinds().contains(kind)) {
            throw MaterializationException.incrementalUnsupported(
                    "definition of " + header.id().canonicalName()
                            + " does not support configured strategy " + configured.value());
        }
        return kind;
    }

    private RefreshKind autoKind(MaterializationObject header, PlannedDefinition planned) {
        return INCREMENTAL_KINDS.stream()
                .filter(candidate -> planned.supportedRefreshKinds().contains(candidate)
                        && extensions.hasStrategy(candidate.strategyId()))
                .findFirst()
                .orElseThrow(() -> MaterializationException.incrementalUnsupported(
                        "no registered incremental strategy applies to "
                                + header.id().canonicalName()));
    }

    private static RefreshKind incrementalKind(StrategyId strategy) {
        if (StrategyId.FULL.equals(strategy)) {
            throw MaterializationException.requestRejected(
                    "FULL is not an incremental strategy");
        }
        return INCREMENTAL_KINDS.stream()
                .filter(kind -> kind.strategyId().equals(strategy))
                .findFirst()
                .orElseThrow(() -> MaterializationException.incrementalUnsupported(
                        "unknown incremental strategy " + strategy.value()));
    }
}
