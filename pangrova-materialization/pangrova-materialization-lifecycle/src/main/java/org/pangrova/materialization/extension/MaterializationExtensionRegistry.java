package org.pangrova.materialization.extension;

import org.pangrova.materialization.api.definition.MaterializationKindHandler;
import org.pangrova.materialization.api.policy.PolicyEvaluator;
import org.pangrova.materialization.api.enums.RefreshKind;
import org.pangrova.materialization.api.refresh.RefreshStrategy;
import org.pangrova.materialization.api.refresh.RefreshStrategyFactory;
import org.pangrova.materialization.api.rewrite.RewriteGuard;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import org.pangrova.materialization.api.exception.MaterializationException;
import org.pangrova.materialization.api.enums.MaterializationKind;
import org.pangrova.materialization.api.identity.StrategyId;
import org.pangrova.connector.api.materialization.SourceRef;

/**
 * Explicit registry of materialization extensions.
 *
 * <p>The Server supplies the class loader for refresh strategy factories. Duplicate identifiers,
         inconsistent strategy kinds, or
 * missing required handlers fail at startup rather than at query time. Adding a
 * strategy, connector policy evaluator, object kind, or rewrite guard requires
 * no change to the kernel.
 */
public final class MaterializationExtensionRegistry {

    private final Map<MaterializationKind, MaterializationKindHandler> kindHandlers;
    private final Map<StrategyId, RefreshStrategy> strategies;
    private final List<RewriteGuard> rewriteGuards;
    private final List<PolicyEvaluator> policyEvaluators;

    /** Discovers and validates the Coordinator's refresh strategies before its SQL handlers start. */
    public static MaterializationExtensionRegistry discover(ClassLoader loader) {
        return fromFactories(ServiceLoader.load(RefreshStrategyFactory.class, loader));
    }

    static MaterializationExtensionRegistry fromFactories(Iterable<RefreshStrategyFactory> factories) {
        Map<RefreshKind, RefreshStrategyFactory> byKind = new LinkedHashMap<>();
        for (RefreshStrategyFactory factory : factories) {
            if (factory.apiVersion() != RefreshStrategyFactory.API_VERSION) {
                throw new IllegalStateException("unsupported refresh strategy factory version for "
                        + factory.getClass().getName() + ": " + factory.apiVersion());
            }
            RefreshKind kind = factory.kind();
            if (kind == null) {
                throw new IllegalStateException("refresh strategy factory must declare its kind");
            }
            if (byKind.putIfAbsent(kind, factory) != null) {
                throw new IllegalStateException("duplicate refresh strategy " + kind.strategyId().value());
            }
        }
        if (!byKind.containsKey(RefreshKind.FULL)) {
            throw new IllegalStateException("missing FULL refresh strategy");
        }
        List<RefreshStrategy> strategies = new ArrayList<>(byKind.size());
        byKind.forEach((kind, factory) -> {
            RefreshStrategy strategy = factory.create();
            if (strategy == null || !kind.equals(strategy.kind()) || !kind.strategyId().equals(strategy.id())) {
                throw new IllegalStateException("refresh strategy factory disagrees with its product for "
                        + kind.strategyId().value());
            }
            strategies.add(strategy);
        });
        return new MaterializationExtensionRegistry(List.of(), strategies, List.of(), List.of());
    }

    /**
     * Creates a registry after validating every extension.
     *
     * @param kindHandlers object-kind handlers
     * @param strategies refresh strategies
     * @param rewriteGuards ordered rewrite guards
     * @param policyEvaluators policy evaluators
     */
    public MaterializationExtensionRegistry(
            Collection<MaterializationKindHandler> kindHandlers,
            Collection<RefreshStrategy> strategies,
            Collection<RewriteGuard> rewriteGuards,
            Collection<PolicyEvaluator> policyEvaluators) {
        this.kindHandlers = indexKinds(kindHandlers);
        this.strategies = indexStrategies(strategies);
        this.rewriteGuards = List.copyOf(rewriteGuards);
        this.policyEvaluators = List.copyOf(policyEvaluators);
        requireUniqueGuards(this.rewriteGuards);
        requireUniqueEvaluators(this.policyEvaluators);
    }

    /**
     * Returns the handler for a kind.
     *
     * @param kind object kind
     * @return registered handler
     * @throws MaterializationException when no handler is registered
     */
    public MaterializationKindHandler kindHandler(MaterializationKind kind) {
        MaterializationKindHandler handler = kindHandlers.get(kind);
        if (handler == null) {
            throw MaterializationException.capabilityUnavailable(
                    "no materialization kind handler registered for " + kind);
        }
        return handler;
    }

    /**
     * Returns a refresh strategy by identifier.
     *
     * @param strategyId strategy identifier
     * @return registered strategy
     * @throws MaterializationException when no strategy is registered
     */
    public RefreshStrategy strategy(StrategyId strategyId) {
        RefreshStrategy strategy = strategies.get(strategyId);
        if (strategy == null) {
            throw MaterializationException.capabilityUnavailable(
                    "no refresh strategy registered for " + strategyId.value());
        }
        return strategy;
    }

    /**
     * Returns whether a strategy identifier is registered.
     *
     * @param strategyId strategy identifier
     * @return true when registered
     */
    public boolean hasStrategy(StrategyId strategyId) {
        return strategies.containsKey(strategyId);
    }

    /**
     * Returns the ordered rewrite guard chain.
     *
     * @return immutable guard list
     */
    public List<RewriteGuard> rewriteGuards() {
        return rewriteGuards;
    }

    /**
     * Returns policy evaluators that support a source.
     *
     * @param source source table
     * @return matching evaluators in registration order
     */
    public List<PolicyEvaluator> policyEvaluatorsFor(SourceRef source) {
        return policyEvaluators.stream().filter(evaluator -> evaluator.supports(source)).toList();
    }

    /**
     * Returns all registered object kinds.
     *
     * @return immutable kind set
     */
    public Set<MaterializationKind> registeredKinds() {
        return Set.copyOf(kindHandlers.keySet());
    }

    /**
     * Fails fast when a required handler or baseline strategy is missing.
     *
     * @param requiredKinds kinds that must be registered by this deployment
     */
    public void requireComplete(Set<MaterializationKind> requiredKinds) {
        if (requiredKinds == null) {
            throw new IllegalArgumentException("requiredKinds must not be null");
        }
        for (MaterializationKind kind : requiredKinds) {
            if (!kindHandlers.containsKey(kind)) {
                throw new IllegalStateException("missing materialization kind handler for " + kind);
            }
        }
        if (!strategies.containsKey(StrategyId.FULL)) {
            throw new IllegalStateException("missing FULL refresh strategy");
        }
        boolean rewriteKindRegistered = kindHandlers.values().stream()
                .anyMatch(MaterializationKindHandler::supportsTransparentRewrite);
        if (rewriteKindRegistered && rewriteGuards.isEmpty()) {
            throw new IllegalStateException("transparent rewrite is enabled without any rewrite guard");
        }
    }

    private static Map<MaterializationKind, MaterializationKindHandler> indexKinds(
            Collection<MaterializationKindHandler> handlers) {
        Map<MaterializationKind, MaterializationKindHandler> indexed = new LinkedHashMap<>();
        for (MaterializationKindHandler handler : handlers) {
            MaterializationKindHandler previous = indexed.putIfAbsent(handler.kind(), handler);
            if (previous != null) {
                throw new IllegalStateException(
                        "duplicate materialization kind handler for " + handler.kind());
            }
        }
        return Map.copyOf(indexed);
    }

    private static Map<StrategyId, RefreshStrategy> indexStrategies(
            Collection<RefreshStrategy> candidates) {
        Map<StrategyId, RefreshStrategy> indexed = new LinkedHashMap<>();
        for (RefreshStrategy strategy : candidates) {
            if (!strategy.kind().strategyId().equals(strategy.id())) {
                throw new IllegalStateException(
                        "strategy " + strategy.id().value() + " declares kind "
                                + strategy.kind().strategyId().value());
            }
            RefreshStrategy previous = indexed.putIfAbsent(strategy.id(), strategy);
            if (previous != null) {
                throw new IllegalStateException(
                        "duplicate refresh strategy " + strategy.id().value());
            }
        }
        return Map.copyOf(indexed);
    }

    private static void requireUniqueGuards(List<RewriteGuard> guards) {
        Set<String> ids = new java.util.HashSet<>();
        for (RewriteGuard guard : guards) {
            if (guard.guardId() == null || guard.guardId().isBlank()) {
                throw new IllegalStateException("rewrite guard id must not be blank");
            }
            if (!ids.add(guard.guardId())) {
                throw new IllegalStateException("duplicate rewrite guard " + guard.guardId());
            }
        }
    }

    private static void requireUniqueEvaluators(List<PolicyEvaluator> evaluators) {
        Set<String> ids = new java.util.HashSet<>();
        for (PolicyEvaluator evaluator : evaluators) {
            if (evaluator.evaluatorId() == null || evaluator.evaluatorId().isBlank()) {
                throw new IllegalStateException("policy evaluator id must not be blank");
            }
            if (!ids.add(evaluator.evaluatorId())) {
                throw new IllegalStateException("duplicate policy evaluator " + evaluator.evaluatorId());
            }
        }
    }
}
