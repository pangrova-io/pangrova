package org.pangrova.materialization.extension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.pangrova.materialization.MaterializationTestFixtures.DEFINER;
import static org.pangrova.materialization.MaterializationTestFixtures.NOW;
import static org.pangrova.materialization.MaterializationTestFixtures.policies;
import static org.pangrova.materialization.MaterializationTestFixtures.source;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.materialization.domain.DefinitionRevision;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationKind;
import org.pangrova.materialization.domain.PolicySnapshot;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.domain.RefreshMode;
import org.pangrova.materialization.domain.SourceVector;
import org.pangrova.materialization.domain.StrategyId;
import org.pangrova.materialization.domain.RefreshPlan;
import org.pangrova.connector.spi.materialization.ChangeProof;
import org.pangrova.connector.spi.materialization.SourceRef;

class MaterializationExtensionRegistryTest {

    @Test
    void resolvesRegisteredExtensionsAndPreservesGuardOrder() {
        RewriteGuard policyGuard = guard("policy", true);
        RewriteGuard freshnessGuard = guard("freshness", true);
        PolicyEvaluator ordersEvaluator = evaluator("orders", "orders");
        PolicyEvaluator customersEvaluator = evaluator("customers", "customers");
        MaterializationExtensionRegistry registry = new MaterializationExtensionRegistry(
                List.of(kindHandler(MaterializationKind.DYNAMIC_TABLE, false),
                        kindHandler(MaterializationKind.MATERIALIZED_VIEW, true)),
                List.of(strategy(StrategyId.FULL, RefreshKind.Full.INSTANCE),
                        strategy(StrategyId.APPEND_ONLY, RefreshKind.IncrementalAppendOnly.INSTANCE)),
                List.of(policyGuard, freshnessGuard),
                List.of(ordersEvaluator, customersEvaluator));

        assertThat(registry.kindHandler(MaterializationKind.MATERIALIZED_VIEW).supportsTransparentRewrite())
                .isTrue();
        assertThat(registry.strategy(StrategyId.APPEND_ONLY).kind())
                .isEqualTo(RefreshKind.IncrementalAppendOnly.INSTANCE);
        assertThat(registry.hasStrategy(StrategyId.CDC)).isFalse();
        assertThat(registry.rewriteGuards()).containsExactly(policyGuard, freshnessGuard);
        assertThat(registry.policyEvaluatorsFor(source("orders"))).containsExactly(ordersEvaluator);
        assertThat(registry.registeredKinds())
                .containsExactlyInAnyOrder(
                        MaterializationKind.DYNAMIC_TABLE, MaterializationKind.MATERIALIZED_VIEW);

        registry.requireComplete(Set.of(MaterializationKind.DYNAMIC_TABLE, MaterializationKind.MATERIALIZED_VIEW));
    }

    @Test
    void rejectsDuplicateKindHandlers() {
        assertThatIllegalStateException()
                .isThrownBy(() -> new MaterializationExtensionRegistry(
                        List.of(kindHandler(MaterializationKind.DYNAMIC_TABLE, false),
                                kindHandler(MaterializationKind.DYNAMIC_TABLE, false)),
                        List.of(),
                        List.of(),
                        List.of()))
                .withMessageContaining("duplicate materialization kind handler");
    }

    @Test
    void rejectsDuplicateOrMismatchedStrategies() {
        assertThatIllegalStateException()
                .isThrownBy(() -> new MaterializationExtensionRegistry(
                        List.of(),
                        List.of(strategy(StrategyId.FULL, RefreshKind.Full.INSTANCE),
                                strategy(StrategyId.FULL, RefreshKind.Full.INSTANCE)),
                        List.of(),
                        List.of()))
                .withMessageContaining("duplicate refresh strategy");

        assertThatIllegalStateException()
                .isThrownBy(() -> new MaterializationExtensionRegistry(
                        List.of(),
                        List.of(new StubStrategy(StrategyId.CDC, RefreshKind.IncrementalAppendOnly.INSTANCE)),
                        List.of(),
                        List.of()))
                .withMessageContaining("declares kind");
    }

    @Test
    void rejectsMissingRequiredHandlersAndBaselineStrategy() {
        MaterializationExtensionRegistry onlyFull = new MaterializationExtensionRegistry(
                List.of(kindHandler(MaterializationKind.DYNAMIC_TABLE, false)),
                List.of(strategy(StrategyId.FULL, RefreshKind.Full.INSTANCE)),
                List.of(),
                List.of());
        MaterializationExtensionRegistry onlyAppendOnly = new MaterializationExtensionRegistry(
                List.of(kindHandler(MaterializationKind.DYNAMIC_TABLE, false)),
                List.of(strategy(StrategyId.APPEND_ONLY, RefreshKind.IncrementalAppendOnly.INSTANCE)),
                List.of(),
                List.of());

        assertThatIllegalStateException()
                .isThrownBy(() -> onlyFull.requireComplete(Set.of(MaterializationKind.MATERIALIZED_VIEW)))
                .withMessageContaining("missing materialization kind handler");
        assertThatIllegalStateException()
                .isThrownBy(() -> onlyAppendOnly.requireComplete(Set.of(MaterializationKind.DYNAMIC_TABLE)))
                .withMessageContaining("missing FULL refresh strategy");
    }

    @Test
    void rewriteParticipationRequiresAtLeastOneGuard() {
        MaterializationExtensionRegistry registry = new MaterializationExtensionRegistry(
                List.of(kindHandler(MaterializationKind.MATERIALIZED_VIEW, true)),
                List.of(strategy(StrategyId.FULL, RefreshKind.Full.INSTANCE)),
                List.of(),
                List.of());

        assertThatIllegalStateException()
                .isThrownBy(() -> registry.requireComplete(Set.of(MaterializationKind.MATERIALIZED_VIEW)))
                .withMessageContaining("transparent rewrite is enabled without any rewrite guard");
    }

    @Test
    void duplicateOrBlankGuardIdsAreRejected() {
        assertThatIllegalStateException()
                .isThrownBy(() -> new MaterializationExtensionRegistry(
                        List.of(), List.of(),
                        List.of(guard("policy", true), guard("policy", false)),
                        List.of()))
                .withMessageContaining("duplicate rewrite guard");
        assertThatIllegalStateException()
                .isThrownBy(() -> new MaterializationExtensionRegistry(
                        List.of(), List.of(), List.of(guard(" ", true)), List.of()))
                .withMessageContaining("rewrite guard id must not be blank");
    }

    @Test
    void duplicateOrBlankEvaluatorIdsAreRejected() {
        assertThatIllegalStateException()
                .isThrownBy(() -> new MaterializationExtensionRegistry(
                        List.of(), List.of(), List.of(),
                        List.of(evaluator("policy", "orders"), evaluator("policy", "customers"))))
                .withMessageContaining("duplicate policy evaluator");
        assertThatIllegalStateException()
                .isThrownBy(() -> new MaterializationExtensionRegistry(
                        List.of(), List.of(), List.of(), List.of(evaluator("", "orders"))))
                .withMessageContaining("policy evaluator id must not be blank");
    }

    @Test
    void unknownExtensionsFailWithStructuredCapabilityErrors() {
        MaterializationExtensionRegistry registry = new MaterializationExtensionRegistry(
                List.of(), List.of(), List.of(), List.of());

        MaterializationException missingKind = catchThrowableOfType(
                () -> registry.kindHandler(MaterializationKind.DYNAMIC_TABLE), MaterializationException.class);
        MaterializationException missingStrategy = catchThrowableOfType(
                () -> registry.strategy(StrategyId.CDC), MaterializationException.class);

        assertThat(missingKind.getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_CAPABILITY_UNAVAILABLE);
        assertThat(missingStrategy.getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_CAPABILITY_UNAVAILABLE);
    }

    @Test
    void rewriteGuardsAreEvaluatedAsAPureChain() {
        RewriteContext allowAll = new RewriteContext(
                org.pangrova.materialization.MaterializationTestFixtures.objectId("mv"),
                DEFINER,
                Set.of(source("orders")),
                false,
                false,
                false,
                true,
                NOW,
                Optional.empty(),
                NOW);

        assertThat(guard("policy", true).evaluate(allowAll).allowed()).isTrue();
        assertThat(guard("policy", false).evaluate(allowAll).allowed()).isFalse();
        assertThat(guard("policy", false).evaluate(allowAll).reason()).contains("policy");
    }

    private static MaterializationKindHandler kindHandler(MaterializationKind kind, boolean rewrite) {
        return new MaterializationKindHandler() {
            @Override
            public MaterializationKind kind() {
                return kind;
            }

            @Override
            public RefreshMode defaultRefreshMode() {
                return RefreshMode.FULL;
            }

            @Override
            public void validateDefinition(DefinitionRevision definition) {
            }

            @Override
            public boolean supportsTransparentRewrite() {
                return rewrite;
            }
        };
    }

    private static RefreshStrategy strategy(StrategyId id, RefreshKind kind) {
        return new StubStrategy(id, kind);
    }

    private static RewriteGuard guard(String id, boolean allowed) {
        return new RewriteGuard() {
            @Override
            public String guardId() {
                return id;
            }

            @Override
            public RewriteDecision evaluate(RewriteContext context) {
                return allowed ? RewriteDecision.allow() : RewriteDecision.skip(id + " rejected");
            }
        };
    }

    private static PolicyEvaluator evaluator(String id, String supportedTable) {
        return new PolicyEvaluator() {
            @Override
            public String evaluatorId() {
                return id;
            }

            @Override
            public boolean supports(SourceRef source) {
                return source.table().equals(supportedTable);
            }

            @Override
            public PolicySnapshot capture(Principal definer, Set<SourceRef> sources) {
                return policies(sources);
            }

            @Override
            public void requireUnchanged(PolicySnapshot planned, SourceVector observed) {
            }
        };
    }

    private static final class StubStrategy implements RefreshStrategy {

        private final StrategyId id;
        private final RefreshKind kind;

        private StubStrategy(StrategyId id, RefreshKind kind) {
            this.id = id;
            this.kind = kind;
        }

        @Override
        public StrategyId id() {
            return id;
        }

        @Override
        public RefreshKind kind() {
            return kind;
        }

        @Override
        public StrategyCompatibility validate(RefreshContext context) {
            return StrategyCompatibility.supported();
        }

        @Override
        public RefreshPlan plan(RefreshContext context) {
            return new RefreshPlan(
                    kind,
                    context.definition().sql(),
                    Map.of(),
                    "plan");
        }

        @Override
        public void verify(RefreshPlan plan, ChangeProof proof) {
        }
    }
}
