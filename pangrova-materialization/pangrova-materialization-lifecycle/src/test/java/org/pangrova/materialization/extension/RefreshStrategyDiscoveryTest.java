package org.pangrova.materialization.extension;

import org.junit.jupiter.api.Test;
import org.pangrova.materialization.api.domain.RefreshKind;
import org.pangrova.materialization.api.domain.StrategyId;
import org.pangrova.materialization.api.refresh.RefreshStrategy;
import org.pangrova.materialization.api.refresh.RefreshStrategyFactory;
import org.pangrova.materialization.strategy.AppendOnlyRefreshStrategy;
import org.pangrova.materialization.strategy.FullRefreshStrategy;

import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class RefreshStrategyDiscoveryTest {
    @Test
    void loadsPackagedStrategiesAndSharesOneInstancePerRegistry() {
        var registry = MaterializationExtensionRegistry.discover(getClass().getClassLoader());
        for (RefreshKind kind : List.of(RefreshKind.Full.INSTANCE, RefreshKind.IncrementalAppendOnly.INSTANCE,
                RefreshKind.IncrementalPartitionReplace.INSTANCE)) {
            RefreshStrategy strategy = registry.strategy(kind.strategyId());
            assertThat(strategy.kind()).isEqualTo(kind);
            assertThat(registry.strategy(kind.strategyId())).isSameAs(strategy);
        }
        assertThat(registry.hasStrategy(StrategyId.AFFECTED_KEY)).isFalse();
        assertThat(registry.hasStrategy(StrategyId.CDC)).isFalse();
    }

    @Test
    void rejectsInvalidSetsBeforeConstructingAnyStrategies() {
        Supplier<RefreshStrategy> never = () -> { throw new AssertionError("premature creation"); };
        var full = new Factory(RefreshStrategyFactory.API_VERSION, RefreshKind.Full.INSTANCE, never);
        var future = new Factory(RefreshStrategyFactory.API_VERSION + 1, RefreshKind.Full.INSTANCE, never);
        assertThatIllegalStateException()
                .isThrownBy(() -> MaterializationExtensionRegistry.fromFactories(List.of()))
                .withMessageContaining("missing FULL");
        assertThatIllegalStateException()
                .isThrownBy(() -> MaterializationExtensionRegistry.fromFactories(List.of(full, full)))
                .withMessageContaining("duplicate refresh strategy FULL");
        assertThatIllegalStateException()
                .isThrownBy(() -> MaterializationExtensionRegistry.fromFactories(List.of(future)))
                .withMessageContaining("version");
    }

    @Test
    void rejectsNullAndMismatchedFactoryProductsAndAcceptsFullOnlyDeployment() {
        var kind = RefreshKind.Full.INSTANCE;
        int version = RefreshStrategyFactory.API_VERSION;
        var wrong = new Factory(version, kind, AppendOnlyRefreshStrategy::new);
        var empty = new Factory(version, kind, () -> null);
        for (var invalid : List.of(wrong, empty)) {
            assertThatIllegalStateException()
                    .isThrownBy(() -> MaterializationExtensionRegistry.fromFactories(List.of(invalid)))
                    .withMessageContaining("product for FULL");
        }
        var registry = MaterializationExtensionRegistry.fromFactories(
                List.of(new Factory(version, kind, FullRefreshStrategy::new)));
        assertThat(registry.hasStrategy(StrategyId.FULL)).isTrue();
        assertThat(registry.hasStrategy(StrategyId.APPEND_ONLY)).isFalse();
    }

    private record Factory(int apiVersion, RefreshKind kind, Supplier<RefreshStrategy> supplier)
            implements RefreshStrategyFactory {
        @Override
        public RefreshStrategy create() {
            return supplier.get();
        }
    }
}
