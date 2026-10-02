package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.TreeMap;

import org.pangrova.shuffle.ShuffleCapabilities;
import org.pangrova.shuffle.ShuffleContext;
import org.pangrova.shuffle.ShuffleDurability;
import org.pangrova.shuffle.ShuffleManagerFactory;
import org.pangrova.shuffle.ShuffleProvider;
import org.junit.jupiter.api.Test;

/**
 * ADR-002 D1 acceptance: ServiceLoader discovery + selection by name, with fast-fail on an unknown
 * provider name (no silent fallback to {@code native}).
 *
 * <p>Also covers G-HA-07: auto-selection of the highest-durability provider when no explicit
 * shuffle.manager is configured.</p>
 */
class ShuffleProviderResolverTest {

    @Test
    void discoversInTreeFactoriesByName() {
        Map<String, ShuffleManagerFactory> byName = ShuffleProviderResolver.discoverFactories();
        assertThat(byName).containsKey(NativeShuffleManagerFactory.NAME);
        assertThat(byName.get(NativeShuffleManagerFactory.NAME))
                .isInstanceOf(NativeShuffleManagerFactory.class);
    }

    @Test
    void nativeProviderIsEphemeralAndNotTaskRetryable() {
        ShuffleProvider provider =
                ShuffleProviderResolver.resolve(NativeShuffleManagerFactory.NAME, Map.of());
        assertThat(provider.capabilities().durability()).isEqualTo(ShuffleDurability.EPHEMERAL);
        assertThat(provider.capabilities().supportsTaskRetry()).isFalse();
        assertThat(provider.lifecycleManager()).isNull();
    }

    @Test
    void unknownNameFailsFastWithAvailableProviders() {
        assertThatThrownBy(() -> ShuffleProviderResolver.resolve("does-not-exist", Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does-not-exist")
                .hasMessageContaining(NativeShuffleManagerFactory.NAME);
    }

    @Test
    void blankNameRejected() {
        assertThatThrownBy(() -> ShuffleProviderResolver.resolve("  ", Map.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ShuffleProviderResolver.resolve(null, Map.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ── G-HA-07: auto-selection tests ──

    @Test
    void resolveWithDurableDefaultDoesNotAdvertiseLocalDiskAsDurable() {
        // The object-store provider has no configured durable backend. It must
        // fail its probe instead of claiming that local disk survives node loss.
        ShuffleProvider provider = ShuffleProviderResolver.resolveWithDurableDefault(null, Map.of());
        assertThat(provider).isNotNull();
        assertThat(provider.capabilities().durability()).isEqualTo(ShuffleDurability.LOCAL_SPILL);
    }

    @Test
    void objectStoreProviderRequiresRealDurableBackend() {
        assertThatThrownBy(() -> ShuffleProviderResolver.resolve(
                ObjectStoreShuffleManagerFactory.NAME, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shuffle.object-store.backend");
        assertThatThrownBy(() -> ShuffleProviderResolver.resolve(
                ObjectStoreShuffleManagerFactory.NAME,
                Map.of("shuffle.object-store.backend", "local-fs")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not guarantee DURABLE");
    }

    @Test
    void resolveWithDurableDefaultUsesExplicitNameWhenProvided() {
        ShuffleProvider provider = ShuffleProviderResolver.resolveWithDurableDefault(
                NativeShuffleManagerFactory.NAME, Map.of());
        assertThat(provider.capabilities().durability()).isEqualTo(ShuffleDurability.EPHEMERAL);
    }

    @Test
    void resolveWithDurableDefaultBlankNameTriggersAutoSelection() {
        ShuffleProvider provider = ShuffleProviderResolver.resolveWithDurableDefault("  ", Map.of());
        assertThat(provider).isNotNull();
        // With only native available, should still get a valid provider
        assertThat(provider.capabilities()).isNotNull();
    }

    @Test
    void selectHighestDurabilityFactoryPrefersDurableOverEphemeral() {
        // Given: factories with different durability tiers
        Map<String, ShuffleManagerFactory> factories = new TreeMap<>();
        factories.put("native", stubFactory("native", ShuffleDurability.EPHEMERAL));
        factories.put("local-spill", stubFactory("local-spill", ShuffleDurability.LOCAL_SPILL));
        factories.put("object-store", stubFactory("object-store", ShuffleDurability.DURABLE));

        ShuffleManagerFactory best = ShuffleProviderResolver.selectHighestDurabilityFactory(
                factories, Map.of());

        assertThat(best.name()).isEqualTo("object-store");
    }

    @Test
    void selectHighestDurabilityFactoryPrefersLocalSpillOverEphemeral() {
        Map<String, ShuffleManagerFactory> factories = new TreeMap<>();
        factories.put("native", stubFactory("native", ShuffleDurability.EPHEMERAL));
        factories.put("local-spill", stubFactory("local-spill", ShuffleDurability.LOCAL_SPILL));

        ShuffleManagerFactory best = ShuffleProviderResolver.selectHighestDurabilityFactory(
                factories, Map.of());

        assertThat(best.name()).isEqualTo("local-spill");
    }

    @Test
    void selectHighestDurabilityFactoryBreaksTiesAlphabetically() {
        Map<String, ShuffleManagerFactory> factories = new TreeMap<>();
        factories.put("alpha", stubFactory("alpha", ShuffleDurability.DURABLE));
        factories.put("beta", stubFactory("beta", ShuffleDurability.DURABLE));

        ShuffleManagerFactory best = ShuffleProviderResolver.selectHighestDurabilityFactory(
                factories, Map.of());

        // Alphabetical tie-break: beta > alpha
        assertThat(best.name()).isEqualTo("beta");
    }

    /** Builds a stub factory that produces a provider with the given durability. */
    private static ShuffleManagerFactory stubFactory(String name, ShuffleDurability durability) {
        return new ShuffleManagerFactory() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public ShuffleProvider create(ShuffleContext context) {
                return new DefaultShuffleProvider(
                        null, null, ShuffleCapabilities.of(durability));
            }
        };
    }
}
