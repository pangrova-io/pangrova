package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import java.util.Comparator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeMap;

import org.pangrova.shuffle.api.capability.ShuffleCapabilities;
import org.pangrova.shuffle.api.contract.ShuffleContext;
import org.pangrova.shuffle.api.capability.ShuffleDurability;
import org.pangrova.shuffle.api.contract.ShuffleManagerFactory;
import org.pangrova.shuffle.api.contract.ShuffleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves a {@link ShuffleProvider} by name through {@link ServiceLoader} (ADR-002 D1).
 *
 * <p>Implementations register a {@link ShuffleManagerFactory} via {@code META-INF/services} and
 * are selected by the stable {@code shuffle.manager} name. An unknown name fails fast with the
 * list of available providers rather than silently falling back to {@code native}.
 *
 * <p><b>G-HA-07 auto-selection:</b> when {@code shuffle.manager} is not explicitly configured
 * (null or blank), {@link #resolveWithDurableDefault(String, Map)} automatically selects the
 * highest-durability provider available (DURABLE > LOCAL_SPILL > EPHEMERAL), enabling
 * fault-tolerant execution by default when a durable provider is on the classpath.
 */
public final class ShuffleProviderResolver {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleProviderResolver.class);

    private ShuffleProviderResolver() {
    }

    /** Discovers all registered factories keyed by their stable {@code name()}. */
    public static Map<String, ShuffleManagerFactory> discoverFactories() {
        Map<String, ShuffleManagerFactory> byName = new TreeMap<>();
        for (ShuffleManagerFactory factory : ServiceLoader.load(
                ShuffleManagerFactory.class, ShuffleProviderResolver.class.getClassLoader())) {
            ShuffleManagerFactory previous = byName.putIfAbsent(factory.name(), factory);
            if (previous != null) {
                throw new IllegalStateException(
                        "Duplicate shuffle manager name '" + factory.name() + "': "
                                + previous.getClass().getName() + " vs "
                                + factory.getClass().getName());
            }
        }
        return byName;
    }

    /**
     * Builds the provider selected by {@code name}.
     *
     * @throws IllegalArgumentException if no factory is registered under {@code name}
     */
    public static ShuffleProvider resolve(String name, Map<String, String> properties) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("shuffle.manager name must not be blank");
        }
        Map<String, ShuffleManagerFactory> byName = discoverFactories();
        ShuffleManagerFactory factory = byName.get(name);
        if (factory == null) {
            throw new IllegalArgumentException(
                    "Unknown shuffle.manager '" + name + "'; available providers: " + byName.keySet());
        }
        return factory.create(new ShuffleContext(name, properties));
    }

    /**
     * Resolves a shuffle provider, auto-selecting the highest-durability provider
     * when no explicit name is configured (G-HA-07).
     *
     * <p>Selection logic:</p>
     * <ol>
     *   <li>If {@code name} is non-blank, delegates to {@link #resolve(String, Map)} (explicit selection).</li>
     *   <li>If {@code name} is null/blank, discovers all providers, probes each for its durability tier,
     *       and selects the one with the highest durability (DURABLE > LOCAL_SPILL > EPHEMERAL).
     *       Ties are broken alphabetically by name for determinism.</li>
     * </ol>
     *
     * <p>This makes durable exchange the default FTE mode when a durable provider is available,
     * without requiring explicit configuration. Operators who want to force ephemeral shuffle
     * (e.g., for latency-sensitive short queries) can still set {@code shuffle.manager=native}.</p>
     *
     * @param name       the configured shuffle.manager name, or null/blank for auto-selection
     * @param properties additional properties passed to the factory
     * @return the resolved shuffle provider (never null)
     * @throws IllegalStateException if no shuffle providers are registered at all
     */
    public static ShuffleProvider resolveWithDurableDefault(String name, Map<String, String> properties) {
        // Explicit name takes precedence
        if (name != null && !name.isBlank()) {
            return resolve(name, properties);
        }

        Map<String, ShuffleManagerFactory> byName = discoverFactories();
        if (byName.isEmpty()) {
            throw new IllegalStateException(
                    "No shuffle providers registered via ServiceLoader. "
                            + "Ensure at least NativeShuffleManagerFactory is on the classpath.");
        }

        // Probe each factory, select the highest durability tier
        ShuffleManagerFactory bestFactory = selectHighestDurabilityFactory(byName, properties);
        LOG.info("event=shuffle_auto_selected provider={} available={} reason=highest_durability",
                bestFactory.name(), byName.keySet());
        return bestFactory.create(new ShuffleContext(bestFactory.name(), properties));
    }

    /**
     * Selects the factory with the highest durability tier.
     * DURABLE(2) > LOCAL_SPILL(1) > EPHEMERAL(0). Ties broken alphabetically.
     */
    static ShuffleManagerFactory selectHighestDurabilityFactory(
            Map<String, ShuffleManagerFactory> byName,
            Map<String, String> properties) {
        return byName.values().stream()
                .max(Comparator
                        .<ShuffleManagerFactory>comparingInt(f -> probeDurabilityOrdinal(f, properties))
                        .thenComparing(ShuffleManagerFactory::name))
                .orElseThrow(() -> new IllegalStateException("No shuffle providers available"));
    }

    /**
     * Probes a factory's durability by creating a temporary provider.
     * Returns ordinal: DURABLE=2, LOCAL_SPILL=1, EPHEMERAL=0.
     */
    private static int probeDurabilityOrdinal(ShuffleManagerFactory factory,
                                               Map<String, String> properties) {
        try {
            ShuffleProvider probe = factory.create(new ShuffleContext(factory.name(), properties));
            ShuffleCapabilities caps = probe.capabilities();
            ShuffleDurability durability = caps.durability();
            // Close the probe provider — we only needed its capabilities
            try {
                probe.close();
            } catch (Exception e) {
                LOG.warn("event=shuffle_probe_close_failed provider={} error={}", factory.name(), e.getMessage());
            }
            return switch (durability) {
                case DURABLE -> 2;
                case LOCAL_SPILL -> 1;
                case EPHEMERAL -> 0;
            };
        } catch (Exception e) {
            LOG.warn("event=shuffle_probe_failed provider={} error={}", factory.name(), e.getMessage());
            return -1; // Probe failure — rank below EPHEMERAL
        }
    }
}
