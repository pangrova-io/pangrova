package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Function;

import org.pangrova.shuffle.ShuffleContext;
import org.pangrova.shuffle.ShuffleDurability;
import org.pangrova.shuffle.ShuffleManagerFactory;
import org.pangrova.shuffle.ShuffleProvider;

/**
 * Celeborn shuffle factory (ADR-002 D4).
 *
 * <p>Adapts Apache Celeborn's {@code LifecycleManager} + {@code ShuffleClient} behind the
 * Pangrova shuffle SPI. Durability is {@link ShuffleDurability#DURABLE}: Celeborn provides
 * push-based aggregation, replication, and backend storage. The adapter maps:
 * <ul>
 *   <li>{@code registerShuffle} → Celeborn {@code registerShuffle}</li>
 *   <li>{@code ExchangeSink.write(mapId, attemptId, ...)} → Celeborn {@code pushData}</li>
 *   <li>{@code mapperEnd} → Celeborn {@code mapperEnd}</li>
 *   <li>{@code stageEnd} → Celeborn stage finalize</li>
 *   <li>{@code createSource(..., startMapIndex, endMapIndex, ...)} → Celeborn
 *       {@code readPartition}</li>
 *   <li>{@code cleanupShuffle} → Celeborn {@code unregisterShuffle}</li>
 * </ul>
 *
 * <p>This factory is always registered in ServiceLoader for name resolution and capability
 * queries. The actual provider implementation lives in the {@code pangrova-shuffle-celeborn}
 * module and is loaded via reflection when {@link #create(ShuffleContext)} is called.
 * If the module is not on the classpath, a clear error is thrown.
 */
public final class CelebornShuffleManagerFactory implements ShuffleManagerFactory {

    public static final String NAME = "celeborn";

    /**
     * FQCN of the provider builder in the pangrova-shuffle-celeborn module.
     * Implements {@code Function<ShuffleContext, ShuffleProvider>}.
     */
    private static final String BUILDER_CLASS =
            "org.pangrova.shuffle.celeborn.CelebornShuffleProviderBuilder";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ShuffleProvider create(ShuffleContext context) {
        try {
            Class<?> builderClass = Class.forName(BUILDER_CLASS);
            Function<ShuffleContext, ShuffleProvider> builder =
                    (Function<ShuffleContext, ShuffleProvider>) builderClass.getDeclaredConstructor().newInstance();
            return builder.apply(context);
        } catch (ClassNotFoundException e) {
            throw new UnsupportedOperationException(
                    "Celeborn shuffle provider module not available (ADR-002 D4-celeborn). "
                    + "Add pangrova-shuffle-celeborn to the classpath, "
                    + "or use shuffle.manager=native.", e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new IllegalStateException(
                    "Celeborn shuffle provider initialization failed: " + cause.getMessage(), cause);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(
                    "Failed to instantiate Celeborn shuffle provider builder: " + e.getMessage(), e);
        }
    }
}
