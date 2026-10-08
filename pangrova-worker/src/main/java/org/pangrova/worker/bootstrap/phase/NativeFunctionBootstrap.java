package org.pangrova.worker.bootstrap.phase;

import org.pangrova.spi.function.AggregateFunction;
import org.pangrova.spi.function.FunctionFactory;
import org.pangrova.spi.function.ScalarFunction;
import org.pangrova.worker.bootstrap.WorkerComponentRefs;
import org.pangrova.worker.jni.NativeFunctionBridge;
import org.pangrova.worker.jni.NativeFunctionConfig;
import org.pangrova.worker.jni.NativeFunctionDescriptor;
import org.pangrova.worker.jni.NativeCapabilityLoader;
import org.pangrova.worker.jni.NativeRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Bootstrap phase that discovers user-defined functions via
 * {@link ServiceLoader} and registers them with the native execution engine.
 *
 * <p>This phase runs after {@link NativeRuntimeBootstrap} and before
 * {@link WorkerTaskBootstrap}. It:
 * <ol>
 *   <li>Discovers {@link FunctionFactory} implementations via ServiceLoader</li>
 *   <li>Collects scalar and aggregate functions from each factory</li>
 *   <li>Registers them in the {@link NativeFunctionBridge} for JNI callback dispatch</li>
 *   <li>Stores the bridge in {@link WorkerComponentRefs} for lifecycle management</li>
 * </ol>
 *
 * <p>Registration is fail-closed. A worker must not accept tasks if its Java
 * function catalog cannot be reproduced in the native expression engine.</p>
 */
public final class NativeFunctionBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(NativeFunctionBootstrap.class);

    private NativeFunctionBootstrap() {}

    /**
     * Discovers and registers UDF/UDAF functions with the native engine.
     *
     * @param refs          component refs to store the function bridge
     * @param functionConfig configuration controlling auto-registration
     */
    public static void initialize(WorkerComponentRefs refs, NativeFunctionConfig functionConfig) {
        initialize(refs, functionConfig, ServiceLoader.load(FunctionFactory.class));
    }

    static void initialize(WorkerComponentRefs refs,
                           NativeFunctionConfig functionConfig,
                           Iterable<FunctionFactory> factories) {
        if (!functionConfig.autoRegister()) {
            if (refs.nativeAvailable()) NativeCapabilityLoader.finalizeStartup();
            LOG.info("event=phase_skipped phase=native_functions reason=auto_register_disabled");
            return;
        }

        if (!refs.nativeFunctionResources().isEmpty()) {
            throw new IllegalStateException("Previous native function owners have not drained");
        }

        NativeFunctionBridge bridge = new NativeFunctionBridge();
        int scalarCount = 0;
        int aggregateCount = 0;
        int factoryCount = 0;

        try {
            for (FunctionFactory factory : factories) {
                if (factory instanceof AutoCloseable closeable) {
                    refs.retainNativeFunctionResource(closeable);
                }
                factoryCount++;
                String pluginName = factory.getName();
                Map<String, String> pluginConfig = functionConfig.configForPlugin(pluginName);
                LOG.info("event=function_factory_discovered plugin={}", pluginName);

                scalarCount += registerScalarFunctions(bridge, factory, pluginConfig, pluginName);
                aggregateCount += registerAggregateFunctions(bridge, factory, pluginConfig, pluginName);
            }

            refs.setNativeFunctionBridge(bridge);

            List<NativeFunctionDescriptor> descriptors = bridge.buildDescriptors();
            if (refs.nativeAvailable()) {
                if (!descriptors.isEmpty()) {
                    NativeRuntime.nativeRegisterFunctions(bridge, descriptors);
                }
                NativeCapabilityLoader.finalizeStartup();
            }
            LOG.info("event=native_functions_registered factories={} scalars={} aggregates={} descriptors={}",
                    factoryCount, scalarCount, aggregateCount, descriptors.size());
        } catch (RuntimeException | Error startupFailure) {
            try { refs.closeNativeFunctionResources(); }
            catch (RuntimeException | Error cleanupFailure) {
                if (cleanupFailure != startupFailure) startupFailure.addSuppressed(cleanupFailure);
            }
            throw startupFailure;
        }
    }

    /** Closes provider registries and other resources owned by function factories. */
    public static void close(WorkerComponentRefs refs) {
        refs.closeNativeFunctionResources();
    }

    private static int registerScalarFunctions(NativeFunctionBridge bridge,
                                               FunctionFactory factory,
                                               Map<String, String> pluginConfig,
                                               String pluginName) {
        int count = 0;
        List<ScalarFunction> scalars = factory.getScalarFunctions(pluginConfig);
        for (ScalarFunction fn : scalars) {
            try {
                bridge.registerScalar(fn);
                count++;
            } catch (RuntimeException e) {
                throw new IllegalStateException(
                        "Failed to register scalar function '" + fn.getMetadata().name()
                                + "' from plugin '" + pluginName + "'", e);
            }
        }
        return count;
    }

    private static int registerAggregateFunctions(NativeFunctionBridge bridge,
                                                  FunctionFactory factory,
                                                  Map<String, String> pluginConfig,
                                                  String pluginName) {
        int count = 0;
        List<AggregateFunction> aggregates = factory.getAggregateFunctions(pluginConfig);
        for (AggregateFunction fn : aggregates) {
            try {
                bridge.registerAggregate(fn);
                count++;
            } catch (RuntimeException e) {
                throw new IllegalStateException(
                        "Failed to register aggregate function '" + fn.getMetadata().name()
                                + "' from plugin '" + pluginName + "'", e);
            }
        }
        return count;
    }
}
