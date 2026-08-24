package org.pangrova.worker.bootstrap.phase;

import org.pangrova.spi.function.AggregateFunction;
import org.pangrova.spi.function.FunctionFactory;
import org.pangrova.spi.function.ScalarFunction;
import org.pangrova.worker.bootstrap.WorkerComponentRefs;
import org.pangrova.worker.jni.NativeFunctionBridge;
import org.pangrova.worker.jni.NativeFunctionConfig;
import org.pangrova.worker.jni.NativeFunctionDescriptor;
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
 * <p>Best-effort: individual factory or function registration failures are logged
 * but do not block worker startup. Queries referencing unregistered UDFs will
 * fail at planning time with a descriptive error.</p>
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
        if (!functionConfig.autoRegister()) {
            LOG.info("event=phase_skipped phase=native_functions reason=auto_register_disabled");
            return;
        }

        NativeFunctionBridge bridge = new NativeFunctionBridge();
        int scalarCount = 0;
        int aggregateCount = 0;
        int factoryCount = 0;

        ServiceLoader<FunctionFactory> loader = ServiceLoader.load(FunctionFactory.class);
        for (FunctionFactory factory : loader) {
            factoryCount++;
            String pluginName = factory.getName();
            Map<String, String> pluginConfig = functionConfig.configForPlugin(pluginName);
            LOG.info("event=function_factory_discovered plugin={}", pluginName);

            scalarCount += registerScalarFunctions(bridge, factory, pluginConfig, pluginName);
            aggregateCount += registerAggregateFunctions(bridge, factory, pluginConfig, pluginName);
        }

        refs.setNativeFunctionBridge(bridge);

        List<NativeFunctionDescriptor> descriptors = bridge.buildDescriptors();
        LOG.info("event=native_functions_registered factories={} scalars={} aggregates={} descriptors={}",
                factoryCount, scalarCount, aggregateCount, descriptors.size());
    }

    private static int registerScalarFunctions(NativeFunctionBridge bridge,
                                               FunctionFactory factory,
                                               Map<String, String> pluginConfig,
                                               String pluginName) {
        int count = 0;
        try {
            List<ScalarFunction> scalars = factory.getScalarFunctions(pluginConfig);
            for (ScalarFunction fn : scalars) {
                try {
                    bridge.registerScalar(fn);
                    count++;
                } catch (Exception e) {
                    LOG.warn("event=scalar_function_register_failed plugin={} function={} error={}",
                            pluginName, fn.getMetadata().name(), e.getMessage());
                }
            }
        } catch (Exception e) {
            LOG.warn("event=scalar_functions_load_failed plugin={} error={}",
                    pluginName, e.getMessage());
        }
        return count;
    }

    private static int registerAggregateFunctions(NativeFunctionBridge bridge,
                                                  FunctionFactory factory,
                                                  Map<String, String> pluginConfig,
                                                  String pluginName) {
        int count = 0;
        try {
            List<AggregateFunction> aggregates = factory.getAggregateFunctions(pluginConfig);
            for (AggregateFunction fn : aggregates) {
                try {
                    bridge.registerAggregate(fn);
                    count++;
                } catch (Exception e) {
                    LOG.warn("event=aggregate_function_register_failed plugin={} function={} error={}",
                            pluginName, fn.getMetadata().name(), e.getMessage());
                }
            }
        } catch (Exception e) {
            LOG.warn("event=aggregate_functions_load_failed plugin={} error={}",
                    pluginName, e.getMessage());
        }
        return count;
    }
}
