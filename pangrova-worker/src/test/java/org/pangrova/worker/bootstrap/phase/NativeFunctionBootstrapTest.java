package org.pangrova.worker.bootstrap.phase;

import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.function.AggregateFunction;
import org.pangrova.spi.function.FunctionFactory;
import org.pangrova.spi.function.FunctionMetadata;
import org.pangrova.spi.function.ScalarFunction;
import org.pangrova.worker.bootstrap.WorkerComponentRefs;
import org.pangrova.worker.jni.NativeFunctionBridge;
import org.pangrova.worker.jni.NativeFunctionConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link NativeFunctionBootstrap}.
 *
 * <p>Since ServiceLoader discovery depends on classpath META-INF/services,
 * these tests verify the bootstrap logic by checking configuration-driven
 * behavior (disabled config skips registration) and the component ref wiring.</p>
 */
class NativeFunctionBootstrapTest {

    @Test
    void disabledConfigSkipsRegistration() {
        WorkerComponentRefs refs = new WorkerComponentRefs();
        NativeFunctionBootstrap.initialize(refs, NativeFunctionConfig.DISABLED);

        // When auto-register is disabled, no bridge should be created
        assertThat(refs.nativeFunctionBridge()).isNull();
    }

    @Test
    void enabledConfigCreatesBridge() {
        WorkerComponentRefs refs = new WorkerComponentRefs();
        NativeFunctionBootstrap.initialize(refs, NativeFunctionConfig.DEFAULT);

        NativeFunctionBridge bridge = refs.nativeFunctionBridge();
        assertThat(bridge).isNotNull();
        // AiFunctionFactory is on the classpath via ServiceLoader => 15 AI scalar functions
        assertThat(bridge.scalarCount()).isEqualTo(15);
        assertThat(bridge.aggregateCount()).isZero();
    }

    @Test
    void bridgeDescriptorsMatchRegisteredFunctions() {
        WorkerComponentRefs refs = new WorkerComponentRefs();
        NativeFunctionBootstrap.initialize(refs, NativeFunctionConfig.DEFAULT);

        assertThat(refs.nativeFunctionBridge().buildDescriptors()).hasSize(15);
    }

    @Test
    void customPluginConfigIsPassed() {
        // Verify the config structure is preserved through the bootstrap path
        var pluginConfigs = Map.<String, Map<String, String>>of(
                "test_plugin", Map.of("key1", "value1"));
        var config = new NativeFunctionConfig(true, pluginConfigs);

        assertThat(config.configForPlugin("test_plugin"))
                .containsEntry("key1", "value1");
        assertThat(config.configForPlugin("unknown_plugin")).isEmpty();
    }

    /**
     * Verifies a FunctionFactory can produce both scalar and aggregate functions.
     * This is a structural test of the factory interface contracts.
     */
    @Test
    void functionFactoryCanProduceBothTypes() {
        FunctionFactory factory = new FunctionFactory() {
            @Override
            public String getName() {
                return "test-factory";
            }

            @Override
            public List<ScalarFunction> getScalarFunctions(Map<String, String> config) {
                return List.of(new ScalarFunction() {
                    @Override
                    public FunctionMetadata getMetadata() {
                        return new FunctionMetadata("test_scalar", "VARCHAR", List.of("VARCHAR"));
                    }

                    @Override
                    public ColumnValue invoke(List<ColumnValue> arguments) {
                        return new ColumnValue.StringValue("result");
                    }
                });
            }

            @Override
            public List<AggregateFunction> getAggregateFunctions(Map<String, String> config) {
                return List.of(new AggregateFunction() {
                    @Override
                    public FunctionMetadata getMetadata() {
                        return new FunctionMetadata("test_agg", "BIGINT", List.of("INT"));
                    }

                    @Override
                    public Object createAccumulator() {
                        return new long[]{0};
                    }

                    @Override
                    public void addInput(Object accumulator, List<ColumnValue> arguments) {
                        ((long[]) accumulator)[0]++;
                    }

                    @Override
                    public ColumnValue getResult(Object accumulator) {
                        return new ColumnValue.LongValue(((long[]) accumulator)[0]);
                    }
                });
            }
        };

        // Register manually into a bridge to verify factory output works
        NativeFunctionBridge bridge = new NativeFunctionBridge();
        for (ScalarFunction fn : factory.getScalarFunctions(Map.of())) {
            bridge.registerScalar(fn);
        }
        for (AggregateFunction fn : factory.getAggregateFunctions(Map.of())) {
            bridge.registerAggregate(fn);
        }

        assertThat(bridge.scalarCount()).isEqualTo(1);
        assertThat(bridge.aggregateCount()).isEqualTo(1);
        assertThat(bridge.buildDescriptors()).hasSize(2);

        // Verify scalar evaluation works through the bridge
        Object result = bridge.evaluateScalar("test_scalar", new Object[]{"input"});
        assertThat(result).isEqualTo("result");

        // Verify aggregate lifecycle works through the bridge
        Object acc = bridge.createAccumulator("test_agg");
        bridge.addInput("test_agg", acc, new Object[]{1});
        bridge.addInput("test_agg", acc, new Object[]{2});
        Object aggResult = bridge.getAggregateResult("test_agg", acc);
        assertThat(aggResult).isEqualTo(2L);
    }
}
