package org.pangrova.worker.bootstrap.phase;

import org.pangrova.types.value.ColumnValue;
import org.pangrova.function.spi.aggregate.AggregateFunction;
import org.pangrova.function.spi.registry.FunctionFactory;
import org.pangrova.function.api.metadata.FunctionMetadata;
import org.pangrova.function.spi.scalar.ScalarFunction;
import org.pangrova.worker.bootstrap.ownership.WorkerComponentRefs;
import org.pangrova.worker.nativebridge.jni.NativeFunctionBridge;
import org.pangrova.worker.nativebridge.jni.NativeFunctionConfig;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link NativeFunctionBootstrap}.
 *
 * <p>Function providers are installed by separate artifacts through
 * {@code META-INF/services}, so the Worker-owned tests exercise the
 * configuration and lifecycle contract with explicit factories. The AI
 * provider's own catalog is covered in its owning module.</p>
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
    void defaultConfigStartsWithoutAnyInstalledFunctionProvider() {
        WorkerComponentRefs refs = new WorkerComponentRefs();

        NativeFunctionBootstrap.initialize(refs, NativeFunctionConfig.DEFAULT);

        assertThat(refs.nativeFunctionBridge()).isNotNull();
        assertThat(refs.nativeFunctionBridge().scalarCount()).isZero();
        NativeFunctionBootstrap.close(refs);
    }

    @Test
    void configuredFactoryRegistersFunctionsAndReleasesOwnership() {
        List<String> closed = new ArrayList<>();
        WorkerComponentRefs refs = new WorkerComponentRefs();
        FunctionFactory factory = new SingleFunctionFactory(closed);

        NativeFunctionBootstrap.initialize(
                refs, NativeFunctionConfig.DEFAULT, List.of(factory));

        NativeFunctionBridge bridge = refs.nativeFunctionBridge();
        assertThat(bridge).isNotNull();
        assertThat(bridge.scalarCount()).isEqualTo(1);
        assertThat(bridge.aggregateCount()).isZero();
        assertThat(bridge.buildDescriptors()).hasSize(1);
        assertThat(refs.nativeFunctionResources()).isNotEmpty();

        NativeFunctionBootstrap.close(refs);
        assertThat(refs.nativeFunctionResources()).isEmpty();
        assertThat(closed).containsExactly("single");
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

    @Test
    void factoryFailureClosesEveryCreatedFactoryInReverseOrder() {
        List<String> closed = new ArrayList<>();
        WorkerComponentRefs refs = new WorkerComponentRefs();
        FunctionFactory first = new CloseTrackingFactory("first", closed, false);
        FunctionFactory failing = new CloseTrackingFactory("failing", closed, true);

        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                NativeFunctionBootstrap.initialize(
                        refs, NativeFunctionConfig.DEFAULT, List.of(first, failing)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("factory failed: failing");

        assertThat(closed).containsExactly("failing", "first");
        assertThat(refs.nativeFunctionResources()).isEmpty();
    }

    @Test
    void registrationFailureClosesFactory() {
        List<String> closed = new ArrayList<>();
        WorkerComponentRefs refs = new WorkerComponentRefs();
        FunctionFactory duplicateFunctions = new DuplicateFunctionFactory(closed);

        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                NativeFunctionBootstrap.initialize(
                        refs, NativeFunctionConfig.DEFAULT, List.of(duplicateFunctions)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to register scalar function");

        assertThat(closed).containsExactly("duplicate");
        assertThat(refs.nativeFunctionResources()).isEmpty();
    }

    @Test
    void duplicateFactoryIdsFailEvenWhenTheyProvideNoFunctions() {
        List<String> closed = new ArrayList<>();
        WorkerComponentRefs refs = new WorkerComponentRefs();
        var first = new CloseTrackingFactory("same-id", closed, false);
        var second = new CloseTrackingFactory("same-id", closed, false);

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> NativeFunctionBootstrap.initialize(
                refs, NativeFunctionConfig.DEFAULT, List.of(first, second)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Duplicate function factory ID: same-id");

        assertThat(closed).containsExactly("same-id", "same-id");
        assertThat(refs.nativeFunctionResources()).isEmpty();
        assertThat(refs.nativeFunctionBridge()).isNull();
    }

    @Test
    void blankFactoryIdFailsAndReleasesItsResources() {
        List<String> closed = new ArrayList<>();
        WorkerComponentRefs refs = new WorkerComponentRefs();
        var factory = new CloseTrackingFactory(" ", closed, false);

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> NativeFunctionBootstrap.initialize(
                refs, NativeFunctionConfig.DEFAULT, List.of(factory)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Function factory ID must not be blank");

        assertThat(closed).containsExactly(" ");
        assertThat(refs.nativeFunctionResources()).isEmpty();
        assertThat(refs.nativeFunctionBridge()).isNull();
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

    private static final class SingleFunctionFactory
            implements FunctionFactory, AutoCloseable {
        private final List<String> closed;

        private SingleFunctionFactory(List<String> closed) {
            this.closed = closed;
        }

        @Override
        public String getName() {
            return "single";
        }

        @Override
        public List<ScalarFunction> getScalarFunctions(Map<String, String> config) {
            return List.of(scalarFunction("single_name"));
        }

        @Override
        public List<AggregateFunction> getAggregateFunctions(Map<String, String> config) {
            return List.of();
        }

        @Override
        public void close() {
            closed.add("single");
        }
    }

    private static final class CloseTrackingFactory
            implements FunctionFactory, AutoCloseable {
        private final String name;
        private final List<String> closed;
        private final boolean fail;

        private CloseTrackingFactory(String name, List<String> closed, boolean fail) {
            this.name = name;
            this.closed = closed;
            this.fail = fail;
        }

        @Override
        public String getName() {
            if (fail) {
                throw new IllegalStateException("factory failed: " + name);
            }
            return name;
        }

        @Override
        public List<ScalarFunction> getScalarFunctions(Map<String, String> config) {
            return List.of();
        }

        @Override
        public List<AggregateFunction> getAggregateFunctions(Map<String, String> config) {
            return List.of();
        }

        @Override
        public void close() {
            closed.add(name);
        }
    }

    private static final class DuplicateFunctionFactory
            implements FunctionFactory, AutoCloseable {
        private final List<String> closed;

        private DuplicateFunctionFactory(List<String> closed) {
            this.closed = closed;
        }

        @Override
        public String getName() {
            return "duplicate";
        }

        @Override
        public List<ScalarFunction> getScalarFunctions(Map<String, String> config) {
            ScalarFunction function = scalarFunction("duplicate_name");
            return List.of(function, function);
        }

        @Override
        public List<AggregateFunction> getAggregateFunctions(Map<String, String> config) {
            return List.of();
        }

        @Override
        public void close() {
            closed.add("duplicate");
        }
    }

    private static ScalarFunction scalarFunction(String name) {
        return new ScalarFunction() {
            @Override
            public FunctionMetadata getMetadata() {
                return new FunctionMetadata(name, "VARCHAR", List.of());
            }

            @Override
            public ColumnValue invoke(List<ColumnValue> arguments) {
                return new ColumnValue.StringValue("value");
            }
        };
    }
}
