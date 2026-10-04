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
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
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

    @TempDir
    Path temporaryDirectory;

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
        NativeFunctionBootstrap.initialize(refs, configuredFunctions());

        NativeFunctionBridge bridge = refs.nativeFunctionBridge();
        assertThat(bridge).isNotNull();
        // AiFunctionFactory is on the classpath via ServiceLoader.
        assertThat(bridge.scalarCount()).isEqualTo(16);
        assertThat(bridge.aggregateCount()).isZero();
        assertThat(refs.nativeFunctionResources()).isNotEmpty();

        NativeFunctionBootstrap.close(refs);
        assertThat(refs.nativeFunctionResources()).isEmpty();
    }

    @Test
    void defaultConfigStartsWithoutAiOrFileFunctions() {
        WorkerComponentRefs refs = new WorkerComponentRefs();

        NativeFunctionBootstrap.initialize(refs, NativeFunctionConfig.DEFAULT);

        assertThat(refs.nativeFunctionBridge()).isNotNull();
        assertThat(refs.nativeFunctionBridge().scalarCount()).isZero();
        NativeFunctionBootstrap.close(refs);
    }

    @Test
    void textAiStartsWithoutFileStorage() {
        WorkerComponentRefs refs = new WorkerComponentRefs();

        NativeFunctionBootstrap.initialize(refs, textOnlyFunctions());

        assertThat(refs.nativeFunctionBridge().scalarCount()).isEqualTo(14);
        NativeFunctionBootstrap.close(refs);
    }

    @Test
    void bridgeDescriptorsMatchRegisteredFunctions() {
        WorkerComponentRefs refs = new WorkerComponentRefs();
        NativeFunctionBootstrap.initialize(refs, configuredFunctions());

        var descriptors = refs.nativeFunctionBridge().buildDescriptors();
        assertThat(descriptors).hasSize(19);
        assertThat(descriptors)
                .allSatisfy(descriptor -> {
                    assertThat(descriptor.functionType())
                            .isEqualTo(org.pangrova.worker.jni.NativeFunctionDescriptor
                                    .FunctionType.SCALAR);
                    try {
                        assertThat(org.pangrova.protocol.v2.NativeFunctionDescriptor
                                .parseFrom(descriptor.toBytes()).getFunctionKind())
                                .isEqualTo(org.pangrova.protocol.v2.NativeFunctionKind
                                        .NATIVE_FUNCTION_KIND_SCALAR);
                    } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
                        throw new AssertionError("descriptor must use the canonical v2 protocol", failure);
                    }
                });
        assertThat(descriptors)
                .filteredOn(descriptor -> descriptor.name().equals("ai_complete"))
                .extracting(descriptor -> descriptor.argumentTypes())
                .containsExactlyInAnyOrder(
                        governedSignature("VARCHAR", "VARCHAR"),
                        governedSignature("VARCHAR", "VARCHAR", "VARCHAR"),
                        governedSignature("VARCHAR", "VARCHAR", "ROW"));
        assertThat(descriptors)
                .filteredOn(descriptor -> descriptor.name().equals("ai_embed"))
                .extracting(descriptor -> descriptor.argumentTypes())
                .containsExactlyInAnyOrder(
                        governedSignature("VARCHAR", "VARCHAR", "VARCHAR"),
                        governedSignature("VARCHAR", "ROW", "VARCHAR"));
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
    void enabledFileConfigFailsClosedWhenIncomplete() {
        WorkerComponentRefs refs = new WorkerComponentRefs();
        NativeFunctionConfig incomplete = new NativeFunctionConfig(
                true, Map.of("ai-inference", Map.of("file.storage-backend", "s3")));

        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                NativeFunctionBootstrap.initialize(refs, incomplete))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("file.capability-secret-reference is required");
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

    private NativeFunctionConfig configuredFunctions() {
        Map<String, String> config = new java.util.LinkedHashMap<>(
                textOnlyFunctions().configForPlugin("ai-inference"));
        config.putAll(Map.ofEntries(
                Map.entry("file.storage-backend", "s3"),
                Map.entry("file.storage-staging-root", temporaryDirectory.toString()),
                Map.entry("file.capability-secret-reference", "test://file-capability"),
                Map.entry("file.storage.s3.bucket", "governed-files"),
                Map.entry("file.storage.s3.region", "us-east-1"),
                Map.entry("file.storage.s3.sse", "AES256")));
        return new NativeFunctionConfig(true, Map.of("ai-inference", Map.copyOf(config)));
    }

    private NativeFunctionConfig textOnlyFunctions() {
        Map<String, String> config = Map.ofEntries(
                Map.entry("models.chat.provider", "openai-compatible"),
                Map.entry("models.chat.provider-model", "chat-deployment-v1"),
                Map.entry("models.chat.version", "v1"),
                Map.entry("models.chat.endpoint",
                        "https://models.example/v1/chat/completions"),
                Map.entry("models.chat.secret-reference", "env://CHAT_KEY"),
                Map.entry("models.chat.capabilities", "inference"),
                Map.entry("models.chat.max-input-tokens", "8192"),
                Map.entry("models.chat.max-output-tokens", "1024"),
                Map.entry("models.chat.max-concurrency", "4"),
                Map.entry("models.chat.max-provider-attempts-per-task", "400"),
                Map.entry("models.chat.max-request-bytes", "262144"),
                Map.entry("models.chat.max-response-bytes", "1048576"),
                Map.entry("models.chat.max-provider-items-per-task", "100"));
        return new NativeFunctionConfig(true, Map.of("ai-inference", config));
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

    private static List<String> governedSignature(String... publicArguments) {
        var signature = new java.util.ArrayList<>(List.of(publicArguments));
        signature.addAll(org.pangrova.protocol.function.declaration.FunctionBindingDeclarations
                .governedLayout("AI_MODEL").signature());
        return List.copyOf(signature);
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
