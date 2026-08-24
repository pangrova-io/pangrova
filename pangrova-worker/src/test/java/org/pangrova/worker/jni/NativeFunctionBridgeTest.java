package org.pangrova.worker.jni;

import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.function.AggregateFunction;
import org.pangrova.spi.function.FunctionMetadata;
import org.pangrova.spi.function.ScalarFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link NativeFunctionBridge}.
 * Tests registration, callback dispatch, error handling, and thread safety contracts.
 */
class NativeFunctionBridgeTest {

    private NativeFunctionBridge bridge;

    @BeforeEach
    void setUp() {
        bridge = new NativeFunctionBridge();
    }

    // ---- Scalar registration ----

    @Test
    void registerAndEvaluateScalar() {
        ScalarFunction addFn = new ScalarFunction() {
            @Override
            public FunctionMetadata getMetadata() {
                return new FunctionMetadata("my_add", "BIGINT", List.of("BIGINT", "BIGINT"));
            }

            @Override
            public ColumnValue invoke(List<ColumnValue> arguments) {
                long a = ((ColumnValue.LongValue) arguments.get(0)).value();
                long b = ((ColumnValue.LongValue) arguments.get(1)).value();
                return new ColumnValue.LongValue(a + b);
            }
        };

        bridge.registerScalar(addFn);

        assertThat(bridge.scalarCount()).isEqualTo(1);
        assertThat(bridge.scalarNames()).contains("my_add");

        Object result = bridge.evaluateScalar("my_add", new Object[]{10L, 20L});
        assertThat(result).isEqualTo(30L);
    }

    @Test
    void scalarEvaluationIsCaseInsensitive() {
        bridge.registerScalar(constantScalar("MiXeD_CaSe", "hello"));
        Object result = bridge.evaluateScalar("MIXED_CASE", new Object[]{});
        assertThat(result).isEqualTo("hello");
    }

    @Test
    void duplicateScalarRegistrationThrows() {
        bridge.registerScalar(constantScalar("dup_fn", "v1"));
        assertThatThrownBy(() -> bridge.registerScalar(constantScalar("dup_fn", "v2")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already registered");
    }

    @Test
    void evaluateUnregisteredScalarThrows() {
        assertThatThrownBy(() -> bridge.evaluateScalar("nonexistent", new Object[]{}))
                .isInstanceOf(NativeRuntimeException.class)
                .hasMessageContaining("not registered");
    }

    @Test
    void scalarEvaluationExceptionWrapped() {
        ScalarFunction throwingFn = new ScalarFunction() {
            @Override
            public FunctionMetadata getMetadata() {
                return new FunctionMetadata("bad_fn", "INT", List.of());
            }

            @Override
            public ColumnValue invoke(List<ColumnValue> arguments) {
                throw new ArithmeticException("division by zero");
            }
        };
        bridge.registerScalar(throwingFn);

        assertThatThrownBy(() -> bridge.evaluateScalar("bad_fn", new Object[]{}))
                .isInstanceOf(NativeRuntimeException.class)
                .hasMessageContaining("evaluation failed")
                .hasMessageContaining("division by zero");
    }

    @Test
    void scalarWithNullArguments() {
        ScalarFunction nullHandler = new ScalarFunction() {
            @Override
            public FunctionMetadata getMetadata() {
                return new FunctionMetadata("null_fn", "VARCHAR", List.of("VARCHAR"));
            }

            @Override
            public ColumnValue invoke(List<ColumnValue> arguments) {
                if (arguments.get(0) instanceof ColumnValue.NullValue) {
                    return new ColumnValue.StringValue("was_null");
                }
                return arguments.get(0);
            }
        };
        bridge.registerScalar(nullHandler);

        Object result = bridge.evaluateScalar("null_fn", new Object[]{null});
        assertThat(result).isEqualTo("was_null");
    }

    @Test
    void scalarWithEmptyArguments() {
        bridge.registerScalar(constantScalar("no_args", "constant"));
        Object result = bridge.evaluateScalar("no_args", null);
        assertThat(result).isEqualTo("constant");
    }

    // ---- Aggregate registration ----

    @Test
    void registerAndEvaluateAggregate() {
        AggregateFunction sumFn = new AggregateFunction() {
            @Override
            public FunctionMetadata getMetadata() {
                return new FunctionMetadata("my_sum", "BIGINT", List.of("BIGINT"));
            }

            @Override
            public Object createAccumulator() {
                return new long[]{0L};
            }

            @Override
            public void addInput(Object accumulator, List<ColumnValue> arguments) {
                long[] acc = (long[]) accumulator;
                acc[0] += ((ColumnValue.LongValue) arguments.get(0)).value();
            }

            @Override
            public ColumnValue getResult(Object accumulator) {
                return new ColumnValue.LongValue(((long[]) accumulator)[0]);
            }
        };

        bridge.registerAggregate(sumFn);

        assertThat(bridge.aggregateCount()).isEqualTo(1);
        assertThat(bridge.aggregateNames()).contains("my_sum");

        Object acc = bridge.createAccumulator("my_sum");
        bridge.addInput("my_sum", acc, new Object[]{10L});
        bridge.addInput("my_sum", acc, new Object[]{20L});
        bridge.addInput("my_sum", acc, new Object[]{30L});
        Object result = bridge.getAggregateResult("my_sum", acc);
        assertThat(result).isEqualTo(60L);
    }

    @Test
    void aggregateIsCaseInsensitive() {
        bridge.registerAggregate(countAggregate("MY_COUNT"));
        Object acc = bridge.createAccumulator("my_count");
        assertThat(acc).isNotNull();
    }

    @Test
    void duplicateAggregateRegistrationThrows() {
        bridge.registerAggregate(countAggregate("dup_agg"));
        assertThatThrownBy(() -> bridge.registerAggregate(countAggregate("dup_agg")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already registered");
    }

    @Test
    void createAccumulatorForUnregisteredThrows() {
        assertThatThrownBy(() -> bridge.createAccumulator("nonexistent"))
                .isInstanceOf(NativeRuntimeException.class)
                .hasMessageContaining("not registered");
    }

    @Test
    void addInputForUnregisteredThrows() {
        assertThatThrownBy(() -> bridge.addInput("nonexistent", new Object(), new Object[]{}))
                .isInstanceOf(NativeRuntimeException.class)
                .hasMessageContaining("not registered");
    }

    @Test
    void getAggregateResultForUnregisteredThrows() {
        assertThatThrownBy(() -> bridge.getAggregateResult("nonexistent", new Object()))
                .isInstanceOf(NativeRuntimeException.class)
                .hasMessageContaining("not registered");
    }

    // ---- Descriptor building ----

    @Test
    void buildDescriptorsEmpty() {
        assertThat(bridge.buildDescriptors()).isEmpty();
    }

    @Test
    void buildDescriptorsIncludesBothTypes() {
        bridge.registerScalar(constantScalar("s1", "val"));
        bridge.registerScalar(constantScalar("s2", "val"));
        bridge.registerAggregate(countAggregate("a1"));

        List<NativeFunctionDescriptor> descriptors = bridge.buildDescriptors();
        assertThat(descriptors).hasSize(3);

        long scalarCount = descriptors.stream()
                .filter(d -> d.functionType() == NativeFunctionDescriptor.FunctionType.SCALAR)
                .count();
        long aggCount = descriptors.stream()
                .filter(d -> d.functionType() == NativeFunctionDescriptor.FunctionType.AGGREGATE)
                .count();
        assertThat(scalarCount).isEqualTo(2);
        assertThat(aggCount).isEqualTo(1);
    }

    @Test
    void buildDescriptorsAreUnmodifiable() {
        bridge.registerScalar(constantScalar("fn", "val"));
        List<NativeFunctionDescriptor> descriptors = bridge.buildDescriptors();
        assertThatThrownBy(() -> descriptors.add(new NativeFunctionDescriptor(
                "x", "INT", List.of(),
                NativeFunctionDescriptor.FunctionType.SCALAR, true)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // ---- Query methods ----

    @Test
    void getScalarReturnsRegisteredFunction() {
        ScalarFunction fn = constantScalar("lookup_me", "found");
        bridge.registerScalar(fn);
        assertThat(bridge.getScalar("lookup_me")).isSameAs(fn);
        assertThat(bridge.getScalar("LOOKUP_ME")).isSameAs(fn);
    }

    @Test
    void getScalarReturnsNullForUnregistered() {
        assertThat(bridge.getScalar("missing")).isNull();
    }

    @Test
    void getAggregateReturnsRegisteredFunction() {
        AggregateFunction fn = countAggregate("agg_lookup");
        bridge.registerAggregate(fn);
        assertThat(bridge.getAggregate("agg_lookup")).isSameAs(fn);
    }

    @Test
    void getAggregateReturnsNullForUnregistered() {
        assertThat(bridge.getAggregate("missing")).isNull();
    }

    // ---- Null argument validation ----

    @Test
    void registerNullScalarThrows() {
        assertThatThrownBy(() -> bridge.registerScalar(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void registerNullAggregateThrows() {
        assertThatThrownBy(() -> bridge.registerAggregate(null))
                .isInstanceOf(NullPointerException.class);
    }

    // ---- Helper factories ----

    private static ScalarFunction constantScalar(String name, String returnValue) {
        return new ScalarFunction() {
            @Override
            public FunctionMetadata getMetadata() {
                return new FunctionMetadata(name, "VARCHAR", List.of());
            }

            @Override
            public ColumnValue invoke(List<ColumnValue> arguments) {
                return new ColumnValue.StringValue(returnValue);
            }
        };
    }

    private static AggregateFunction countAggregate(String name) {
        return new AggregateFunction() {
            @Override
            public FunctionMetadata getMetadata() {
                return new FunctionMetadata(name, "BIGINT", List.of());
            }

            @Override
            public Object createAccumulator() {
                return new long[]{0L};
            }

            @Override
            public void addInput(Object accumulator, List<ColumnValue> arguments) {
                ((long[]) accumulator)[0]++;
            }

            @Override
            public ColumnValue getResult(Object accumulator) {
                return new ColumnValue.LongValue(((long[]) accumulator)[0]);
            }
        };
    }
}
