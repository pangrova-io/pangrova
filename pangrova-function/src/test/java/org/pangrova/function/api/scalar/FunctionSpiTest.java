package org.pangrova.function.api.scalar;

import org.pangrova.function.api.aggregate.AggregateFunction;
import org.pangrova.function.api.metadata.FunctionMetadata;
import org.pangrova.function.api.registry.FunctionFactory;
import org.pangrova.connector.spi.data.value.ColumnValue;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the Function/UDF SPI family (G-SPI-08).
 */
class FunctionSpiTest {

    // ── FunctionMetadata ──

    @Test
    void metadataRecordFields() {
        FunctionMetadata meta = new FunctionMetadata(
                "my_add", "BIGINT", List.of("BIGINT", "BIGINT"), true, "Adds two numbers");

        assertThat(meta.name()).isEqualTo("my_add");
        assertThat(meta.returnType()).isEqualTo("BIGINT");
        assertThat(meta.argumentTypes()).containsExactly("BIGINT", "BIGINT");
        assertThat(meta.deterministic()).isTrue();
        assertThat(meta.description()).isEqualTo("Adds two numbers");
    }

    @Test
    void metadataConvenienceConstructor() {
        FunctionMetadata meta = new FunctionMetadata("upper_case", "VARCHAR", List.of("VARCHAR"));

        assertThat(meta.deterministic()).isTrue();
        assertThat(meta.description()).isEmpty();
    }

    @Test
    void metadataNullArgumentTypesDefaultsToEmpty() {
        FunctionMetadata meta = new FunctionMetadata("now", "TIMESTAMP", null, false, "Current time");

        assertThat(meta.argumentTypes()).isEmpty();
        assertThat(meta.deterministic()).isFalse();
    }

    @Test
    void metadataRejectsNullName() {
        assertThatThrownBy(() -> new FunctionMetadata(null, "BIGINT", List.of()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void metadataRejectsNullReturnType() {
        assertThatThrownBy(() -> new FunctionMetadata("f", null, List.of()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void metadataArgumentTypesAreImmutable() {
        List<String> args = new java.util.ArrayList<>(List.of("INT"));
        FunctionMetadata meta = new FunctionMetadata("f", "INT", args);
        assertThatThrownBy(() -> meta.argumentTypes().add("BIGINT"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // ── ScalarFunction ──

    @Test
    void scalarFunctionInvocation() {
        ScalarFunction doubler = new ScalarFunction() {
            @Override
            public FunctionMetadata getMetadata() {
                return new FunctionMetadata("double_it", "BIGINT", List.of("BIGINT"));
            }

            @Override
            public ColumnValue invoke(List<ColumnValue> arguments) {
                ColumnValue arg = arguments.get(0);
                if (arg instanceof ColumnValue.LongValue lv) {
                    return new ColumnValue.LongValue(lv.value() * 2);
                }
                return new ColumnValue.NullValue();
            }
        };

        assertThat(doubler.getMetadata().name()).isEqualTo("double_it");

        ColumnValue result = doubler.invoke(List.of(new ColumnValue.LongValue(21)));
        assertThat(result).isEqualTo(new ColumnValue.LongValue(42));
    }

    @Test
    void scalarFunctionHandlesNull() {
        ScalarFunction identity = new ScalarFunction() {
            @Override
            public FunctionMetadata getMetadata() {
                return new FunctionMetadata("identity", "VARCHAR", List.of("VARCHAR"));
            }

            @Override
            public ColumnValue invoke(List<ColumnValue> arguments) {
                return arguments.get(0);
            }
        };

        ColumnValue result = identity.invoke(List.of(new ColumnValue.NullValue()));
        assertThat(result).isInstanceOf(ColumnValue.NullValue.class);
    }

    // ── AggregateFunction ──

    @Test
    void aggregateFunctionLifecycle() {
        AggregateFunction summer = new AggregateFunction() {
            @Override
            public FunctionMetadata getMetadata() {
                return new FunctionMetadata("my_sum", "BIGINT", List.of("BIGINT"));
            }

            @Override
            public Object createAccumulator() {
                return new long[]{0};
            }

            @Override
            public void addInput(Object accumulator, List<ColumnValue> arguments) {
                long[] acc = (long[]) accumulator;
                ColumnValue arg = arguments.get(0);
                if (arg instanceof ColumnValue.LongValue lv) {
                    acc[0] += lv.value();
                }
            }

            @Override
            public ColumnValue getResult(Object accumulator) {
                return new ColumnValue.LongValue(((long[]) accumulator)[0]);
            }
        };

        Object acc = summer.createAccumulator();
        summer.addInput(acc, List.of(new ColumnValue.LongValue(10)));
        summer.addInput(acc, List.of(new ColumnValue.LongValue(20)));
        summer.addInput(acc, List.of(new ColumnValue.LongValue(12)));

        ColumnValue result = summer.getResult(acc);
        assertThat(result).isEqualTo(new ColumnValue.LongValue(42));
    }

    @Test
    void aggregateFunctionEmptyGroup() {
        AggregateFunction counter = new AggregateFunction() {
            @Override
            public FunctionMetadata getMetadata() {
                return new FunctionMetadata("my_count", "BIGINT", List.of("BIGINT"));
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
        };

        // empty group — no addInput calls
        Object acc = counter.createAccumulator();
        assertThat(counter.getResult(acc)).isEqualTo(new ColumnValue.LongValue(0));
    }

    // ── FunctionFactory ──

    @Test
    void factoryProvidesScalarAndAggregateFunctions() {
        ScalarFunction scalar = new ScalarFunction() {
            @Override
            public FunctionMetadata getMetadata() {
                return new FunctionMetadata("negate", "BIGINT", List.of("BIGINT"));
            }

            @Override
            public ColumnValue invoke(List<ColumnValue> arguments) {
                if (arguments.get(0) instanceof ColumnValue.LongValue lv) {
                    return new ColumnValue.LongValue(-lv.value());
                }
                return new ColumnValue.NullValue();
            }
        };

        AggregateFunction aggregate = new AggregateFunction() {
            @Override
            public FunctionMetadata getMetadata() {
                return new FunctionMetadata("my_max", "BIGINT", List.of("BIGINT"));
            }

            @Override
            public Object createAccumulator() {
                return new long[]{Long.MIN_VALUE};
            }

            @Override
            public void addInput(Object accumulator, List<ColumnValue> arguments) {
                if (arguments.get(0) instanceof ColumnValue.LongValue lv) {
                    long[] acc = (long[]) accumulator;
                    acc[0] = Math.max(acc[0], lv.value());
                }
            }

            @Override
            public ColumnValue getResult(Object accumulator) {
                return new ColumnValue.LongValue(((long[]) accumulator)[0]);
            }
        };

        FunctionFactory factory = new FunctionFactory() {
            @Override
            public String getName() {
                return "math-extras";
            }

            @Override
            public List<ScalarFunction> getScalarFunctions(Map<String, String> config) {
                return List.of(scalar);
            }

            @Override
            public List<AggregateFunction> getAggregateFunctions(Map<String, String> config) {
                return List.of(aggregate);
            }
        };

        assertThat(factory.getName()).isEqualTo("math-extras");
        assertThat(factory.getScalarFunctions(Map.of())).hasSize(1);
        assertThat(factory.getAggregateFunctions(Map.of())).hasSize(1);
    }

    @Test
    void factoryDefaultMethodsReturnEmptyLists() {
        FunctionFactory factory = new FunctionFactory() {
            @Override
            public String getName() {
                return "empty-plugin";
            }
        };

        assertThat(factory.getScalarFunctions(Map.of())).isEmpty();
        assertThat(factory.getAggregateFunctions(Map.of())).isEmpty();
    }
}
