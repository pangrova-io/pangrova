package org.pangrova.spi.function.table;

import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.connector.data.TypedRow;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the Table Function (PTF) SPI family (G-SPI-08).
 */
class TableFunctionSpiTest {

    // ── TableFunctionArgument ──

    @Test
    void argumentConvenienceConstructor() {
        TableFunctionArgument arg = new TableFunctionArgument("n", "INTEGER");

        assertThat(arg.name()).isEqualTo("n");
        assertThat(arg.typeName()).isEqualTo("INTEGER");
        assertThat(arg.required()).isTrue();
        assertThat(arg.defaultValue()).isNull();
    }

    @Test
    void argumentOptionalWithDefault() {
        TableFunctionArgument arg = new TableFunctionArgument(
                "separator", "VARCHAR", false, new ColumnValue.StringValue(","));

        assertThat(arg.required()).isFalse();
        assertThat(arg.defaultValue()).isEqualTo(new ColumnValue.StringValue(","));
    }

    @Test
    void argumentRejectsNullName() {
        assertThatThrownBy(() -> new TableFunctionArgument(null, "INT"))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void argumentRejectsNullType() {
        assertThatThrownBy(() -> new TableFunctionArgument("x", null))
                .isInstanceOf(NullPointerException.class);
    }

    // ── ReturnColumnDescriptor ──

    @Test
    void returnColumnDescriptorFields() {
        ReturnColumnDescriptor col = new ReturnColumnDescriptor("id", "BIGINT");

        assertThat(col.name()).isEqualTo("id");
        assertThat(col.typeName()).isEqualTo("BIGINT");
    }

    @Test
    void returnColumnDescriptorRejectsNulls() {
        assertThatThrownBy(() -> new ReturnColumnDescriptor(null, "INT"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ReturnColumnDescriptor("id", null))
                .isInstanceOf(NullPointerException.class);
    }

    // ── TableFunction ──

    @Test
    void tableFunctionExecutesAndProducesRows() {
        // A simple "sequence(n)" PTF that produces rows 1..n
        TableFunction sequenceFn = new TableFunction() {
            @Override
            public String getName() {
                return "sequence";
            }

            @Override
            public List<TableFunctionArgument> getArguments() {
                return List.of(new TableFunctionArgument("n", "INTEGER"));
            }

            @Override
            public List<ReturnColumnDescriptor> getReturnType() {
                return List.of(new ReturnColumnDescriptor("value", "INTEGER"));
            }

            @Override
            public Iterator<TypedRow> execute(List<ColumnValue> arguments) {
                int n = ((ColumnValue.IntValue) arguments.get(0)).value();
                List<TypedRow> rows = new ArrayList<>();
                for (int i = 1; i <= n; i++) {
                    rows.add(TypedRow.builder()
                            .putInt("value", i)
                            .build());
                }
                return rows.iterator();
            }
        };

        assertThat(sequenceFn.getName()).isEqualTo("sequence");
        assertThat(sequenceFn.getArguments()).hasSize(1);
        assertThat(sequenceFn.getReturnType()).hasSize(1);
        assertThat(sequenceFn.getReturnType().get(0).name()).isEqualTo("value");

        // Execute with n=3
        Iterator<TypedRow> iter = sequenceFn.execute(
                List.of(new ColumnValue.IntValue(3)));

        List<Integer> values = new ArrayList<>();
        while (iter.hasNext()) {
            values.add(iter.next().getInt("value"));
        }
        assertThat(values).containsExactly(1, 2, 3);
    }

    @Test
    void tableFunctionEmptyResult() {
        TableFunction emptyFn = new TableFunction() {
            @Override
            public String getName() {
                return "empty_table";
            }

            @Override
            public List<TableFunctionArgument> getArguments() {
                return List.of();
            }

            @Override
            public List<ReturnColumnDescriptor> getReturnType() {
                return List.of(new ReturnColumnDescriptor("dummy", "VARCHAR"));
            }

            @Override
            public Iterator<TypedRow> execute(List<ColumnValue> arguments) {
                return List.<TypedRow>of().iterator();
            }
        };

        Iterator<TypedRow> iter = emptyFn.execute(List.of());
        assertThat(iter.hasNext()).isFalse();
    }

    // ── TableFunctionFactory ──

    @Test
    void factoryProvidesTableFunctions() {
        TableFunction fn = new TableFunction() {
            @Override
            public String getName() {
                return "csv_reader";
            }

            @Override
            public List<TableFunctionArgument> getArguments() {
                return List.of(new TableFunctionArgument("path", "VARCHAR"));
            }

            @Override
            public List<ReturnColumnDescriptor> getReturnType() {
                return List.of(
                        new ReturnColumnDescriptor("line_number", "INTEGER"),
                        new ReturnColumnDescriptor("line", "VARCHAR"));
            }

            @Override
            public Iterator<TypedRow> execute(List<ColumnValue> arguments) {
                return List.<TypedRow>of().iterator();
            }
        };

        TableFunctionFactory factory = new TableFunctionFactory() {
            @Override
            public String getName() {
                return "file-readers";
            }

            @Override
            public List<TableFunction> getTableFunctions(Map<String, String> config) {
                return List.of(fn);
            }
        };

        assertThat(factory.getName()).isEqualTo("file-readers");
        List<TableFunction> functions = factory.getTableFunctions(Map.of());
        assertThat(functions).hasSize(1);
        assertThat(functions.get(0).getName()).isEqualTo("csv_reader");
        assertThat(functions.get(0).getReturnType()).hasSize(2);
    }
}
