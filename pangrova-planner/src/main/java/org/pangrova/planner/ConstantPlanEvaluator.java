/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.planner;

import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.connector.data.TypedRow;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Evaluates constant Calcite plans (LogicalValues, tableless expressions) into
 * framework-agnostic {@link TypedRow} results. This keeps Calcite types confined
 * to the planner module — downstream consumers (coordinator) receive only SPI types.
 */
public final class ConstantPlanEvaluator {

    private ConstantPlanEvaluator() {}

    /**
     * Result of evaluating a constant plan: column names + typed rows.
     */
    public record EvaluatedConstant(List<String> columnNames, List<TypedRow> rows) {
        public EvaluatedConstant {
            columnNames = List.copyOf(columnNames);
            rows = List.copyOf(rows);
        }
    }

    /**
     * Evaluates a LogicalValues node by extracting literal values directly.
     */
    public static EvaluatedConstant evaluateValues(Values values) {
        var fields = values.getRowType().getFieldList();
        List<String> columnNames = fields.stream()
                .map(RelDataTypeField::getName)
                .toList();
        var tuples = values.getTuples();
        List<TypedRow> rows = new ArrayList<>(tuples.size());
        for (var tuple : tuples) {
            Map<String, ColumnValue> rowValues = new LinkedHashMap<>();
            for (int col = 0; col < fields.size(); col++) {
                Object raw = extractLiteralValue(tuple.get(col), fields.get(col));
                rowValues.put(columnNames.get(col), ColumnValue.of(raw));
            }
            rows.add(TypedRow.of(rowValues));
        }
        return new EvaluatedConstant(columnNames, rows);
    }

    /**
     * Evaluates a tableless constant RelNode (e.g. LogicalProject over LogicalValues)
     * using Calcite's Interpreter and converts results to TypedRow.
     */
    public static EvaluatedConstant evaluateExpression(RelNode relNode) {
        var fields = relNode.getRowType().getFieldList();
        List<String> columnNames = fields.stream()
                .map(RelDataTypeField::getName)
                .toList();
        List<TypedRow> rows = new ArrayList<>();
        try (Interpreter interp = new Interpreter(constantDataContext(relNode), relNode)) {
            for (Object[] values : interp) {
                Map<String, ColumnValue> rowValues = new LinkedHashMap<>();
                for (int col = 0; col < fields.size(); col++) {
                    Object converted = convertInterpreterValue(values[col], fields.get(col));
                    rowValues.put(columnNames.get(col), ColumnValue.of(converted));
                }
                rows.add(TypedRow.of(rowValues));
            }
        }
        return new EvaluatedConstant(columnNames, rows);
    }

    /**
     * Builds a minimal {@link DataContext} that exposes only the {@link JavaTypeFactory}
     * the Calcite Interpreter needs for compiling constant expressions. Calcite's
     * built-in {@code DataContexts.EMPTY} throws {@code UnsupportedOperationException}
     * from {@code getTypeFactory()}, which breaks every constant-fold dispatch
     * (SELECT 1, SELECT UPPER('x'), etc.).
     */
    private static DataContext constantDataContext(RelNode relNode) {
        RelDataTypeFactory clusterTf = relNode.getCluster().getTypeFactory();
        final JavaTypeFactory typeFactory = (clusterTf instanceof JavaTypeFactory jtf)
                ? jtf
                : new JavaTypeFactoryImpl();
        return new DataContext() {
            @Override public SchemaPlus getRootSchema() { return null; }
            @Override public JavaTypeFactory getTypeFactory() { return typeFactory; }
            @Override public QueryProvider getQueryProvider() { return null; }
            @Override public Object get(String name) { return null; }
        };
    }

    private static Object extractLiteralValue(RexLiteral literal, RelDataTypeField field) {
        if (literal.isNull()) {
            return null;
        }
        SqlTypeName typeName = field.getType().getSqlTypeName();
        return switch (typeName) {
            case INTEGER -> literal.getValueAs(Integer.class);
            case BIGINT -> literal.getValueAs(Long.class);
            case SMALLINT -> literal.getValueAs(Short.class);
            case TINYINT -> literal.getValueAs(Byte.class);
            case FLOAT, REAL -> literal.getValueAs(Float.class);
            case DOUBLE -> literal.getValueAs(Double.class);
            case BOOLEAN -> literal.getValueAs(Boolean.class);
            case VARCHAR, CHAR -> literal.getValueAs(String.class);
            case DECIMAL -> literal.getValueAs(BigDecimal.class);
            default -> literal.getValueAs(String.class);
        };
    }

    private static Object convertInterpreterValue(Object value, RelDataTypeField field) {
        if (value == null) {
            return null;
        }
        SqlTypeName typeName = field.getType().getSqlTypeName();
        return switch (typeName) {
            case INTEGER -> (value instanceof Number n) ? n.intValue() : value;
            case BIGINT -> (value instanceof Number n) ? n.longValue() : value;
            case SMALLINT -> (value instanceof Number n) ? n.shortValue() : value;
            case TINYINT -> (value instanceof Number n) ? n.byteValue() : value;
            case FLOAT, REAL -> (value instanceof Number n) ? n.floatValue() : value;
            case DOUBLE -> (value instanceof Number n) ? n.doubleValue() : value;
            case BOOLEAN -> value;
            case VARCHAR, CHAR -> value.toString();
            case DECIMAL -> (value instanceof BigDecimal) ? value
                    : new BigDecimal(value.toString());
            default -> value.toString();
        };
    }
}
