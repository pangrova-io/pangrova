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
package org.pangrova.worker.ai.function.table;

import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.function.table.ReturnColumnDescriptor;
import org.pangrova.spi.function.table.TableFunction;
import org.pangrova.spi.function.table.TableFunctionArgument;
import org.pangrova.spi.index.vector.DistanceMetric;
import org.pangrova.spi.index.vector.VectorSearchResult;
import org.pangrova.worker.execution.VectorSearchExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Table-valued function: {@code VECTOR_SEARCH(table_name, query_vector, top_k, metric)}.
 *
 * <p>Returns (row_id BIGINT, distance DOUBLE) for the top-K nearest neighbors
 * in the specified table's vector column.</p>
 */
public final class VectorSearchTableFunction implements TableFunction {

    private final VectorSearchExecutor executor;

    public VectorSearchTableFunction(VectorSearchExecutor executor) {
        this.executor = executor;
    }

    @Override
    public String getName() {
        return "vector_search";
    }

    @Override
    public List<TableFunctionArgument> getArguments() {
        return List.of(
                new TableFunctionArgument("table_name", "VARCHAR"),
                new TableFunctionArgument("vector_column", "VARCHAR"),
                new TableFunctionArgument("query_vector", "VARCHAR"),
                new TableFunctionArgument("top_k", "INTEGER"),
                new TableFunctionArgument("metric", "VARCHAR", false,
                        new ColumnValue.StringValue("COSINE")));
    }

    @Override
    public List<ReturnColumnDescriptor> getReturnType() {
        return List.of(
                new ReturnColumnDescriptor("row_id", "BIGINT"),
                new ReturnColumnDescriptor("distance", "DOUBLE"));
    }

    @Override
    public Iterator<TypedRow> execute(List<ColumnValue> arguments) {
        if (arguments.size() < 4) {
            throw new IllegalArgumentException(
                    "vector_search requires at least 4 arguments");
        }
        String tableName = extractString(arguments.get(0));
        String vectorColumn = extractString(arguments.get(1));
        float[] queryVector = parseQueryVector(extractString(arguments.get(2)));
        int topK = extractInt(arguments.get(3));
        String metricStr = arguments.size() > 4 ? extractString(arguments.get(4)) : "COSINE";
        DistanceMetric metric = DistanceMetric.valueOf(metricStr.toUpperCase());

        VectorSearchResult result = executor.execute(
                tableName, vectorColumn, queryVector, topK, metric, Map.of());

        List<TypedRow> rows = new ArrayList<>(result.neighbors().size());
        for (VectorSearchResult.ScoredRow sr : result.neighbors()) {
            rows.add(TypedRow.builder()
                    .put("row_id", new ColumnValue.LongValue(sr.rowOffset()))
                    .put("distance", new ColumnValue.DoubleValue(sr.distance()))
                    .build());
        }
        return Collections.unmodifiableList(rows).iterator();
    }

    private static String extractString(ColumnValue cv) {
        if (cv instanceof ColumnValue.StringValue sv) return sv.value();
        throw new IllegalArgumentException("Expected VARCHAR argument");
    }

    private static int extractInt(ColumnValue cv) {
        if (cv instanceof ColumnValue.IntValue iv) return iv.value();
        if (cv instanceof ColumnValue.LongValue lv) return (int) lv.value();
        throw new IllegalArgumentException("Expected INTEGER argument");
    }

    private static float[] parseQueryVector(String json) {
        String trimmed = json.trim();
        if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        String[] parts = trimmed.split(",");
        float[] result = new float[parts.length];
        for (int i = 0; i < parts.length; i++) {
            try {
                result[i] = Float.parseFloat(parts[i].trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Invalid float at index " + i + " in query_vector: '"
                                + parts[i].trim() + "'", e);
            }
        }
        return result;
    }
}
