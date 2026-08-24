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
import org.pangrova.spi.index.vector.VectorIndexLoader;
import org.pangrova.spi.index.vector.VectorSearchRequest;
import org.pangrova.spi.index.vector.VectorSearchResult;
import org.pangrova.worker.execution.VectorSearchExecutor;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class VectorSearchTableFunctionTest {

    private static final VectorSearchResult TWO_NEIGHBORS = new VectorSearchResult(
            "file.parquet",
            List.of(
                    new VectorSearchResult.ScoredRow(10L, 0.1),
                    new VectorSearchResult.ScoredRow(20L, 0.2)));

    private final VectorSearchTableFunction function = new VectorSearchTableFunction(
            new VectorSearchExecutor(new FixedLoader(TWO_NEIGHBORS)));

    @Test
    void nameIsVectorSearch() {
        assertEquals("vector_search", function.getName());
    }

    @Test
    void executeReturnsScoredRows() {
        List<TypedRow> rows = collect(function.execute(List.of(
                new ColumnValue.StringValue("cat.schema.tbl"),
                new ColumnValue.StringValue("embedding"),
                new ColumnValue.StringValue("[0.1, 0.2, 0.3]"),
                new ColumnValue.IntValue(5))));

        assertEquals(2, rows.size());
        assertEquals(10L, rows.get(0).getLong("row_id"));
        assertEquals(0.1, rows.get(0).getDouble("distance"), 1e-9);
        assertEquals(20L, rows.get(1).getLong("row_id"));
        assertEquals(0.2, rows.get(1).getDouble("distance"), 1e-9);
    }

    @Test
    void emptyVectorProducesNoRows() {
        VectorSearchTableFunction fn = new VectorSearchTableFunction(
                new VectorSearchExecutor(new FixedLoader(
                        new VectorSearchResult("f.parquet", List.of()))));

        List<TypedRow> rows = collect(fn.execute(List.of(
                new ColumnValue.StringValue("t"),
                new ColumnValue.StringValue("v"),
                new ColumnValue.StringValue("[1.0]"),
                new ColumnValue.IntValue(3))));

        assertTrue(rows.isEmpty());
    }

    @Test
    void rejectsTooFewArguments() {
        assertThrows(IllegalArgumentException.class,
                () -> function.execute(List.of(
                        new ColumnValue.StringValue("tbl"),
                        new ColumnValue.StringValue("vec"))));
    }

    @Test
    void rejectsMalformedQueryVector() {
        assertThrows(IllegalArgumentException.class,
                () -> function.execute(List.of(
                        new ColumnValue.StringValue("tbl"),
                        new ColumnValue.StringValue("vec"),
                        new ColumnValue.StringValue("[not-a-float]"),
                        new ColumnValue.IntValue(3))));
    }

    @Test
    void bracketsAroundVectorAreOptional() {
        assertDoesNotThrow(() -> function.execute(List.of(
                new ColumnValue.StringValue("tbl"),
                new ColumnValue.StringValue("vec"),
                new ColumnValue.StringValue("0.1, 0.2"),
                new ColumnValue.IntValue(2))));
    }

    @Test
    void hasCorrectReturnColumns() {
        List<org.pangrova.spi.function.table.ReturnColumnDescriptor> cols = function.getReturnType();
        assertEquals(2, cols.size());
        assertEquals("row_id", cols.get(0).name());
        assertEquals("distance", cols.get(1).name());
    }

    // ---- helpers ----

    private static List<TypedRow> collect(Iterator<TypedRow> it) {
        List<TypedRow> rows = new ArrayList<>();
        it.forEachRemaining(rows::add);
        return rows;
    }

    private static final class FixedLoader implements VectorIndexLoader {
        private final VectorSearchResult result;
        FixedLoader(VectorSearchResult result) { this.result = result; }

        @Override
        public VectorSearchResult searchFile(String dataFilePath, String vectorColumn,
                                              VectorSearchRequest request,
                                              Map<String, String> searchParams) {
            return result;
        }
    }
}
