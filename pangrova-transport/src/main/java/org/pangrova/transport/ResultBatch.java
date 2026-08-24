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
package org.pangrova.transport;

import java.util.List;

/**
 * Type-erased result batch carrying column metadata and decoded row data.
 * Produced by {@link ArrowBatchDecoder}; contains no Arrow types.
 *
 * <p>Row values are Java primitives or {@code String}:
 * INTEGER→Integer, BIGINT→Long, FLOAT/DOUBLE→Double, VARCHAR→String, BOOLEAN→Boolean,
 * NULL→null, other→Object from Arrow {@code getObject()}.
 *
 * @param columns ordered column descriptors
 * @param rows    decoded rows; each element is an Object[] aligned with columns
 */
public record ResultBatch(List<ColumnDescriptor> columns, List<Object[]> rows) {

    public static final ResultBatch EMPTY = new ResultBatch(List.of(), List.of());

    public boolean isEmpty() {
        return rows.isEmpty();
    }

    public List<String> columnNames() {
        return columns.stream().map(ColumnDescriptor::name).toList();
    }
}
