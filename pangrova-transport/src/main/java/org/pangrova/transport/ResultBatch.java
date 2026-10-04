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

import org.apache.arrow.vector.PeriodDuration;
import org.apache.arrow.vector.util.Text;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Type-erased result batch carrying column metadata and decoded row data.
 * Produced by {@link ArrowBatchDecoder}; its public signature contains no Arrow types.
 *
 * <p>Row values are decoded Java values: INTEGER→Integer, BIGINT→Long,
 * FLOAT/DOUBLE→Double, VARCHAR→String, BOOLEAN→Boolean, Pangrova VECTOR→float[],
 * NULL→null, and other complex values from Arrow {@code getObject()}.
 * Mutable cells are detached on input and output; immutable scalar cells are shared.
 * These heap snapshots are a client ownership boundary, not a zero-copy execution format.
 *
 * @param columns ordered column descriptors
 * @param rows    decoded rows; each element is an Object[] aligned with columns
 */
public record ResultBatch(List<ColumnDescriptor> columns, List<Object[]> rows) {

    private static final Set<Class<?>> IMMUTABLE_CELLS = Set.of(
            String.class, Boolean.class, Character.class, Byte.class, Short.class,
            Integer.class, Long.class, Float.class, Double.class, BigDecimal.class, BigInteger.class,
            java.time.LocalDate.class, java.time.LocalTime.class, java.time.LocalDateTime.class,
            java.time.OffsetTime.class, java.time.OffsetDateTime.class, java.time.ZonedDateTime.class,
            java.time.Instant.class, java.time.Duration.class, java.time.Period.class, PeriodDuration.class);

    public static final ResultBatch EMPTY = new ResultBatch(List.of(), List.of());

    /** Takes ownership by snapshot rather than retaining caller-owned collections or mutable cells. */
    public ResultBatch {
        columns = List.copyOf(columns);
        rows = snapshotRows(rows);
    }

    /** Returns a detached read-only row collection; mutable cells belong only to this returned snapshot. */
    @Override
    public List<Object[]> rows() {
        return snapshotRows(rows);
    }

    public boolean isEmpty() {
        return rows.isEmpty();
    }

    public List<String> columnNames() {
        return columns.stream().map(ColumnDescriptor::name).toList();
    }

    private static List<Object[]> snapshotRows(List<Object[]> source) {
        return source.stream().map(row -> (Object[]) snapshotCell(row, 0)).toList();
    }

    private static Object snapshotCell(Object value, int depth) {
        if (depth > 64) throw new IllegalArgumentException("Result cell nesting exceeds 64 levels");
        if (value == null || IMMUTABLE_CELLS.contains(value.getClass())) return value;
        if (value instanceof Object[] values) {
            Object[] result = values.clone();
            for (int index = 0; index < values.length; index++) {
                result[index] = snapshotCell(values[index], depth + 1);
            }
            return result;
        }
        if (value.getClass().isArray()) {
            int length = Array.getLength(value);
            Object result = Array.newInstance(value.getClass().getComponentType(), length);
            System.arraycopy(value, 0, result, 0, length);
            return result;
        }
        if (value instanceof List<?> values) {
            var result = new ArrayList<Object>(values.size());
            values.forEach(cell -> result.add(snapshotCell(cell, depth + 1)));
            return Collections.unmodifiableList(result);
        }
        if (value instanceof Map<?, ?> values) {
            var result = new LinkedHashMap<Object, Object>();
            values.forEach((key, cell) -> result.put(snapshotCell(key, depth + 1), snapshotCell(cell, depth + 1)));
            return Collections.unmodifiableMap(result);
        }
        if (value.getClass() == Text.class) return new Text((Text) value);
        if (value.getClass() == Date.class || value.getClass() == java.sql.Date.class
                || value.getClass() == java.sql.Time.class || value.getClass() == java.sql.Timestamp.class) {
            return ((Date) value).clone();
        }
        throw new IllegalArgumentException("Unsupported result cell: " + value.getClass().getName());
    }
}
