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
package org.pangrova.client.jdbc;

import org.junit.jupiter.api.Test;
import org.pangrova.transport.client.ColumnDescriptor;
import org.pangrova.transport.client.ResultBatch;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.transport.client.ArrowBatchDecoder;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.Array;
import java.sql.Types;
import java.sql.ResultSetMetaData;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PangrovaResultSetVectorTest {

    @ParameterizedTest
    @CsvSource({"false,false,false", "false,false,true", "false,true,false", "false,true,true",
            "true,false,false", "true,false,true", "true,true,false", "true,true,true"})
    void actualArrowDeclarationControlsJdbcValuesMetadataAndIndependentArrayLifetime(
            boolean doublePrecision, boolean childNullable, boolean parentNullable) throws Exception {
        ResultBatch batch = actualVectorBatch(doublePrecision, childNullable, parentNullable, false);
        ResultBatch empty = actualVectorBatch(doublePrecision, childNullable, parentNullable, true);
        String className = doublePrecision
                ? (childNullable ? Double[].class.getName() : double[].class.getName())
                : (childNullable ? Float[].class.getName() : float[].class.getName());
        Array saved;
        Object expected;
        try (PangrovaResultSet result = new PangrovaResultSet(batch, null);
             PangrovaResultSet noRows = new PangrovaResultSet(empty, null)) {
            assertThat(result.getMetaData().getColumnClassName(1)).isEqualTo(className);
            assertThat(noRows.getMetaData().getColumnClassName(1)).isEqualTo(className);
            assertThat(result.getMetaData().isNullable(1)).isEqualTo(parentNullable
                    ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls);
            assertThat(noRows.next()).isFalse();
            assertThat(result.next()).isTrue();
            expected = result.getObject(1);
            assertThat(expected.getClass().getName()).isEqualTo(className);
            assertCoordinates(expected, doublePrecision, childNullable);
            assertThat(result.getString(1)).isEqualTo(doublePrecision
                    ? (childNullable ? "[1.0000000000000002, null, 3.5]"
                            : "[1.0000000000000002, -1.2345678912345679E8, 3.5]")
                    : (childNullable ? "[0.25, null, 3.5]" : "[0.25, -0.5, 3.5]"));
            assertThat(result.getMetaData().getColumnLabel(1)).isEqualTo("aliased_embedding");
            saved = result.getArray(1);
            assertThat(saved.getBaseType()).isEqualTo(doublePrecision ? Types.DOUBLE : Types.REAL);
            assertThat(saved.getBaseTypeName()).isEqualTo(doublePrecision ? "DOUBLE" : "REAL");
            assertThat(saved.getArray()).isEqualTo(expected);
            Object slice = saved.getArray(2, 1);
            assertThat(slice.getClass()).isEqualTo(expected.getClass());
            assertThat(java.lang.reflect.Array.get(slice, 0)).isEqualTo(childNullable ? null
                    : java.lang.reflect.Array.get(expected, 1));
            assertThat(java.lang.reflect.Array.getLength(saved.getArray(4, 0))).isZero();
            assertThatThrownBy(() -> saved.getArray(0, 1)).isInstanceOf(java.sql.SQLException.class);
            assertThatThrownBy(() -> saved.getArray(3, 2)).isInstanceOf(java.sql.SQLException.class);
            Object returned = saved.getArray();
            java.lang.reflect.Array.set(returned, 0, doublePrecision ? (Object) 99.0 : 99.0f);
            assertThat(saved.getArray()).isEqualTo(expected);
            if (parentNullable) {
                assertThat(result.next()).isTrue();
                assertThat(result.getObject(1)).isNull();
                assertThat(result.wasNull()).isTrue();
                assertThat(result.getArray(1)).isNull();
            }
        }
        assertThat(saved.getArray()).isEqualTo(expected);
        saved.free();
        saved.free();
        assertThatThrownBy(saved::getArray).isInstanceOf(java.sql.SQLException.class);
        assertThatThrownBy(saved::getBaseType).isInstanceOf(java.sql.SQLException.class);
        assertThatThrownBy(() -> saved.getArray(1, 0)).isInstanceOf(java.sql.SQLException.class);
    }

    private static void assertCoordinates(Object values, boolean doublePrecision, boolean nullable) {
        if (doublePrecision && nullable) {
            assertThat((Double[]) values).containsExactly(1.0000000000000002, null, 3.5);
        } else if (doublePrecision) {
            assertThat((double[]) values).containsExactly(1.0000000000000002, -123456789.12345679, 3.5);
        } else if (nullable) {
            assertThat((Float[]) values).containsExactly(0.25f, null, 3.5f);
        } else {
            assertThat((float[]) values).containsExactly(0.25f, -0.5f, 3.5f);
        }
    }

    private static ResultBatch actualVectorBatch(boolean doublePrecision, boolean childNullable,
                                                boolean parentNullable, boolean empty) throws Exception {
        Field child = new Field("item", new FieldType(childNullable, new ArrowType.FloatingPoint(
                doublePrecision ? FloatingPointPrecision.DOUBLE : FloatingPointPrecision.SINGLE), null), null);
        Field vector = new Field("aliased_embedding", new FieldType(parentNullable, ArrowType.List.INSTANCE, null,
                Map.of("ARROW:extension:name", "pangrova.vector", "ARROW:extension:metadata", "3")), List.of(child));
        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(vector)), allocator);
             ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
            root.allocateNew();
            ListVector list = (ListVector) root.getVector(0);
            if (!empty) {
                list.startNewValue(0);
                if (doublePrecision) {
                    Float8Vector values = (Float8Vector) list.getDataVector();
                    values.setSafe(0, 1.0000000000000002);
                    values.setSafe(1, -123456789.12345679);
                    values.setSafe(2, 3.5);
                } else {
                    Float4Vector values = (Float4Vector) list.getDataVector();
                    values.setSafe(0, 0.25f);
                    values.setSafe(1, -0.5f);
                    values.setSafe(2, 3.5f);
                }
                if (childNullable) list.getDataVector().setNull(1);
                list.endValue(0, 3);
                if (parentNullable) list.setNull(1);
                root.setRowCount(parentNullable ? 2 : 1);
            }
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(bytes))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return ArrowBatchDecoder.decode(bytes.toByteArray());
        }
    }

    @Test
    void doubleVectorMetadataStringAndArrayPreservePrecisionAndOwnership() throws Exception {
        double[] coordinates = {1.0000000000000002, -123456789.12345679};
        ResultBatch batch = new ResultBatch(
                List.of(new ColumnDescriptor("embedding", Types.ARRAY, "VECTOR(2,DOUBLE)", 2, 0,
                        java.util.Optional.of(VectorLogicalType.create(2, VectorLogicalType.FLOAT64, true, false)))),
                java.util.Collections.singletonList(new Object[] {coordinates}));
        try (PangrovaResultSet result = new PangrovaResultSet(batch, null)) {
            assertThat(result.next()).isTrue();
            assertThat(result.getObject(1)).isEqualTo(coordinates);
            assertThat(result.getString(1)).isEqualTo(java.util.Arrays.toString(coordinates));
            assertThat(result.getMetaData().getColumnClassName(1)).isEqualTo(double[].class.getName());
            assertThat(result.getMetaData().getColumnTypeName(1)).isEqualTo("VECTOR(2,DOUBLE)");
            Array array = result.getArray(1);
            assertThat(array.getBaseType()).isEqualTo(Types.DOUBLE);
            assertThat(array.getBaseTypeName()).isEqualTo("DOUBLE");
            assertThat(array.getArray()).isEqualTo(coordinates);
            assertThat(array.getArray(2, 1)).isEqualTo(new double[] {coordinates[1]});
            ((double[]) array.getArray())[0] = 10;
            assertThat(array.getArray()).isEqualTo(coordinates);
            array.free();
            assertThatThrownBy(array::getArray).isInstanceOf(java.sql.SQLException.class);
        }
    }

    @Test
    void vectorObjectAndStringRetainCoordinatesAndDimension() throws Exception {
        float[] vector = {0.25f, -0.5f};
        ResultBatch batch = new ResultBatch(
                List.of(new ColumnDescriptor("embedding", Types.ARRAY, "VECTOR(2)", 2, 0,
                        java.util.Optional.of(VectorLogicalType.create(2)))),
                List.<Object[]>of(new Object[] {vector}));

        try (PangrovaResultSet result = new PangrovaResultSet(batch, null)) {
            assertThat(result.next()).isTrue();
            assertThat(result.getObject(1)).isNotSameAs(vector).isEqualTo(vector);
            assertThat(result.getString(1)).isEqualTo("[0.25, -0.5]");
            assertThat(result.getMetaData().getColumnTypeName(1)).isEqualTo("VECTOR(2)");
            assertThat(result.getMetaData().getColumnClassName(1))
                    .isEqualTo(float[].class.getName());
            Array array = result.getArray(1);
            assertThat(array.getBaseType()).isEqualTo(Types.REAL);
            assertThat(array.getArray()).isEqualTo(vector);
            assertThat(array.getArray(2, 1)).isEqualTo(new float[] {-0.5f});
            assertThatThrownBy(() -> array.getArray(3, 1))
                    .isInstanceOf(java.sql.SQLException.class);
            array.free();
            assertThatThrownBy(array::getArray)
                    .isInstanceOf(java.sql.SQLException.class);
        }
    }

    @Test
    void mutableCoordinatesRemainOwnedByTheirConsumerSnapshot() throws Exception {
        float[] vector = {0.25f, -0.5f};
        ResultBatch batch = new ResultBatch(
                List.of(new ColumnDescriptor("embedding", Types.ARRAY, "VECTOR(2)", 2, 0,
                        java.util.Optional.of(VectorLogicalType.create(2)))),
                List.<Object[]>of(new Object[] {vector}));
        vector[0] = 100;

        try (PangrovaResultSet first = new PangrovaResultSet(batch, null);
                PangrovaResultSet second = new PangrovaResultSet(batch, null)) {
            assertThat(first.next()).isTrue();
            assertThat(second.next()).isTrue();
            assertThat(first.getObject(1)).isEqualTo(new float[] {0.25f, -0.5f});
            Array array = first.getArray(1);
            try {
                ((float[]) first.getObject(1))[0] = 200;
                ((float[]) batch.rows().get(0)[0])[1] = 300;
                assertThat(second.getObject(1)).isEqualTo(new float[] {0.25f, -0.5f});
                assertThat(second.getString(1)).isEqualTo("[0.25, -0.5]");
                assertThat(batch.rows().get(0)[0]).isEqualTo(new float[] {0.25f, -0.5f});
                assertThat(array.getArray()).isEqualTo(new float[] {0.25f, -0.5f});
                ((float[]) array.getArray())[0] = 400;
                assertThat(array.getArray()).isEqualTo(new float[] {0.25f, -0.5f});
            } finally {
                array.free();
            }
            assertThatThrownBy(array::getArray).isInstanceOf(java.sql.SQLException.class);
        }
    }
}
