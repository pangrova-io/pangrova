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
package org.pangrova.spi.connector.data.value;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ColumnValueVectorTest {

    @Test
    void nullableVectorCoordinatesPreserveNullsAndDetachEveryPublicSnapshot() {
        Float[] floats = {1.0f, null, 3.0f};
        Double[] doubles = {1.0000000000000002, null, -123456789.12345679};
        var floatValue = (ColumnValue.NullableVectorValue) ColumnValue.of(floats);
        var doubleValue = (ColumnValue.NullableDoubleVectorValue) ColumnValue.of(doubles);
        floats[0] = 99.0f;
        doubles[0] = 99.0;
        floatValue.value()[1] = 10.0f;
        doubleValue.value()[1] = 10.0;
        ((Float[]) ColumnValue.toRaw(floatValue))[0] = 88.0f;
        ((Double[]) ColumnValue.toRaw(doubleValue))[0] = 88.0;
        assertArrayEquals(new Float[]{1.0f, null, 3.0f}, (Float[]) ColumnValue.toRaw(floatValue));
        assertArrayEquals(new Double[]{1.0000000000000002, null, -123456789.12345679},
                (Double[]) ColumnValue.toRaw(doubleValue));
        var row = TypedRow.builder().putRaw("float", ColumnValue.toRaw(floatValue))
                .putRaw("double", ColumnValue.toRaw(doubleValue)).build();
        assertInstanceOf(Float[].class, row.getRaw("float"));
        assertInstanceOf(Double[].class, row.getRaw("double"));
    }

    @Test
    void requiredVectorAccessorsDoNotExposeOwnedCoordinates() {
        var floats = new ColumnValue.VectorValue(new float[]{1.0f});
        var doubles = new ColumnValue.DoubleVectorValue(new double[]{1.0000000000000002});
        floats.value()[0] = 99.0f;
        doubles.value()[0] = 99.0;
        assertArrayEquals(new float[]{1.0f}, floats.value());
        assertArrayEquals(new double[]{1.0000000000000002}, doubles.value());
    }

    @Test
    void doubleStorageVectorRoundTripPreservesPrecisionAndOwnership() {
        double[] source = {1.0000000000000002, -123456789.12345679};
        double[] expected = source.clone();
        ColumnValue value = ColumnValue.of(source);
        assertInstanceOf(ColumnValue.DoubleVectorValue.class, value);
        source[0] = 10;
        assertArrayEquals(expected, (double[]) ColumnValue.toRaw(value));
        double[] raw = (double[]) ColumnValue.toRaw(value);
        raw[1] = 20;
        assertArrayEquals(expected, (double[]) ColumnValue.toRaw(value));
        TypedRow row = TypedRow.builder().putRaw("vector", expected).build();
        assertArrayEquals(expected, (double[]) row.getRaw("vector"));
        assertInstanceOf(ColumnValue.ArrayValue.class, ColumnValue.of(java.util.List.of(1.0, 2.0)));
    }

    @Test
    void vectorValueRoundTrip() {
        float[] original = {0.1f, 0.2f, 0.3f};
        ColumnValue cv = ColumnValue.of(original);
        assertInstanceOf(ColumnValue.VectorValue.class, cv);

        Object raw = ColumnValue.toRaw(cv);
        assertInstanceOf(float[].class, raw);
        assertArrayEquals(original, (float[]) raw, 1e-6f);
    }

    @Test
    void vectorValueDefensiveCopyOnConstruction() {
        float[] original = {1.0f, 2.0f};
        ColumnValue.VectorValue vv = new ColumnValue.VectorValue(original);
        original[0] = 99.0f;
        assertEquals(1.0f, vv.value()[0], 1e-6f);
    }

    @Test
    void vectorValueDefensiveCopyOnToRaw() {
        float[] original = {1.0f, 2.0f};
        ColumnValue.VectorValue vv = new ColumnValue.VectorValue(original);
        float[] extracted = (float[]) ColumnValue.toRaw(vv);
        extracted[0] = 99.0f;
        assertEquals(1.0f, vv.value()[0], 1e-6f);
    }

    @Test
    void ofNullReturnsNullValue() {
        ColumnValue cv = ColumnValue.of(null);
        assertInstanceOf(ColumnValue.NullValue.class, cv);
    }

    @Test
    void toRawNullValueReturnsNull() {
        assertNull(ColumnValue.toRaw(new ColumnValue.NullValue()));
    }

    @Test
    void emptyVectorArray() {
        float[] empty = {};
        ColumnValue cv = ColumnValue.of(empty);
        assertInstanceOf(ColumnValue.VectorValue.class, cv);
        float[] raw = (float[]) ColumnValue.toRaw(cv);
        assertEquals(0, raw.length);
    }
}
