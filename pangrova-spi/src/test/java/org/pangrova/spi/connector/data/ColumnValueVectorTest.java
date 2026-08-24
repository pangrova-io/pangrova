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
package org.pangrova.spi.connector.data;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ColumnValueVectorTest {

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
