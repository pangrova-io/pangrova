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

import org.pangrova.connector.spi.data.value.ColumnValue;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.connector.spi.data.page.Page;
import org.pangrova.connector.spi.data.page.NativePage;
import org.pangrova.connector.spi.data.type.PangrovaType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BatchSerializerVectorTest {

    @Test
    void vectorColumnRoundTrip() throws IOException {
        float[] vec1 = {0.1f, 0.2f, 0.3f};
        float[] vec2 = {0.4f, 0.5f, 0.6f};

        List<String> colNames = List.of("embedding");
        TypedRow row1 = TypedRow.builder()
                .put("embedding", new ColumnValue.VectorValue(vec1))
                .build();
        TypedRow row2 = TypedRow.builder()
                .put("embedding", new ColumnValue.VectorValue(vec2))
                .build();
        Page page = new Page(colNames, List.of(row1, row2));

        List<BatchSerializer.ColumnMeta> columns = List.of(
                new BatchSerializer.ColumnMeta("embedding", PangrovaType.VECTOR, 3));

        NativePage nativePage = BatchSerializer.serialize(page, columns);
        assertNotNull(nativePage.serializedBatch());
        assertTrue(nativePage.serializedBatch().length > 0);

        Page deserialized = BatchSerializer.deserialize(nativePage);
        assertEquals(2, deserialized.getPositionCount());

        Object raw1 = ColumnValue.toRaw(deserialized.rows().get(0).get("embedding"));
        assertInstanceOf(float[].class, raw1);
        assertArrayEquals(vec1, (float[]) raw1, 1e-6f);

        Object raw2 = ColumnValue.toRaw(deserialized.rows().get(1).get("embedding"));
        assertArrayEquals(vec2, (float[]) raw2, 1e-6f);
    }

    @Test
    void vectorColumnWithNulls() throws IOException {
        float[] vec = {1.0f, 2.0f};

        TypedRow row1 = TypedRow.builder()
                .put("v", new ColumnValue.VectorValue(vec))
                .build();
        TypedRow row2 = TypedRow.builder()
                .put("v", new ColumnValue.NullValue())
                .build();
        Page page = new Page(List.of("v"), List.of(row1, row2));

        List<BatchSerializer.ColumnMeta> columns = List.of(
                new BatchSerializer.ColumnMeta("v", PangrovaType.VECTOR, 2));

        NativePage nativePage = BatchSerializer.serialize(page, columns);
        Page deserialized = BatchSerializer.deserialize(nativePage);

        assertEquals(2, deserialized.getPositionCount());
        Object raw1 = ColumnValue.toRaw(deserialized.rows().get(0).get("v"));
        assertInstanceOf(float[].class, raw1);
        assertArrayEquals(vec, (float[]) raw1, 1e-6f);

        Object raw2 = ColumnValue.toRaw(deserialized.rows().get(1).get("v"));
        assertNull(raw2);
    }

    @Test
    void columnMetaBackwardCompatibility() {
        var meta = new BatchSerializer.ColumnMeta("col", PangrovaType.VARCHAR);
        assertEquals("col", meta.name());
        assertEquals(PangrovaType.VARCHAR, meta.type());
        assertEquals(0, meta.dimension());
    }

    @Test
    void mixedColumnsWithVector() throws IOException {
        float[] vec = {0.5f, 0.6f, 0.7f};

        TypedRow row = TypedRow.builder()
                .put("id", new ColumnValue.IntValue(42))
                .put("emb", new ColumnValue.VectorValue(vec))
                .put("name", new ColumnValue.StringValue("test"))
                .build();
        Page page = new Page(List.of("id", "emb", "name"), List.of(row));

        List<BatchSerializer.ColumnMeta> columns = List.of(
                new BatchSerializer.ColumnMeta("id", PangrovaType.INTEGER),
                new BatchSerializer.ColumnMeta("emb", PangrovaType.VECTOR, 3),
                new BatchSerializer.ColumnMeta("name", PangrovaType.VARCHAR));

        NativePage nativePage = BatchSerializer.serialize(page, columns);
        Page deserialized = BatchSerializer.deserialize(nativePage);

        assertEquals(1, deserialized.getPositionCount());
        TypedRow result = deserialized.rows().get(0);
        assertEquals(42, ColumnValue.toRaw(result.get("id")));
        assertArrayEquals(vec, (float[]) ColumnValue.toRaw(result.get("emb")), 1e-6f);
        assertEquals("test", ColumnValue.toRaw(result.get("name")));
    }
}
