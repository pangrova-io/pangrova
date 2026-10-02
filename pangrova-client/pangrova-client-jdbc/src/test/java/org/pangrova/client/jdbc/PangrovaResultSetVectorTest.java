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
import org.pangrova.transport.ColumnDescriptor;
import org.pangrova.transport.ResultBatch;

import java.sql.Array;
import java.sql.Types;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PangrovaResultSetVectorTest {

    @Test
    void vectorObjectAndStringRetainCoordinatesAndDimension() throws Exception {
        float[] vector = {0.25f, -0.5f};
        ResultBatch batch = new ResultBatch(
                List.of(new ColumnDescriptor("embedding", Types.ARRAY, "VECTOR(2)", 2, 0)),
                List.<Object[]>of(new Object[] {vector}));

        try (PangrovaResultSet result = new PangrovaResultSet(batch, null)) {
            assertThat(result.next()).isTrue();
            assertThat(result.getObject(1)).isSameAs(vector);
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
}
