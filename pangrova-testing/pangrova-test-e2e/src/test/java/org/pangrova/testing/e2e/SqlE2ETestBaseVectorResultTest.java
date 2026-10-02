package org.pangrova.testing.e2e;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.connector.data.TypedRow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SqlE2ETestBaseVectorResultTest {

    @Test
    void flightResultPreservesAnnotatedVectorAndGenericArray() {
        Field item = new Field("item", FieldType.notNullable(
                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
        Field vector = new Field("embedding", new FieldType(true,
                ArrowType.List.INSTANCE, null,
                Map.of("ARROW:extension:name", "pangrova.vector",
                        "ARROW:extension:metadata", "2")), List.of(item));
        Field array = new Field("ordinary_array", FieldType.nullable(
                ArrowType.List.INSTANCE), List.of(item));

        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(
                     new Schema(List.of(vector, array)), allocator)) {
            root.allocateNew();
            for (int col = 0; col < 2; col++) {
                ListVector list = (ListVector) root.getVector(col);
                list.startNewValue(0);
                Float4Vector child = (Float4Vector) list.getDataVector();
                child.setSafe(0, 0.25f);
                child.setSafe(1, -0.5f);
                list.endValue(0, 2);
            }
            root.setRowCount(1);

            List<TypedRow> rows = new ArrayList<>();
            SqlE2ETestBase.appendBatchRows(root, rows);

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).get("embedding"))
                    .isInstanceOf(ColumnValue.VectorValue.class);
            assertThat(rows.get(0).getRaw("embedding"))
                    .isEqualTo(new float[] {0.25f, -0.5f});
            assertThat(rows.get(0).get("ordinary_array"))
                    .isInstanceOf(ColumnValue.ArrayValue.class);
        }
    }
}
