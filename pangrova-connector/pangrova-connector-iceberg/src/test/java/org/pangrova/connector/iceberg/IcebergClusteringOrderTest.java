package org.pangrova.connector.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.enums.ClusteringMode;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class IcebergClusteringOrderTest {
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "x", Types.LongType.get()),
            Types.NestedField.optional(2, "y", Types.LongType.get()));

    @Test
    void interleavesBothAxesAndOrdersSignedValuesAndNulls() {
        Record nearX = row(1L, 0L);
        Record nearY = row(0L, 3L);
        Record negative = row(-1L, 0L);
        Record nullAxis = row(null, 2L);
        List<Record> rows = new ArrayList<>(List.of(nullAxis, nearY, nearX, negative));
        var declaration = new IcebergClusteringOrder(ClusteringMode.INTERLEAVE, SCHEMA.columns(), "test");

        declaration.interleave(rows);

        assertThat(rows).containsExactly(negative, nearX, nearY, nullAxis);
    }

    private static Record row(Long x, Long y) {
        Record row = GenericRecord.create(SCHEMA);
        row.setField("x", x);
        row.setField("y", y);
        return row;
    }
}
