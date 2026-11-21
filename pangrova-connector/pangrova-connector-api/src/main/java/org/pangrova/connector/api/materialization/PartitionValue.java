package org.pangrova.connector.api.materialization;

import org.pangrova.common.api.annotation.Stable;

/**
 * One identity partition column value of a changed partition.
 *
 * <p>The literal is canonical SQL, rendered by the connector for the source
 * column type (for example {@code 42}, {@code 'eu'}, or {@code DATE '2024-01-01'}).
 * A null literal means the partition value is null.
 *
 * @param column source column name of the identity partition field
 * @param literal canonical SQL literal of the partition value, or null for null
 */
@Stable(since = "1.1")
public record PartitionValue(String column, String literal) {

    public PartitionValue {
        if (column == null || column.isBlank()) {
            throw new IllegalArgumentException("partition column must not be blank");
        }
    }
}
