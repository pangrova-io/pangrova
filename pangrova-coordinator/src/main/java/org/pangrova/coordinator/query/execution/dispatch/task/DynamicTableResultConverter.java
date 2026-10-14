package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.dynamictable.DdlResult;
import org.pangrova.spi.connector.data.TypedRow;

/**
 * Converts {@link DdlResult} from dynamic table operations to {@link TypedRow} lists.
 */
public final class DynamicTableResultConverter {

    private DynamicTableResultConverter() {}

    /** Converts a dynamic table DDL result into query rows. */
    public static List<TypedRow> toRows(DdlResult result) {
        if (result instanceof DdlResult.Success s) {
            return List.of(TypedRow.builder().putString("result", s.message()).build());
        }
        if (result instanceof DdlResult.Rejected r) {
            throw new IllegalStateException("Dynamic Table DDL rejected: " + r.reason());
        }
        throw new IllegalStateException("Unknown DdlResult type: " + result.getClass());
    }
}
