package org.pangrova.coordinator.query.statement.dml;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.ValidationException;

/**
 * Validates DML data integrity: rejects NaN and Infinity floating-point values.
 */
final class DmlValidator {

    private DmlValidator() {}

    static void validateNoNanOrInfinity(org.pangrova.spi.connector.data.page.Page page) {
        for (org.pangrova.spi.connector.data.TypedRow row : page.rows()) {
            for (String col : row.columnNames()) {
                Object val = row.getRaw(col);
                validateNotNanOrInf(val, col);
            }
        }
    }

    private static void validateNotNanOrInf(Object val, String col) {
        if (val instanceof Double d && (d.isNaN() || d.isInfinite())) {
            throw new ValidationException(PangrovaErrorCode.VALIDATION_INVALID_ARGUMENT,
                    "Invalid floating-point value in column '" + col + "': " + d);
        }
        if (val instanceof Float f && (f.isNaN() || f.isInfinite())) {
            throw new ValidationException(PangrovaErrorCode.VALIDATION_INVALID_ARGUMENT,
                    "Invalid floating-point value in column '" + col + "': " + f);
        }
    }
}
