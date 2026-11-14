package org.pangrova.connector.api.metadata;

import org.pangrova.connector.api.enums.SampleMethod;
import org.pangrova.common.api.annotation.Experimental;

/**
 * Describes a sample pushdown request: the sampling percentage and method.
 *
 * <p>Connectors that support sampling at the storage layer (e.g., reading only
 * a subset of Parquet row groups) can absorb this to avoid reading the full
 * dataset when the query uses {@code TABLESAMPLE}.</p>
 */
@Experimental(reason = "G-CONN-06: sample pushdown SPI")
public record SampleInfo(double percentage, SampleMethod method) {


    public SampleInfo {
        if (percentage <= 0.0 || percentage > 100.0) {
            throw new IllegalArgumentException(
                    "percentage must be in (0, 100], got " + percentage);
        }
        if (method == null) {
            throw new IllegalArgumentException("method must not be null");
        }
    }
}
