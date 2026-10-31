package org.pangrova.connector.spi.metadata;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.pangrova.common.api.annotation.Stable;

/**
 * A metadata listing's Native source and fixed string equality predicates.
 * Carries no SQL text or rows. Normal query binding freezes the actual source
 * and its declaration, and normal authorization checks its base table.
 */
@Stable(since = "1.0")
public record MetadataTableListing(MetadataTableKind source, Map<String, String> equalityFilters) {
    public MetadataTableListing {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(equalityFilters, "equalityFilters");
        var filters = new TreeMap<String, String>();
        equalityFilters.forEach((column, value) -> {
            if (column == null || column.isBlank() || value == null) {
                throw new IllegalArgumentException("Metadata filter requires a column and nonnull value");
            }
            filters.put(column, value);
        });
        equalityFilters = Collections.unmodifiableMap(filters);
    }

    public MetadataTableListing(MetadataTableKind source) {
        this(source, Map.of());
    }
}
