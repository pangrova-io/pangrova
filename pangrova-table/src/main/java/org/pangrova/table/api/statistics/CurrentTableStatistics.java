package org.pangrova.table.api.statistics;

import org.pangrova.common.api.annotation.Internal;

import java.util.Optional;

/** Reads analyzed statistics only while the captured stable table and version are current. */
@Internal
@FunctionalInterface
public interface CurrentTableStatistics {
    Optional<TableControlStatistics> fresh(String catalog, String schema, String table);
}
