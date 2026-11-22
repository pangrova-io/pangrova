package org.pangrova.table.api.statistics;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.table.api.ownership.TableControlRecord;

import java.util.Optional;

/** Reads analyzed statistics matching an already authorized stable table and version. */
@Internal
@FunctionalInterface
public interface CurrentTableStatistics {
    Optional<TableControlStatistics> fresh(TableControlRecord owner);
}
