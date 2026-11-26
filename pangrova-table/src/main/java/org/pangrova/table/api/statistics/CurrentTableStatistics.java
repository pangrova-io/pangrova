package org.pangrova.table.api.statistics;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.table.api.ownership.TableControlRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Reads analyzed statistics matching an already authorized stable table and version. */
@Internal
@FunctionalInterface
public interface CurrentTableStatistics {
    Optional<TableControlStatistics> fresh(TableControlRecord owner);

    /** Returns the matching estimates for this set of admitted inputs. */
    default Map<TableControlRecord, TableControlStatistics> freshBatch(Set<TableControlRecord> owners) {
        Map<TableControlRecord, TableControlStatistics> results = new HashMap<>();
        for (var owner : owners) {
            fresh(owner).ifPresent(statistics -> results.put(owner, statistics));
        }
        return Map.copyOf(results);
    }
}
