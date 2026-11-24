package org.pangrova.table.api.statistics;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.table.api.identity.CatalogDeploymentIdentity;
import org.pangrova.table.api.ownership.TableControlRecord;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Reads analyzed estimates and their durable catalog deployments from the same snapshot. */
@Internal
public interface TableStatisticsSnapshotStore {
    /** Supplies one observation per requested owner, including missing data. */
    Map<TableControlRecord, Observation> observeForPlanning(Set<TableControlRecord> owners);

    record Observation(Optional<CatalogDeploymentIdentity> deployment, Optional<TableControlStatistics> statistics) { }
}
