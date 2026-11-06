package org.pangrova.planner.core.analysis;

import org.pangrova.planner.core.metadata.PlanTable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pangrova.planner.api.error.MetadataChangedDuringPlanningException;
import org.pangrova.planner.spi.metadata.MetadataVersionProvider;
import org.apache.calcite.rel.RelNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pins and verifies metadata versions across catalogs referenced in a query plan
 * to ensure cross-catalog read consistency during planning.
 */
final class CrossCatalogVersionPinner {

    private static final Logger LOG = LoggerFactory.getLogger(CrossCatalogVersionPinner.class);

    private final MetadataVersionProvider metadataVersionProvider;

    CrossCatalogVersionPinner(MetadataVersionProvider metadataVersionProvider) {
        this.metadataVersionProvider = metadataVersionProvider;
    }

    /**
     * Pins metadata versions for all catalogs referenced in the plan.
     * Returns an empty map when fewer than 2 catalogs are referenced.
     */
    Map<String, Long> pinMetadataVersions(RelNode relNode) {
        if (metadataVersionProvider == null) {
            return Map.of();
        }
        Set<String> catalogs = extractCatalogs(relNode);
        if (catalogs.size() <= 1) {
            return Map.of();
        }
        Map<String, Long> versions = new HashMap<>();
        for (String catalog : catalogs) {
            versions.put(catalog, metadataVersionProvider.getLatestChangeSequence(catalog));
        }
        return versions;
    }

    /**
     * Verifies that metadata versions haven't changed since pinning.
     * Throws {@link MetadataChangedDuringPlanningException} on mismatch.
     */
    void verifyMetadataVersions(Map<String, Long> pinnedVersions) {
        if (pinnedVersions.isEmpty() || metadataVersionProvider == null) {
            return;
        }
        for (var entry : pinnedVersions.entrySet()) {
            long current = metadataVersionProvider.getLatestChangeSequence(entry.getKey());
            if (current != entry.getValue()) {
                LOG.warn("event=metadata_changed_during_planning catalog={} pinned={} current={}",
                        entry.getKey(), entry.getValue(), current);
                throw new MetadataChangedDuringPlanningException(entry.getKey());
            }
        }
    }

    /**
     * Extracts catalog names from all TableScan nodes in the plan.
     * Catalog name is the first element of the qualified table name.
     */
    static Set<String> extractCatalogs(RelNode relNode) {
        Set<String> catalogs = new HashSet<>();
        collectCatalogs(relNode, catalogs);
        return catalogs;
    }

    private static void collectCatalogs(RelNode node, Set<String> catalogs) {
        if (node instanceof org.apache.calcite.rel.core.TableScan scan) {
            List<String> qualifiedName = PlanTable.require(scan)
                    .getQualifiedName();
            if (qualifiedName.size() >= 3) {
                catalogs.add(qualifiedName.get(0));
            }
        }
        for (RelNode input : node.getInputs()) {
            collectCatalogs(input, catalogs);
        }
    }
}
