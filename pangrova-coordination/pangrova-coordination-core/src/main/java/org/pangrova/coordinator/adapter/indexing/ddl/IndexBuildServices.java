package org.pangrova.coordinator.adapter.indexing.ddl;

import org.pangrova.indexing.core.build.IndexBuildCoordinator;

import java.util.Objects;

/** Distinct build planes: non-AI data-skipping indexes and AI Native artifacts. */
public record IndexBuildServices(
        IndexBuildCoordinator dataSkipping,
        DurableIndexBuildService aiNative) {

    public IndexBuildServices {
        Objects.requireNonNull(dataSkipping, "dataSkipping must not be null");
    }
}
