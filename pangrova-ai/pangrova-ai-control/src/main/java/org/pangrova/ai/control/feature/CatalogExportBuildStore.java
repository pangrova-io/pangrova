package org.pangrova.ai.control.feature;

import org.pangrova.databuild.api.build.DataBuildDispatchSpec;
import org.pangrova.databuild.api.build.DataBuildJob;
import org.pangrova.databuild.api.build.DataBuildPage;
import org.pangrova.common.api.annotation.Internal;

import java.util.List;

/** Atomically binds a Feature View or Training Dataset version to its generic durable build. */
@Internal
public interface CatalogExportBuildStore {
    void createCatalogExportJob(CatalogExportRequest request, DataBuildJob job,
            DataBuildDispatchSpec dispatchSpec, List<DataBuildPage> pages);
}
