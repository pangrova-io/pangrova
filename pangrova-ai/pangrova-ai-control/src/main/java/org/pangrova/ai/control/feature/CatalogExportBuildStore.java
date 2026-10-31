package org.pangrova.ai.control.feature;

import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.job.GenerationPage;
import org.pangrova.common.api.annotation.Internal;

import java.util.List;

/** Atomically binds a Feature View or Training Dataset version to its generic durable build. */
@Internal
public interface CatalogExportBuildStore {
    void createCatalogExportJob(CatalogExportRequest request, GenerationJob job,
            GenerationDispatchSpec dispatchSpec, List<GenerationPage> pages);
}
