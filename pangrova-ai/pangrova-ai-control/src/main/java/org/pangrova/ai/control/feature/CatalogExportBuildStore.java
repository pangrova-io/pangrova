package org.pangrova.ai.control.feature;

import org.pangrova.spi.generation.job.GenerationDispatchSpec;
import org.pangrova.spi.generation.job.GenerationJob;
import org.pangrova.spi.generation.job.GenerationPage;
import org.pangrova.common.api.annotation.Internal;

import java.util.List;

/** Atomically binds a Feature View or Training Dataset version to its generic durable build. */
@Internal
public interface CatalogExportBuildStore {
    void createCatalogExportJob(CatalogExportRequest request, GenerationJob job,
            GenerationDispatchSpec dispatchSpec, List<GenerationPage> pages);
}
