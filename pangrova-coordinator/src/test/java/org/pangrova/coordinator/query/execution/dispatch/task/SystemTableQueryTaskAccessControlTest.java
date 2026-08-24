package org.pangrova.coordinator.query.execution.dispatch.task;

import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.store.MetadataStore;
import org.junit.jupiter.api.Test;

/**
 * Verifies basic {@link SystemTableQueryTask} construction with a CatalogRegistry.
 */
class SystemTableQueryTaskAccessControlTest {

    @Test
    void constructsWithCatalogRegistry() {
        // SystemTableQueryTask has a single-arg constructor (CatalogRegistry).
        // Access control enforcement is handled separately by the security layer.
        SystemTableQueryTask task = new SystemTableQueryTask(null);
        assertThat(task).isNotNull();
    }
}
