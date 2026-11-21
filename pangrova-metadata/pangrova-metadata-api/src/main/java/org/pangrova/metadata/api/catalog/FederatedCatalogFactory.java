package org.pangrova.metadata.api.catalog;

import java.util.Map;
import org.pangrova.common.api.annotation.Experimental;

/**
 * Factory for creating FederatedCatalog instances from configuration properties.
 */
@Experimental
public interface FederatedCatalogFactory {

    /** Stable, nonblank identifier unique within a process. */
    String name();

    /** Creates one catalog for a deployment. Its handle owns closing the returned instance. */
    FederatedCatalog create(Map<String, String> properties);
}
