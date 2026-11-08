package org.pangrova.metadata.federation;

import java.util.Map;
import org.pangrova.metadata.api.catalog.FederatedCatalog;

/**
 * Factory for creating FederatedCatalog instances from configuration properties.
 */
public interface FederatedCatalogFactory {

    String name();

    FederatedCatalog create(Map<String, String> properties);
}
