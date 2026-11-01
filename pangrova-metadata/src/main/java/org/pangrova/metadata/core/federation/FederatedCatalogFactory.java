package org.pangrova.metadata.core.federation;

import java.util.Map;

/**
 * Factory for creating FederatedCatalog instances from configuration properties.
 */
public interface FederatedCatalogFactory {

    String name();

    FederatedCatalog create(Map<String, String> properties);
}
