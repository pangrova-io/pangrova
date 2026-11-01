package org.pangrova.metadata.api.catalog;

import org.pangrova.metadata.api.session.MetadataSessionContext;

import java.util.List;
import java.util.Optional;

/** Catalog lifecycle use cases. */
public interface CatalogLifecyclePort {

    void createCatalog(MetadataSessionContext session, CatalogDescriptor descriptor);

    void dropCatalog(MetadataSessionContext session, String catalogName, boolean cascade);

    List<String> listCatalogs(MetadataSessionContext session);

    Optional<CatalogDescriptor> getCatalogDescriptor(String catalogName);

    Optional<CatalogDescriptor> findCatalogDescriptorRevision(
            String catalogName, String incarnationId, long revision);
}
