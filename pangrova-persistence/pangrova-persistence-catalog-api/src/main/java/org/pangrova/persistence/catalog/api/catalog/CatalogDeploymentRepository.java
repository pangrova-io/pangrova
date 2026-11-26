package org.pangrova.persistence.catalog.api.catalog;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;

import java.util.List;
import java.util.Optional;

/**
 * Current catalog deployment authority and immutable deployment history.
 *
 * <p>Implementations must be thread-safe. Saving a descriptor atomically publishes its
 * current revision, immutable history and catalog invalidation. Updates must validate
 * the incarnation and next revision; an older observation must not replace current
 * authority. A failed or indeterminate commit must be reported, never turned into absence.
 * The caller owns any injected shared resources; this port does not grant connection ownership.
 */
@Experimental
public interface CatalogDeploymentRepository {
    void saveCatalogDescriptor(CatalogDescriptor descriptor);

    Optional<CatalogDescriptor> getCatalogDescriptor(String name);

    /** Reads the current incarnation and revision used by table ownership fences. */
    default Optional<CatalogDeploymentIdentity> findCurrentDeployment(String name) {
        return getCatalogDescriptor(name).map(CatalogDescriptor::identity);
    }

    /**
     * Reads exactly the requested immutable deployment. Empty means it does not exist;
     * an implementation without revision history must reject explicitly. Never return current
     * deployment as a substitute for the requested incarnation/revision.
     */
    Optional<CatalogDescriptor> findCatalogDescriptorRevision(String name, String incarnationId, long revision);

    /**
     * Deletes only the captured deployment and appends its invalidation in the same transaction.
     * Returns false if the revision is no longer current or already absent. Storage and
     * unconfirmed commit failures propagate. History remains available to pinned tasks.
     */
    boolean deleteCatalogDescriptor(CatalogDeploymentIdentity expected);

    List<CatalogDescriptor> listAllCatalogDescriptors();
}
