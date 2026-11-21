package org.pangrova.persistence.materialization.api.publication;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.materialization.api.port.PublishedMaterialization;

import java.util.List;
import java.util.Optional;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.domain.TenantId;

/**
 * Read model of published materializations.
 *
 * <p>The catalog is a cache, never the authority. Every entry is derived from
 * the durable repository and carries the registry epoch; plan and result caches
 * must include the epoch in their keys so publication or drop invalidates them.
 */
@Experimental
public interface MaterializationCatalog {

    /**
     * Looks up a published materialization by identity.
     *
     * @param object object identity
     * @return published projection when a published generation exists
     */
    Optional<PublishedMaterialization> findPublished(MaterializationId object);

    /**
     * Lists published materializations of a tenant.
     *
     * @param tenant tenant
     * @return published projections
     */
    List<PublishedMaterialization> listPublished(TenantId tenant);

    /**
     * Returns the registry epoch for a tenant.
     *
     * @param tenant tenant
     * @return monotonically increasing epoch
     */
    long registryEpoch(TenantId tenant);
}
