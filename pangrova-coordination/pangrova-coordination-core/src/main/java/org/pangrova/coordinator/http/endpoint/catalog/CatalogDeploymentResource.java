package org.pangrova.coordinator.http.endpoint.catalog;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import org.pangrova.coordinator.http.endpoint.AuthenticationFilter;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.service.MetadataService;
import org.pangrova.protocol.rest.catalog.CatalogDeploymentResponse;

/** Authenticated internal endpoint for exact catalog revision deployment. */
@Path("/v1/internal/catalog-deployments")
@Produces(MediaType.APPLICATION_JSON)
public final class CatalogDeploymentResource {

    private final MetadataService metadataService;

    public CatalogDeploymentResource(MetadataService metadataService) {
        this.metadataService = java.util.Objects.requireNonNull(metadataService, "metadataService");
    }

    /** Returns deployment information for a catalog. */
    @GET
    @Path("/{catalog}")
    public CatalogDeploymentResponse get(
            @PathParam("catalog") String catalog,
            @QueryParam("incarnation") String incarnation,
            @QueryParam("revision") long revision,
            @Context ContainerRequestContext context) {
        AuthenticationFilter.requireInternalServiceIdentity(context);
        if (catalog == null || catalog.isBlank()
                || incarnation == null || incarnation.isBlank() || revision <= 0) {
            throw new NotFoundException();
        }
        CatalogDescriptor descriptor = metadataService.findCatalogDescriptorRevision(
                catalog, incarnation, revision).orElseThrow(NotFoundException::new);
        return new CatalogDeploymentResponse(
                descriptor.name(), descriptor.provider(), descriptor.incarnationId(),
                descriptor.revision(), descriptor.properties(), descriptor.allowedTenants());
    }
}
