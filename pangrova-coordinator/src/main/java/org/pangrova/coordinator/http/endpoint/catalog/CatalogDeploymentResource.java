package org.pangrova.coordinator.http.endpoint.catalog;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.service.MetadataService;
import org.pangrova.protocol.rest.catalog.CatalogDeploymentResponse;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** Authenticated internal endpoint for exact catalog revision deployment. */
@Path("/v1/internal/catalog-deployments")
@Produces(MediaType.APPLICATION_JSON)
public final class CatalogDeploymentResource {

    private final MetadataService metadataService;
    private final byte[] expectedSecret;

    public CatalogDeploymentResource(MetadataService metadataService, String internalAuthSecret) {
        this.metadataService = java.util.Objects.requireNonNull(metadataService, "metadataService");
        if (internalAuthSecret == null || internalAuthSecret.isBlank()) {
            throw new IllegalArgumentException("internalAuthSecret must not be blank");
        }
        this.expectedSecret = internalAuthSecret.getBytes(StandardCharsets.UTF_8);
    }

    @GET
    @Path("/{catalog}")
    public CatalogDeploymentResponse get(
            @PathParam("catalog") String catalog,
            @QueryParam("incarnation") String incarnation,
            @QueryParam("revision") long revision,
            @HeaderParam("X-Internal-Auth") String providedSecret) {
        requireAuthenticated(providedSecret);
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

    private void requireAuthenticated(String providedSecret) {
        byte[] provided = providedSecret == null
                ? new byte[0]
                : providedSecret.getBytes(StandardCharsets.UTF_8);
        if (!MessageDigest.isEqual(expectedSecret, provided)) {
            throw new WebApplicationException("Unauthorized", Response.Status.UNAUTHORIZED);
        }
    }
}
