package org.pangrova.coordinator.http.endpoint.file;

import org.pangrova.file.api.access.FileAccessGrant;

import java.util.UUID;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import org.pangrova.file.core.space.FileSpaceAccessService;
import org.pangrova.file.core.space.FileUrlService;
import org.pangrova.coordinator.file.StageFileUrlService;
import org.pangrova.coordinator.http.endpoint.AuthenticationFilter;
import org.pangrova.security.api.AccessDeniedException;
import org.pangrova.security.api.UserIdentity;

/**
 * Files API access ticket for Volume and Stage objects.
 *
 * <p>Databricks exposes file bytes through a single Files API path; Pangrova keeps the
 * same path shape but splits authorization from transfer, because the coordinator must
 * not carry media bytes. A caller asks here for one operation on one path, then sends
 * the returned signed descriptor to the named Worker, which owns storage credentials.</p>
 */
@Path("/v1")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public final class FileSpaceFilesResource {

    private final FileSpaceAccessService service;
    private final FileUrlService urls;
    private final StageFileUrlService stageUrls;

    public FileSpaceFilesResource(FileSpaceAccessService service, FileUrlService urls,
            StageFileUrlService stageUrls) {
        this.service = service;
        this.urls = urls;
        this.stageUrls = java.util.Objects.requireNonNull(stageUrls, "stageUrls");
    }

    /** Issues a signed descriptor for one file operation. */
    @POST
    @Path("/{kind}/{catalog}/{schema}/{name}/files/access")
    public FileAccessGrant access(@PathParam("kind") String kind,
            @PathParam("catalog") String catalog, @PathParam("schema") String schema,
            @PathParam("name") String name, AccessRequest request,
            @Context ContainerRequestContext context) {
        if (request == null) {
            throw new AccessDeniedException("File access request is required");
        }
        return service.grant(user(context), kind, catalog, schema, name, request.path(),
                request.operation());
    }

    /**
     * Issues a provider-signed URL for one object.
     *
     * <p>This is the bearer-token delivery mode: anyone holding the URL can use it until
     * it expires. Callers that need user-bound or re-authorized delivery must not treat
     * this URL as a scoped URL.</p>
     */
    @POST
    @Path("/{kind}/{catalog}/{schema}/{name}/files/presign")
    public FileUrlService.IssuedUrl presign(@PathParam("kind") String kind,
            @PathParam("catalog") String catalog, @PathParam("schema") String schema,
            @PathParam("name") String name, PresignRequest request,
            @Context ContainerRequestContext context) {
        if (request == null) {
            throw new AccessDeniedException("Presign request is required");
        }
        return urls.issue(user(context), kind, catalog, schema, name, request.path(),
                request.read(), request.ttlSeconds());
    }

    /** Creates a stable Stage URL which requires an authenticated, authorized role at every access. */
    @POST
    @Path("/stages/{catalog}/{schema}/{name}/files/stage-url")
    public StageUrl stageUrl(@PathParam("catalog") String catalog, @PathParam("schema") String schema,
            @PathParam("name") String name, StageUrlRequest request,
            @Context ContainerRequestContext context, @Context UriInfo uriInfo) {
        if (request == null) {
            throw new AccessDeniedException("Stage URL request is required");
        }
        return new StageUrl(stageUrls.build(user(context), catalog, schema, name, request.path(),
                uriInfo.getBaseUri()).toASCIIString());
    }

    /** Redirects an authorized read without carrying any file bytes through the Coordinator. */
    @GET
    @Path("/stage-files/{id}")
    @Produces(MediaType.WILDCARD)
    public Response stageFile(@PathParam("id") UUID id, @QueryParam("path") String path,
            @Context ContainerRequestContext context) {
        return Response.temporaryRedirect(stageUrls.read(user(context), id, path))
                .header("Cache-Control", "no-store")
                .header("Referrer-Policy", "no-referrer").build();
    }

    /** One space-relative file path. */
    public record StageUrlRequest(String path) {
    }

    /** Credential-free stable link. */
    public record StageUrl(String url) {
    }

    private static UserIdentity user(ContainerRequestContext context) {
        Object property = context == null ? null
                : context.getProperty(AuthenticationFilter.USER_IDENTITY_PROPERTY);
        if (property instanceof UserIdentity identity) {
            return identity;
        }
        throw new AccessDeniedException(
                "Authenticated principal is required for file access");
    }

    /** Requested operation and space-relative path. */
    public record AccessRequest(String path, String operation) {
    }

    /**
     * Requested URL kind and lifetime.
     *
     * @param read       {@code true} for download, {@code false} for upload
     * @param ttlSeconds requested lifetime; non-positive takes the one-hour default
     */
    public record PresignRequest(String path, boolean read, long ttlSeconds) {
    }
}
