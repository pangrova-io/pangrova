package org.pangrova.server.coordinator.http.endpoint;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Response;

import java.io.InputStream;

/**
 * Serves static Web UI resources (HTML, CSS, JS) from the classpath.
 * Resources are loaded from {@code webui/} directory on the classpath.
 */
@Path("/ui")
public final class WebUiStaticResource {

    private static final String RESOURCE_BASE = "webui/";

    @GET
    public Response index() {
        return serveResource("index.html", "text/html");
    }

    @GET
    @Path("/{path: .+}")
    public Response serve(@PathParam("path") String path) {
        String contentType = resolveContentType(path);
        return serveResource(path, contentType);
    }

    private static Response serveResource(String path, String contentType) {
        String sanitized = sanitizePath(path);
        if (sanitized == null) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        String resourcePath = RESOURCE_BASE + sanitized;
        InputStream stream = WebUiStaticResource.class.getClassLoader()
                .getResourceAsStream(resourcePath);
        if (stream == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        return Response.ok(stream, contentType).build();
    }

    private static String sanitizePath(String path) {
        if (path == null || path.contains("..") || path.contains("\\")) {
            return null;
        }
        return path.startsWith("/") ? path.substring(1) : path;
    }

    private static String resolveContentType(String path) {
        if (path.endsWith(".html")) {
            return "text/html";
        } else if (path.endsWith(".css")) {
            return "text/css";
        } else if (path.endsWith(".js")) {
            return "application/javascript";
        } else if (path.endsWith(".json")) {
            return "application/json";
        } else if (path.endsWith(".svg")) {
            return "image/svg+xml";
        } else if (path.endsWith(".png")) {
            return "image/png";
        }
        return "application/octet-stream";
    }
}
