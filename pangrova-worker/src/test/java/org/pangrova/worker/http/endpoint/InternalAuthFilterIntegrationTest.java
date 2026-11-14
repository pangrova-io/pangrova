package org.pangrova.worker.http.endpoint;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.transport.http.EmbeddedHttpServerFactory;

/** Exercises the Worker's actual Jersey authentication filter with HTTP requests. */
class InternalAuthFilterIntegrationTest {
    private static final HttpClient CLIENT = HttpClient.newHttpClient();
    private Server server;
    private int port;

    @BeforeEach
    void startServer() throws Exception {
        ResourceConfig resources = new ResourceConfig();
        resources.register(new InternalAuthFilter("worker-test-secret"));
        resources.register(ProbeResource.class);
        resources.register(JacksonFeature.class);
        ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        handler.setContextPath("/");
        handler.addServlet(new ServletHolder(new ServletContainer(resources)), "/*");
        server = EmbeddedHttpServerFactory.create(0, handler);
        server.start();
        port = EmbeddedHttpServerFactory.getPort(server);
    }

    @AfterEach
    void stopServer() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void exactHealthProbesArePublicButOtherPathsRequireInternalAuth() throws Exception {
        assertThat(get("/v1/health", null).statusCode()).isEqualTo(200);
        assertThat(get("/v1/health/live", null).statusCode()).isEqualTo(200);
        assertThat(get("/v1/health/live/extra", null).statusCode()).isEqualTo(401);
        assertThat(get("/v1/task", null).statusCode()).isEqualTo(401);
        assertThat(get("/v1/task", "worker-test-secret").statusCode()).isEqualTo(200);
    }

    private HttpResponse<String> get(String path, String internalAuth) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path)).GET();
        if (internalAuth != null) {
            request.header(InternalAuthFilter.AUTH_HEADER, internalAuth);
        }
        return CLIENT.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Path("/")
    public static class ProbeResource {
        @GET
        @Path("v1/health")
        public Response ready() {
            return Response.ok("ready").build();
        }

        @GET
        @Path("v1/health/live")
        public Response live() {
            return Response.ok("live").build();
        }

        @GET
        @Path("v1/health/live/extra")
        public Response extra() {
            return Response.ok("extra").build();
        }

        @GET
        @Path("v1/task")
        public Response task() {
            return Response.ok("task").build();
        }
    }
}
