package org.pangrova.coordinator.http.endpoint.catalog;

import jakarta.ws.rs.container.ContainerRequestContext;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.model.Resource;
import org.glassfish.jersey.servlet.ServletContainer;
import org.junit.jupiter.api.Test;
import org.pangrova.common.http.jetty.EmbeddedHttpServerFactory;
import org.pangrova.coordinator.http.endpoint.AuthenticationFilter;
import org.pangrova.coordinator.http.endpoint.CoordinatorExceptionMapper;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.coordinator.testinfra.SignedOidcIssuer;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.service.DefaultMetadataService;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.security.core.authentication.AuthenticationManager;
import org.pangrova.security.api.authentication.AuthenticationRequest;
import org.pangrova.security.auth.oauth2.OAuth2Authenticator;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.worker.nativebridge.jni.write.catalog.CatalogDeploymentClient;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real Worker HTTP client, Coordinator security filter/resource and PostgreSQL revision authority. */
class CatalogDeploymentResourceIntegrationTest extends ContainerTestBase {
    private static final String INTERNAL_SECRET = "catalog-deployment-test-secret";
    private static final List<String> REJECTED_ROUTE_ROOTS = List.of(
            "/v1/query", "/v1/internal/other", "/v1/internal/catalog-deployments-extra",
            "/v1/worker-extra", "/v1/workers-extra", "/v1/task-extra", "/v1/task-status");

    @Test
    void servesOnlyExactHistoricalSnapshotAfterRevisionAndIncarnationChange() throws Exception {
        try (var fixture = new CatalogHttpFixture()) {
            CatalogDescriptor old = descriptor("old", Set.of("tenant-a"));
            fixture.store.saveCatalogDescriptor(old);
            CatalogDescriptor newer = new CatalogDescriptor(old.name(), old.type(), old.provider(),
                    old.state(), Map.of("path", "new"), "", Set.of("tenant-b"), old.incarnationId(), 2);
            fixture.store.saveCatalogDescriptor(newer);

            var historical = fixture.client.fetch("catalog", old.incarnationId(), 1);
            assertThat(historical.properties()).containsEntry("path", "old");
            assertThat(historical.allowedTenants()).containsExactly("tenant-a");
            assertThat(historical.catalogRevision()).isEqualTo(1);
            assertThat(fixture.client.fetch("catalog", old.incarnationId(), 2).properties())
                    .containsEntry("path", "new");

            fixture.store.deleteCatalogDescriptor("catalog");
            CatalogDescriptor replacement = descriptor("replacement", Set.of());
            fixture.store.saveCatalogDescriptor(replacement);
            assertThat(fixture.client.fetch("catalog", old.incarnationId(), 1).properties())
                    .containsEntry("path", "old");
            assertThatThrownBy(() -> fixture.client.fetch("catalog", replacement.incarnationId(), 2))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("status 404");
            assertThatThrownBy(() -> fixture.client.fetch("catalog", "missing", 1))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("status 404");
            assertThatThrownBy(() -> fixture.client.fetch("catalog", old.incarnationId(), 0))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("status 404");
            assertThatThrownBy(() -> new CatalogDeploymentClient(fixture.baseUri(), "incorrect-secret")
                    .fetch("catalog", old.incarnationId(), 1))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("status 401");
        }
    }

    @Test
    void deploymentRequiresFilterIssuedInternalIdentityNotExternalIdentityOrOldHeader() throws Exception {
        try (var fixture = new CatalogHttpFixture()) {
            var catalog = descriptor("protected-path", Set.of("tenant-a"));
            fixture.store.saveCatalogDescriptor(catalog);
            String path = fixture.catalogPath("/v1/internal/catalog-deployments", catalog);
            assertThat(fixture.get(path, Map.of()).statusCode()).isEqualTo(401);
            assertThat(fixture.get(path, Map.of("Authorization", "Bearer wrong-secret")).statusCode())
                    .isEqualTo(401);
            assertThat(fixture.get(path, Map.of("X-Internal-Auth", INTERNAL_SECRET)).statusCode()).isEqualTo(401);

            String token = fixture.issuer.token("catalog-tests", true);
            var actualIdentity = fixture.authentication.authenticate(new AuthenticationRequest(
                    null, null, token, null, Map.of(), "127.0.0.1"));
            assertThat(actualIdentity.success()).isTrue();
            assertThat(actualIdentity.userIdentity().principal()).isEqualTo("alice");
            String signedUser = "Bearer " + token;
            var denied = fixture.get(path, Map.of("Authorization", signedUser));
            assertThat(denied.statusCode()).isEqualTo(403);
            assertThat(denied.body()).contains("ACCESS_DENIED", "USER_ERROR")
                    .doesNotContain("protected-path", INTERNAL_SECRET);
            var forged = fixture.get(path, Map.of("Authorization", signedUser, "X-Internal-Auth", INTERNAL_SECRET));
            assertThat(forged.statusCode()).isEqualTo(403);
            assertThat(fixture.client.fetch("catalog", catalog.incarnationId(), 1).properties())
                    .containsEntry("path", "protected-path");
        }
    }

    @Test
    void internalBearerIsConfinedToCompleteInternalRouteSegments() throws Exception {
        try (var fixture = new CatalogHttpFixture()) {
            var catalog = descriptor("protected-path", Set.of("tenant-a"));
            fixture.store.saveCatalogDescriptor(catalog);
            for (String root : REJECTED_ROUTE_ROOTS) {
                // Jersey binds the very same real handler and PG authority at these test-only paths.
                // An overly broad auth prefix would let it serve the real protected revision.
                var response = fixture.get(fixture.catalogPath(root, catalog),
                        Map.of("Authorization", "Bearer " + INTERNAL_SECRET));
                assertThat(response.statusCode()).as("internal credential isolation at %s", root).isEqualTo(403);
                assertThat(response.body()).doesNotContain("protected-path", INTERNAL_SECRET);
            }
        }
    }

    private static CatalogDescriptor descriptor(String path, Set<String> tenants) {
        return new CatalogDescriptor("catalog", CatalogType.INTERNAL, "iceberg", CatalogState.ACTIVE,
                Map.of("path", path), "", tenants);
    }

    private static final class CatalogHttpFixture implements AutoCloseable {
        private SignedOidcIssuer issuer;
        private OAuth2Authenticator authenticator;
        private AuthenticationManager authentication;
        private PostgresCatalogDeploymentRepository store;
        private CatalogDeploymentClient client;
        private Server server;
        private final HttpClient http = HttpClient.newHttpClient();

        private CatalogHttpFixture() throws Exception {
            try {
                store = new PostgresCatalogDeploymentRepository(SharedPostgresContainer.migratedDataSource("catalog_http"));
                issuer = new SignedOidcIssuer();
                authenticator = new OAuth2Authenticator();
                authenticator.init(Map.of("issuerUri", issuer.uri(), "audience", "catalog-tests"));
                authentication = new AuthenticationManager(List.of(authenticator));
                var endpoint = new CatalogDeploymentResource(
                        new DefaultMetadataService(new CatalogRegistry(store), null, null, null));
                var config = new ResourceConfig();
                config.register(new AuthenticationFilter(authentication, INTERNAL_SECRET));
                config.register(endpoint);
                config.register(CoordinatorExceptionMapper.class);
                config.register(JacksonFeature.class);
                bindActualHandlerToRejectedPaths(config, endpoint);
                var handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
                handler.setContextPath("/");
                handler.addServlet(new ServletHolder(new ServletContainer(config)), "/*");
                server = EmbeddedHttpServerFactory.create(0, handler);
                server.start();
                client = new CatalogDeploymentClient(baseUri(), INTERNAL_SECRET);
            } catch (Exception | Error failure) {
                try {
                    close();
                } catch (Exception cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        }

        private static void bindActualHandlerToRejectedPaths(ResourceConfig config,
                CatalogDeploymentResource endpoint) throws NoSuchMethodException {
            var method = CatalogDeploymentResource.class.getMethod("get", String.class, String.class,
                    long.class, ContainerRequestContext.class);
            for (String root : REJECTED_ROUTE_ROOTS) {
                var resource = Resource.builder(root + "/{catalog}");
                resource.addMethod("GET").produces("application/json").handledBy(endpoint, method);
                config.registerResources(resource.build());
            }
        }

        private String baseUri() {
            return "http://127.0.0.1:" + EmbeddedHttpServerFactory.getPort(server);
        }

        private String catalogPath(String root, CatalogDescriptor catalog) {
            return root + "/catalog?incarnation=" + catalog.incarnationId() + "&revision=1";
        }

        private HttpResponse<String> get(String path, Map<String, String> headers) throws Exception {
            var request = HttpRequest.newBuilder(URI.create(baseUri() + path)).timeout(Duration.ofSeconds(10)).GET();
            headers.forEach(request::header);
            return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        }

        @Override
        public void close() throws Exception {
            Exception failure = null;
            AutoCloseable authOwner = authentication == null ? authenticator : authentication;
            for (var owner : new AutoCloseable[]{server == null ? null : server::stop, authOwner, issuer}) {
                if (owner == null) continue;
                try {
                    owner.close();
                } catch (Exception cleanup) {
                    if (failure == null) failure = cleanup;
                    else failure.addSuppressed(cleanup);
                }
            }
            if (failure != null) throw failure;
        }
    }
}
