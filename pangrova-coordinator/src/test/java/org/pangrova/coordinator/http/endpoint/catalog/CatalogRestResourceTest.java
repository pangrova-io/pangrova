package org.pangrova.coordinator.http.endpoint.catalog;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.coordinator.http.CoordinatorHttpServer;
import org.pangrova.coordinator.http.CoordinatorHttpServerContext;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.schema.TableMetadata;
import org.pangrova.metadata.api.schema.SchemaChange;
import org.pangrova.metadata.api.MetadataGateway;
import org.pangrova.metadata.api.binding.QueryMetadataLifecyclePort;
import org.pangrova.metadata.api.binding.TableResolutionPort;
import org.pangrova.metadata.api.catalog.CatalogLifecyclePort;
import org.pangrova.metadata.api.control.ColumnTagPort;
import org.pangrova.metadata.api.mutation.TableDdlPort;
import org.pangrova.metadata.api.session.MetadataSessionContext;
import org.pangrova.metadata.api.binding.ResolvedTable;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Optional;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the Catalog REST API endpoints.
 *
 * <p>Starts a minimal {@link CoordinatorHttpServer} with a stub
 * {@link MetadataService} and validates the HTTP contract for
 * namespace and table operations.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CatalogRestResourceTest {

    private CoordinatorHttpServer httpServer;
    private HttpClient httpClient;
    private String baseUrl;
    private final ObjectMapper mapper = JsonMapperFactory.create();

    @BeforeAll
    void setUp() throws Exception {
        var stub = new StubMetadataService();
        MetadataGateway stubService = new MetadataGateway(stub, stub, stub, stub, stub);

        var taskTracker = new CoordinatorTaskTracker();
        var workerManager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        var queryManager = org.pangrova.coordinator.testinfra.StatusOnlyQueryManagerFixture.create(
                workerManager, taskTracker);

        var receiverContext = TaskStatusReceiverContext.withoutSpeculation(
                queryManager, taskTracker, workerManager);
        var receiver = new TaskStatusReceiver(receiverContext);

        var serverContext = new CoordinatorHttpServerContext(
                receiver, null, null, null, null, null, null, null, null, stubService);

        httpServer = new CoordinatorHttpServer(0, serverContext);
        httpServer.start();

        baseUrl = "http://localhost:" + httpServer.getPort();
        httpClient = HttpClient.newHttpClient();
    }

    @AfterAll
    void tearDown() throws Exception {
        if (httpServer != null) {
            httpServer.stop();
        }
    }

    @Test
    void getConfigReturnsOk() throws Exception {
        HttpResponse<String> response = get("/v1/config");

        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode body = mapper.readTree(response.body());
        assertThat(body.has("defaults")).isTrue();
    }

    @Test
    void listNamespacesReturnsSchemas() throws Exception {
        HttpResponse<String> response = get("/v1/namespaces");

        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode body = mapper.readTree(response.body());
        assertThat(body.has("namespaces")).isTrue();
        assertThat(body.get("namespaces").isArray()).isTrue();
    }

    @Test
    void createNamespaceReturns200() throws Exception {
        String requestBody = mapper.writeValueAsString(Map.of(
                "namespace", List.of("new_schema"),
                "properties", Map.of()));

        HttpResponse<String> response = post("/v1/namespaces", requestBody);

        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode body = mapper.readTree(response.body());
        assertThat(body.has("namespace")).isTrue();
    }

    @Test
    void getNamespaceReturnsDetails() throws Exception {
        HttpResponse<String> response = get("/v1/namespaces/test_catalog");

        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode body = mapper.readTree(response.body());
        assertThat(body.has("namespace")).isTrue();
    }

    @Test
    void namespaceExistsReturns204() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/v1/namespaces/test_catalog"))
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(204);
    }

    @Test
    void nonExistentNamespaceReturns404() throws Exception {
        HttpResponse<String> response = get("/v1/namespaces/no_such_catalog");

        assertThat(response.statusCode()).isEqualTo(404);
    }

    @Test
    void listTablesReturnsArray() throws Exception {
        HttpResponse<String> response = get("/v1/namespaces/test_catalog/tables");

        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode body = mapper.readTree(response.body());
        assertThat(body.has("identifiers")).isTrue();
        assertThat(body.get("identifiers").isArray()).isTrue();
    }

    // ── HTTP helpers ──

    private HttpResponse<String> get(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .GET()
                .header("Accept", "application/json")
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(String path, String jsonBody) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    // ── Stub MetadataService ──

    private static final class StubMetadataService implements CatalogLifecyclePort, TableDdlPort, TableResolutionPort, QueryMetadataLifecyclePort, ColumnTagPort {
    @Override
    public void setTag(MetadataSessionContext session, QualifiedTableRef tableRef,
            String columnName, String tagKey, String value) {}

    @Override
    public void unsetTag(MetadataSessionContext session, QualifiedTableRef tableRef,
            String columnName, String tagKey) {}

    @Override
    public java.util.Map<String, String> listTags(
            MetadataSessionContext session, QualifiedTableRef tableRef, String columnName) {
        return java.util.Map.of();
    }

    @Override
    public int clearTableTags(MetadataSessionContext session, java.util.UUID tableId) {
        return 0;
    }

    @Override
    public int renameTableTags(MetadataSessionContext session,
            QualifiedTableRef from, QualifiedTableRef to) {
        return 0;
    }

    @Override
    public Optional<CatalogDescriptor> getCatalogDescriptor(String catalogName) {
        return Optional.empty();
    }

    @Override
    public Optional<CatalogDescriptor> findCatalogDescriptorRevision(
            String catalogName, String incarnationId, long revision) {
        return Optional.empty();
    }


        @Override
        public List<String> listCatalogs(MetadataSessionContext session) {
            return List.of("test_catalog");
        }

        @Override
        public List<String> listSchemas(MetadataSessionContext session, String catalogName) {
            return List.of("default", "information_schema");
        }

        @Override
        public void createCatalog(MetadataSessionContext session, CatalogDescriptor descriptor) {}

        @Override
        public void dropCatalog(MetadataSessionContext session, String catalogName, boolean cascade) {}

        @Override
        public ResolvedTable resolveTable(MetadataSessionContext session, QualifiedTableRef tableRef) {
            return null;
        }

        @Override
        public void createTable(MetadataSessionContext session, QualifiedTableRef tableRef, TableMetadata metadata) {}

        @Override
        public void dropTable(MetadataSessionContext session, QualifiedTableRef tableRef) {}

        @Override
        public void alterTable(MetadataSessionContext session, QualifiedTableRef tableRef, List<SchemaChange> changes) {}

        @Override
        public void beginQuery(org.pangrova.metadata.api.session.MetadataSessionContext context) {}

        @Override
        public void cleanupQuery(org.pangrova.spi.connector.ConnectorQueryAuthority authority) {}
    }
}
