package org.pangrova.testing.integration.e2e.write;

import net.jqwik.api.lifecycle.BeforeProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Real-deployment lifecycle components; only explicitly named SQL tests prove JNI execution. */
@Tag("integration")
@Tag("native-required")
public abstract class NativeWriteLifecycleTestBase extends SqlE2ETestBase {
    protected static final String CATALOG = "native_write_lifecycle";
    protected static final String SCHEMA = "default";
    private TaskRequest authenticatedRequest;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA,
                icebergWarehouse().resolve("native-write-lifecycle").toString()));
    }

    @BeforeEach
    @BeforeProperty
    protected void captureActualAuthenticatedTask() throws Exception {
        registerTestConnectors(connectorConfigs());
        var response = submitQueryViaHttp("SELECT CAST(1 AS BIGINT) AS actor_probe");
        await().atMost(Duration.ofSeconds(30)).until(() -> taskTracker.getTaskIds(response.queryId()).stream()
                .anyMatch(id -> taskTracker.getTaskRequest(id) != null));
        authenticatedRequest = taskTracker.getTaskIds(response.queryId()).stream()
                .map(taskTracker::getTaskRequest).filter(java.util.Objects::nonNull)
                .findFirst().orElseThrow().taskRequest();
        var session = authenticatedCatalogSession(CATALOG);
        assertThat(authenticatedRequest.getSecurity().getPrincipal()).isEqualTo(session.getUser());
        assertThat(authenticatedRequest.getSecurity().getTenantId())
                .isEqualTo(session.getProperties().get("tenant_id"));
        assertThat(consumeSubmittedQuery(response)).singleElement()
                .satisfies(row -> assertThat(row.getLong("actor_probe")).isEqualTo(1));
    }

    protected final NativeWriteLifecycleFixture lifecycle() {
        return lifecycle("http://localhost:" + coordinatorHttpServer.getPort());
    }

    protected final NativeWriteLifecycleFixture lifecycle(String deploymentUrl) {
        return new NativeWriteLifecycleFixture(metadataBootstrap.catalogRegistry().getCatalog(CATALOG),
                authenticatedCatalogSession(CATALOG), authenticatedRequest,
                deploymentUrl, E2E_INTERNAL_AUTH_SECRET);
    }

    protected final TaskRequest authenticatedRequest() {
        return java.util.Objects.requireNonNull(authenticatedRequest);
    }

    protected final String newTable() {
        String name = "lifecycle_" + UUID.randomUUID().toString().replace("-", "");
        sqlRunner.executeDdl("CREATE TABLE " + qualified(name) + " (id BIGINT)"
                + NativeWriteLifecycleFixture.naturalFlushTableOptions());
        return name;
    }

    protected static String qualified(String table) {
        return CATALOG + "." + SCHEMA + "." + table;
    }
}
