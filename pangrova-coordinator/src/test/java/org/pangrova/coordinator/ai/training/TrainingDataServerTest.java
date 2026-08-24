package org.pangrova.coordinator.ai.training;

import org.pangrova.observability.noop.NoOpMetricsProvider;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.UserIdentity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link TrainingDataServer}.
 */
class TrainingDataServerTest {

    private TrainingDataServer server;
    private final UserIdentity testUser = new UserIdentity(
            "test-user", Set.of("ml-team"), Map.of(), Instant.now());

    @BeforeEach
    void setUp() {
        TrainingDataServerConfig config = new TrainingDataServerConfig(
                true, 9090, 3, "jwt", 65536);
        server = new TrainingDataServer(config, new AllowAllAccessControl(), new NoOpMetricsProvider());
    }

    @AfterEach
    void tearDown() {
        server.close();
    }

    @Test
    void start_setsStartedFlag() {
        assertThat(server.isStarted()).isFalse();
        server.start();
        assertThat(server.isStarted()).isTrue();
    }

    @Test
    void start_throwsIfAlreadyStarted() {
        server.start();
        assertThatThrownBy(() -> server.start())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already started");
    }

    @Test
    void start_throwsIfClosed() {
        server.close();
        assertThatThrownBy(() -> server.start())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void close_isIdempotent() {
        server.start();
        server.close();
        server.close(); // second close should not throw
        assertThat(server.isStarted()).isFalse();
    }

    @Test
    void port_returnsConfiguredPort() {
        assertThat(server.port()).isEqualTo(9090);
    }

    @Test
    void getFlightInfo_parsesValidCommand() {
        server.start();
        String json = """
                {"table": "catalog.schema.knowledge_base", "version": "tag:training_v3",
                 "split": "hash(id) % 10 < 8", "columns": ["id", "content", "embedding"]}
                """;
        byte[] command = json.getBytes(StandardCharsets.UTF_8);

        List<TrainingDataServer.EndpointTicket> endpoints = server.getFlightInfo(command, testUser);

        assertThat(endpoints).hasSize(1);
        assertThat(endpoints.get(0).port()).isEqualTo(9090);
        assertThat(endpoints.get(0).ticket()).contains("knowledge_base");
    }

    @Test
    void getFlightInfo_rejectsInvalidJson() {
        server.start();
        byte[] command = "not json".getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> server.getFlightInfo(command, testUser))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid Flight descriptor JSON");
    }

    @Test
    void getFlightInfo_rejectsMissingTable() {
        server.start();
        String json = """
                {"version": "tag:v1"}
                """;
        byte[] command = json.getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> server.getFlightInfo(command, testUser))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("table");
    }

    @Test
    void getFlightInfo_rejectsInvalidSplitExpression() {
        server.start();
        String json = """
                {"table": "catalog.schema.tbl", "split": "SELECT * FROM evil"}
                """;
        byte[] command = json.getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> server.getFlightInfo(command, testUser))
                .isInstanceOf(InvalidSplitExpressionException.class);
    }

    @Test
    void getFlightInfo_rejectsNonQualifiedTable() {
        server.start();
        String json = """
                {"table": "just_a_table"}
                """;
        byte[] command = json.getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> server.getFlightInfo(command, testUser))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fully qualified");
    }

    @Test
    void getFlightInfo_acceptsMinimalCommand() {
        server.start();
        String json = """
                {"table": "cat.schema.tbl"}
                """;
        byte[] command = json.getBytes(StandardCharsets.UTF_8);

        List<TrainingDataServer.EndpointTicket> endpoints = server.getFlightInfo(command, testUser);
        assertThat(endpoints).isNotEmpty();
    }

    @Test
    void openStream_acquiresPermitAndTracksStream() {
        server.start();
        byte[] ticket = "{}".getBytes(StandardCharsets.UTF_8);

        TrainingDataServer.StreamContext ctx = server.openStream(ticket, testUser);

        assertThat(ctx.streamId()).isNotBlank();
        assertThat(ctx.identity()).isEqualTo(testUser);
        assertThat(server.activeStreamCount()).isEqualTo(1);
    }

    @Test
    void openStream_rejectsWhenMaxStreamsReached() {
        server.start();
        byte[] ticket = "{}".getBytes(StandardCharsets.UTF_8);

        // Open 3 streams (max configured)
        server.openStream(ticket, testUser);
        server.openStream(ticket, testUser);
        server.openStream(ticket, testUser);

        assertThatThrownBy(() -> server.openStream(ticket, testUser))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Maximum concurrent streams");
    }

    @Test
    void closeStream_releasesPermitAndUpdatesCount() {
        server.start();
        byte[] ticket = "{}".getBytes(StandardCharsets.UTF_8);

        TrainingDataServer.StreamContext ctx = server.openStream(ticket, testUser);
        assertThat(server.activeStreamCount()).isEqualTo(1);

        server.closeStream(ctx.streamId(), 1024);
        assertThat(server.activeStreamCount()).isEqualTo(0);
    }

    @Test
    void closeStream_allowsNewStreamAfterRelease() {
        server.start();
        byte[] ticket = "{}".getBytes(StandardCharsets.UTF_8);

        // Fill all 3 slots
        TrainingDataServer.StreamContext ctx1 = server.openStream(ticket, testUser);
        server.openStream(ticket, testUser);
        server.openStream(ticket, testUser);

        // Close one
        server.closeStream(ctx1.streamId(), 512);

        // Should be able to open another
        TrainingDataServer.StreamContext ctx4 = server.openStream(ticket, testUser);
        assertThat(ctx4.streamId()).isNotBlank();
    }

    @Test
    void closeStream_ignoreDuplicateClose() {
        server.start();
        byte[] ticket = "{}".getBytes(StandardCharsets.UTF_8);

        TrainingDataServer.StreamContext ctx = server.openStream(ticket, testUser);
        server.closeStream(ctx.streamId(), 100);
        server.closeStream(ctx.streamId(), 200); // should not throw or double-release

        assertThat(server.activeStreamCount()).isEqualTo(0);
    }

    @Test
    void getFlightInfo_enforcesRbac() {
        TrainingDataServerConfig config = new TrainingDataServerConfig(
                true, 9091, 5, "jwt", 65536);
        TrainingDataServer restrictedServer = new TrainingDataServer(
                config, new DenyAllAccessControl(), new NoOpMetricsProvider());
        restrictedServer.start();

        String json = """
                {"table": "catalog.schema.secret_table"}
                """;
        byte[] command = json.getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> restrictedServer.getFlightInfo(command, testUser))
                .isInstanceOf(SecurityException.class);

        restrictedServer.close();
    }

    /**
     * AccessControl implementation that permits all operations for functional testing.
     */
    private static final class AllowAllAccessControl implements AccessControl {
        @Override
        public void checkCanSelectFromTable(UserIdentity user, String catalog, String schema, String table) {}
        @Override
        public void checkCanInsertIntoTable(UserIdentity user, String catalog, String schema, String table) {}
        @Override
        public void checkCanDeleteFromTable(UserIdentity user, String catalog, String schema, String table) {}
        @Override
        public void checkCanCreateTable(UserIdentity user, String catalog, String schema, String table) {}
        @Override
        public void checkCanDropTable(UserIdentity user, String catalog, String schema, String table) {}
        @Override
        public void checkCanAlterTable(UserIdentity user, String catalog, String schema, String table) {}
        @Override
        public void checkCanRenameTable(UserIdentity user, String catalog, String schema, String oldTable, String newCatalog, String newSchema, String newTable) {}
        @Override
        public void checkCanShowColumns(UserIdentity user, String catalog, String schema, String table) {}
        @Override
        public void checkCanSelectFromColumns(UserIdentity user, String catalog, String schema, String table, Set<String> columns) {}
        @Override
        public void checkCanAccessCatalog(UserIdentity user, String catalog) {}
        @Override
        public void checkCanAccessSchema(UserIdentity user, String catalog, String schema) {}
        @Override
        public void checkCanCreateSchema(UserIdentity user, String catalog, String schema) {}
        @Override
        public void checkCanDropSchema(UserIdentity user, String catalog, String schema) {}
        @Override
        public void checkCanAlterSchema(UserIdentity user, String catalog, String schema) {}
        @Override
        public void checkCanShowSchemas(UserIdentity user, String catalog) {}
        @Override
        public void checkCanShowTables(UserIdentity user, String catalog, String schema) {}
        @Override
        public Optional<String> getRowFilter(UserIdentity user, String catalog, String schema, String table) { return Optional.empty(); }
        @Override
        public Optional<String> getColumnMask(UserIdentity user, String catalog, String schema, String table, String column) { return Optional.empty(); }
    }

    /**
     * AccessControl implementation that denies all operations for RBAC enforcement testing.
     */
    private static final class DenyAllAccessControl implements AccessControl {
        @Override
        public void checkCanSelectFromTable(UserIdentity user, String catalog, String schema, String table) {
            throw new SecurityException("Access denied to " + catalog + "." + schema + "." + table);
        }
        @Override
        public void checkCanInsertIntoTable(UserIdentity user, String catalog, String schema, String table) {
            throw new SecurityException("Access denied");
        }
        @Override
        public void checkCanDeleteFromTable(UserIdentity user, String catalog, String schema, String table) {
            throw new SecurityException("Access denied");
        }
        @Override
        public void checkCanCreateTable(UserIdentity user, String catalog, String schema, String table) {
            throw new SecurityException("Access denied");
        }
        @Override
        public void checkCanDropTable(UserIdentity user, String catalog, String schema, String table) {
            throw new SecurityException("Access denied");
        }
        @Override
        public void checkCanAlterTable(UserIdentity user, String catalog, String schema, String table) {
            throw new SecurityException("Access denied");
        }
        @Override
        public void checkCanRenameTable(UserIdentity user, String catalog, String schema, String oldTable, String newCatalog, String newSchema, String newTable) {
            throw new SecurityException("Access denied");
        }
        @Override
        public void checkCanShowColumns(UserIdentity user, String catalog, String schema, String table) {
            throw new SecurityException("Access denied");
        }
        @Override
        public void checkCanSelectFromColumns(UserIdentity user, String catalog, String schema, String table, Set<String> columns) {
            throw new SecurityException("Access denied");
        }
        @Override
        public void checkCanAccessCatalog(UserIdentity user, String catalog) {
            throw new SecurityException("Access denied");
        }
        @Override
        public void checkCanAccessSchema(UserIdentity user, String catalog, String schema) {
            throw new SecurityException("Access denied");
        }
        @Override
        public void checkCanCreateSchema(UserIdentity user, String catalog, String schema) {
            throw new SecurityException("Access denied");
        }
        @Override
        public void checkCanDropSchema(UserIdentity user, String catalog, String schema) {
            throw new SecurityException("Access denied");
        }
        @Override
        public void checkCanAlterSchema(UserIdentity user, String catalog, String schema) {
            throw new SecurityException("Access denied");
        }
        @Override
        public void checkCanShowSchemas(UserIdentity user, String catalog) {
            throw new SecurityException("Access denied");
        }
        @Override
        public void checkCanShowTables(UserIdentity user, String catalog, String schema) {
            throw new SecurityException("Access denied");
        }
        @Override
        public Optional<String> getRowFilter(UserIdentity user, String catalog, String schema, String table) { return Optional.empty(); }
        @Override
        public Optional<String> getColumnMask(UserIdentity user, String catalog, String schema, String table, String column) { return Optional.empty(); }
    }
}
