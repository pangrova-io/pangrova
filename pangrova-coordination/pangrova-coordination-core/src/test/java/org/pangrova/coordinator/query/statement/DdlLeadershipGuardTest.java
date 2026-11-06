package org.pangrova.coordinator.query.statement;

import org.pangrova.query.core.statement.StatementClassifier;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.persistence.coordination.memory.membership.InMemoryCoordinatorRegistry;
import org.pangrova.persistence.coordination.memory.lease.InMemoryDistributedLease;
import org.pangrova.coordinator.query.execution.dispatch.DdlLeadershipGuard;
import org.pangrova.coordinator.query.execution.dispatch.DdlNotOnLeaderException;
import org.pangrova.parser.api.ast.CreateCatalogStatement;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.InsertStatement;
import org.pangrova.parser.api.ast.ShowSchemasStatement;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.CreateSchemaStatement;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;
import org.pangrova.parser.api.ast.statement.DropCatalogStatement;
import org.pangrova.parser.api.ast.statement.DropTableStatement;
import org.pangrova.parser.api.ast.type.DataTypeSpec;
import org.pangrova.coordination.api.membership.CoordinatorSnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * F-CHAIN-HA-06: Verifies DDL leadership guard behavior:
 * - When enabled (guard is non-null): rejects DDL on non-leader, allows on leader
 * - When disabled (guard is null / default): DDL executes on any coordinator
 *
 * <p>The guard is disabled by default ({@code coordinator.ha.ddl-leader-routing=false}).
 * It is only active when explicitly enabled in configuration.</p>
 */
class DdlLeadershipGuardTest {

    private static final String LEADER_ID = "coord-1";
    private static final String STANDBY_ID = "coord-2";
    private static final String LEADER_HOST = "leader-host";
    private static final int LEADER_PORT = 8080;

    private InMemoryDistributedLease lease;
    private InMemoryCoordinatorRegistry registry;
    private HighAvailabilityCoordinationService leaderService;
    private HighAvailabilityCoordinationService standbyService;

    @BeforeEach
    void setUp() {
        lease = new InMemoryDistributedLease();
        registry = new InMemoryCoordinatorRegistry();

        // Set up leader coordinator
        leaderService = new HighAvailabilityCoordinationService(lease, new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());
        leaderService.setCoordinatorRegistry(registry, LEADER_HOST, LEADER_PORT);
        leaderService.tryBecomeLeader(LEADER_ID);

        // Register leader in the coordinator registry with LEADER role
        registry.register(new CoordinatorSnapshot(
                LEADER_ID, LEADER_HOST, LEADER_PORT,
                "LEADER", 1, System.currentTimeMillis(), System.currentTimeMillis()));

        // Set up standby coordinator (shares the same lease backend)
        standbyService = new HighAvailabilityCoordinationService(lease, new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());
        standbyService.setCoordinatorRegistry(registry, "standby-host", 8081);
    }

    // --- DDL rejection on non-leader ---

    @Test
    void createTableRejectedOnNonLeader() {
        DdlLeadershipGuard guard = new DdlLeadershipGuard(standbyService, STANDBY_ID);
        Statement stmt = createTableStatement();

        assertThatThrownBy(() -> guard.requireLeaderForDdl(stmt))
                .isInstanceOf(DdlNotOnLeaderException.class)
                .satisfies(ex -> {
                    DdlNotOnLeaderException ddlEx = (DdlNotOnLeaderException) ex;
                    assertThat(ddlEx.getLeaderEndpoint())
                            .isEqualTo("http://" + LEADER_HOST + ":" + LEADER_PORT);
                });
    }

    @Test
    void dropTableRejectedOnNonLeader() {
        DdlLeadershipGuard guard = new DdlLeadershipGuard(standbyService, STANDBY_ID);
        Statement stmt = new DropTableStatement(
                new Identifier(List.of("db", "orders")), false);

        assertThatThrownBy(() -> guard.requireLeaderForDdl(stmt))
                .isInstanceOf(DdlNotOnLeaderException.class);
    }

    @Test
    void createSchemaRejectedOnNonLeader() {
        DdlLeadershipGuard guard = new DdlLeadershipGuard(standbyService, STANDBY_ID);
        Statement stmt = new CreateSchemaStatement(
                new Identifier(List.of("new_schema")), false);

        assertThatThrownBy(() -> guard.requireLeaderForDdl(stmt))
                .isInstanceOf(DdlNotOnLeaderException.class);
    }

    @Test
    void createCatalogRejectedOnNonLeader() {
        DdlLeadershipGuard guard = new DdlLeadershipGuard(standbyService, STANDBY_ID);
        Statement stmt = new CreateCatalogStatement(
                "test_catalog", Optional.of("iceberg"), Map.of());

        assertThatThrownBy(() -> guard.requireLeaderForDdl(stmt))
                .isInstanceOf(DdlNotOnLeaderException.class);
    }

    @Test
    void dropCatalogRejectedOnNonLeader() {
        DdlLeadershipGuard guard = new DdlLeadershipGuard(standbyService, STANDBY_ID);
        Statement stmt = new DropCatalogStatement("old_catalog");

        assertThatThrownBy(() -> guard.requireLeaderForDdl(stmt))
                .isInstanceOf(DdlNotOnLeaderException.class);
    }

    @Test
    void storageProfileMutationsCannotRunOnStandbyWhenLeadershipRoutingIsEnabled() {
        var guard = new DdlLeadershipGuard(standbyService, STANDBY_ID);
        var parser = new org.pangrova.parser.core.parse.AntlrSqlParser();
        for (String sql : List.of("CREATE STORAGE PROFILE staging WITH ('type'='S3', 'bucket'='example')",
                "DROP STORAGE PROFILE IF EXISTS staging")) {
            assertThatThrownBy(() -> guard.requireLeaderForDdl(parser.parse(sql)))
                    .isInstanceOf(DdlNotOnLeaderException.class);
        }
    }

    // --- DDL allowed on leader ---

    @Test
    void createTableAllowedOnLeader() {
        DdlLeadershipGuard guard = new DdlLeadershipGuard(leaderService, LEADER_ID);
        Statement stmt = createTableStatement();

        assertThatCode(() -> guard.requireLeaderForDdl(stmt))
                .doesNotThrowAnyException();
    }

    @Test
    void dropTableAllowedOnLeader() {
        DdlLeadershipGuard guard = new DdlLeadershipGuard(leaderService, LEADER_ID);
        Statement stmt = new DropTableStatement(
                new Identifier(List.of("db", "orders")), false);

        assertThatCode(() -> guard.requireLeaderForDdl(stmt))
                .doesNotThrowAnyException();
    }

    // --- Non-DDL statements pass on any node ---

    @Test
    void showSchemasAllowedOnNonLeader() {
        DdlLeadershipGuard guard = new DdlLeadershipGuard(standbyService, STANDBY_ID);
        Statement stmt = new ShowSchemasStatement(null);

        assertThatCode(() -> guard.requireLeaderForDdl(stmt))
                .doesNotThrowAnyException();
    }

    @Test
    void insertAllowedOnNonLeader() {
        DdlLeadershipGuard guard = new DdlLeadershipGuard(standbyService, STANDBY_ID);
        Statement stmt = new InsertStatement(
                new Identifier(List.of("db", "orders")),
                Optional.of(List.of("id", "name")),
                List.of(List.of()));

        assertThatCode(() -> guard.requireLeaderForDdl(stmt))
                .doesNotThrowAnyException();
    }

    // --- Leader endpoint resolution ---

    @Test
    void exceptionContainsLeaderEndpointUrl() {
        DdlLeadershipGuard guard = new DdlLeadershipGuard(standbyService, STANDBY_ID);
        Statement stmt = createTableStatement();

        try {
            guard.requireLeaderForDdl(stmt);
        } catch (DdlNotOnLeaderException ex) {
            assertThat(ex.getLeaderEndpoint()).isEqualTo("http://leader-host:8080");
            assertThat(ex.getMessage()).contains("leader coordinator");
            return;
        }
        throw new AssertionError("Expected DdlNotOnLeaderException");
    }

    @Test
    void exceptionHasNullEndpointWhenNoLeaderRegistered() {
        // Create a standby service with no registry configured
        var isolatedLease = new InMemoryDistributedLease();
        var isolatedService = new HighAvailabilityCoordinationService(isolatedLease, new org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore());
        // No coordinator registry set — leader endpoint resolution returns null
        DdlLeadershipGuard guard = new DdlLeadershipGuard(isolatedService, "orphan-node");
        Statement stmt = createTableStatement();

        assertThatThrownBy(() -> guard.requireLeaderForDdl(stmt))
                .isInstanceOf(DdlNotOnLeaderException.class)
                .satisfies(ex -> {
                    DdlNotOnLeaderException ddlEx = (DdlNotOnLeaderException) ex;
                    assertThat(ddlEx.getLeaderEndpoint()).isNull();
                });
    }

    // --- StatementClassifier.requiresLeaderRouting coverage ---

    @Test
    void requiresLeaderRoutingCoversCoreDdlTypes() {
        assertThat(StatementClassifier.requiresLeaderRouting(createTableStatement())).isTrue();
        assertThat(StatementClassifier.requiresLeaderRouting(
                new DropTableStatement(new Identifier(List.of("t")), false))).isTrue();
        assertThat(StatementClassifier.requiresLeaderRouting(
                new CreateSchemaStatement(new Identifier(List.of("s")), false))).isTrue();
        assertThat(StatementClassifier.requiresLeaderRouting(
                new CreateCatalogStatement("c", Optional.of("iceberg"), Map.of()))).isTrue();
        assertThat(StatementClassifier.requiresLeaderRouting(
                new DropCatalogStatement("c"))).isTrue();
    }

    @Test
    void requiresLeaderRoutingReturnsFalseForReadOnly() {
        assertThat(StatementClassifier.requiresLeaderRouting(
                new ShowSchemasStatement(null))).isFalse();
    }

    @Test
    void requiresLeaderRoutingReturnsFalseForDml() {
        Statement insert = new InsertStatement(
                new Identifier(List.of("db", "t")),
                Optional.of(List.of("id")), List.of(List.of()));
        assertThat(StatementClassifier.requiresLeaderRouting(insert)).isFalse();
    }

    @Test
    void viewAndEmbeddingModelMutationsRequireLeaderRouting() {
        var parser = new org.pangrova.parser.core.parse.AntlrSqlParser();
        assertThat(StatementClassifier.requiresLeaderRouting(parser.parse("CREATE VIEW v AS SELECT 1"))).isTrue();
        assertThat(StatementClassifier.requiresLeaderRouting(parser.parse("DROP VIEW v"))).isTrue();
        assertThat(StatementClassifier.requiresLeaderRouting(
                new org.pangrova.parser.api.ast.statement.CreateEmbeddingModelStatement("m", Map.of())))
                .isTrue();
        assertThat(StatementClassifier.isShow(parser.parse("SHOW VIEWS"))).isTrue();
        assertThat(StatementClassifier.isShow(parser.parse("SHOW STORAGE PROFILES"))).isTrue();
    }

    // --- Helper methods ---

    private static CreateTableStatement createTableStatement() {
        return new CreateTableStatement(
                new Identifier(List.of("db", "orders")),
                List.of(new CreateTableStatement.ColumnDefinition(
                        "id", org.pangrova.parser.core.build.type.RawSqlTypeParser.parse("BIGINT"))),
                false, List.of(), List.of(), Optional.empty(), Map.of());
    }
}
