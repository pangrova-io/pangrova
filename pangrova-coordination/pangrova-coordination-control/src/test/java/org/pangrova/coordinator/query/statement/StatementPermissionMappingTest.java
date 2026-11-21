package org.pangrova.coordinator.query.statement;
import org.pangrova.query.dispatch.StatementResult;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.execution.dispatch.*;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.parser.api.ast.CreateMaterializedViewStatement;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.InsertStatement;
import org.pangrova.parser.api.ast.Literal;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.parse.AntlrSqlParser;
import org.pangrova.parser.api.ast.statement.AlterTableStatement;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;
import org.pangrova.parser.api.ast.statement.DropMaterializedViewStatement;
import org.pangrova.parser.api.ast.statement.DropTableStatement;
import org.pangrova.parser.api.ast.statement.RefreshMaterializedViewStatement;
import org.pangrova.parser.api.ast.type.DataTypeSpec;
import org.pangrova.parser.api.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.api.ast.statement.dml.UpdateStatement;
import org.pangrova.security.api.exception.AuthorizationDeniedException;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.policy.authorization.DynamicRbacAccessControl;
import org.pangrova.testing.testkit.security.policy.StaticGrantStore;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.testing.testkit.security.mask.EmptyColumnMaskStore;
import org.pangrova.testing.testkit.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.api.authorization.AccessControl;
import org.pangrova.security.api.identity.UserIdentity;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;

import static org.pangrova.coordinator.testinfra.CoordinatorRoleFixtures.mappedRoles;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that StatementDispatcher (via PermissionChecker) maps each statement type
 * to the correct AccessControl method.
 *
 * <p>Strategy: grant only one privilege at a time, then assert that the
 * matching statement passes and non-matching statements are rejected.</p>
 */
class StatementPermissionMappingTest {

    private static final Identifier TABLE = Identifier.of("cat", "sch", "tbl");
    private static final UserIdentity USER = new UserIdentity(
            "test-user", Set.of("role1"), Map.of(), Instant.now());

    // ── DELETE → checkCanDeleteFromTable ──

    @Test
    void delete_requiresDeletePrivilege() {
        StatementDispatcher dispatcher = dispatcherWithGrants(grant(Privilege.DELETE));
        // DELETE privilege granted → no AuthorizationDeniedException
        assertThatCode(() -> executeDirect(dispatcher, deleteStmt()))
                .doesNotThrowAnyException();
    }

    @Test
    void delete_rejectedWithoutDeletePrivilege() {
        StatementDispatcher dispatcher = dispatcherWithGrants(grant(Privilege.INSERT));
        assertThatThrownBy(() -> executeDirect(dispatcher, deleteStmt()))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    // ── INSERT → checkCanInsertIntoTable ──

    @Test
    void insert_rejectedWithoutInsertPrivilege() {
        StatementDispatcher dispatcher = dispatcherWithGrants(grant(Privilege.DELETE));
        assertThatThrownBy(() -> executeDirect(dispatcher, insertStmt()))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    // ── UPDATE → checkCanInsertIntoTable + checkCanDeleteFromTable ──

    @Test
    void update_rejectedWithOnlyInsertPrivilege() {
        StatementDispatcher dispatcher = dispatcherWithGrants(grant(Privilege.INSERT));
        assertThatThrownBy(() -> executeDirect(dispatcher, updateStmt()))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    @Test
    void update_rejectedWithOnlyDeletePrivilege() {
        StatementDispatcher dispatcher = dispatcherWithGrants(grant(Privilege.DELETE));
        assertThatThrownBy(() -> executeDirect(dispatcher, updateStmt()))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    // ── DROP TABLE → checkCanDropTable ──

    @Test
    void dropTable_rejectedWithoutDropPrivilege() {
        StatementDispatcher dispatcher = dispatcherWithGrants(grant(Privilege.ALTER));
        assertThatThrownBy(() -> executeDirect(dispatcher, dropTableStmt()))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    // ── CREATE TABLE → checkCanAlterTable ──

    @Test
    void createTable_rejectedWithoutAlterPrivilege() {
        StatementDispatcher dispatcher = dispatcherWithGrants(grant(Privilege.INSERT));
        assertThatThrownBy(() -> executeDirect(dispatcher, createTableStmt()))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    // ── ALTER TABLE → checkCanAlterTable ──

    @Test
    void alterTable_rejectedWithoutAlterPrivilege() {
        StatementDispatcher dispatcher = dispatcherWithGrants(grant(Privilege.INSERT));
        assertThatThrownBy(() -> executeDirect(dispatcher, alterTableStmt()))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    // ── CREATE MATERIALIZED VIEW → checkCanCreateTable ──

    @Test
    void createMaterializedView_requiresCreateTablePrivilege() {
        StatementDispatcher allowed = dispatcherWithGrants(grant(Privilege.CREATE_TABLE));
        assertThatCode(() -> executeDirect(allowed, createMvStmt()))
                .doesNotThrowAnyException();

        StatementDispatcher denied = dispatcherWithGrants(grant(Privilege.ALTER));
        assertThatThrownBy(() -> executeDirect(denied, createMvStmt()))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    // ── REFRESH MATERIALIZED VIEW → checkCanAlterTable ──

    @Test
    void refreshMaterializedView_requiresAlterPrivilege() {
        StatementDispatcher allowed = dispatcherWithGrants(grant(Privilege.ALTER));
        assertThatCode(() -> executeDirect(allowed, refreshMvStmt()))
                .doesNotThrowAnyException();

        StatementDispatcher denied = dispatcherWithGrants(grant(Privilege.SELECT));
        assertThatThrownBy(() -> executeDirect(denied, refreshMvStmt()))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    // ── DROP MATERIALIZED VIEW → checkCanDropTable ──

    @Test
    void dropMaterializedView_requiresDropPrivilege() {
        StatementDispatcher allowed = dispatcherWithGrants(grant(Privilege.DROP));
        assertThatCode(() -> executeDirect(allowed, dropMvStmt()))
                .doesNotThrowAnyException();

        StatementDispatcher denied = dispatcherWithGrants(grant(Privilege.ALTER));
        assertThatThrownBy(() -> executeDirect(denied, dropMvStmt()))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    // ── No access control → no rejection ──

    @Test
    void noAccessControl_neverRejects() {
        StatementDispatcher dispatcher = buildDispatcher(null);
        // No AccessControl — permission check skipped, factory executes normally
        assertThatCode(() -> executeDirect(dispatcher, deleteStmt()))
                .doesNotThrowAnyException();
    }

    // ── Helpers ──

    private void executeDirect(StatementDispatcher dispatcher, Statement stmt) {
        var session = new QuerySession("q-perm", Map.of(), USER, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
        dispatcher.executeDirect(stmt, null, "q-perm", session);
    }

    private static StatementDispatcher dispatcherWithGrants(PrivilegeGrant... grants) {
        DynamicRbacAccessControl ac = new DynamicRbacAccessControl(
                new StaticGrantStore(List.of(grants)),
                mappedRoles(java.util.Map.ofEntries(java.util.Map.entry("role1", "role1"))),
                new RowLevelSecurityFilter(new EmptyRowPolicyStore()),
                new ColumnMaskingProcessor(new EmptyColumnMaskStore()));
        return buildDispatcher(ac);
    }

    private static StatementDispatcher buildDispatcher(AccessControl accessControl) {
        MetadataTask<Statement> noOp = (stmt, ctx) -> new StatementResult(
                List.of(), List.of(), null, StatementCategory.DDL);
        var tasks = new HashMap<Class<? extends Statement>, MetadataTask<?>>();
        tasks.put(DeleteStatement.class, noOp);
        tasks.put(InsertStatement.class, noOp);
        tasks.put(UpdateStatement.class, noOp);
        tasks.put(DropTableStatement.class, noOp);
        tasks.put(CreateTableStatement.class, noOp);
        tasks.put(AlterTableStatement.DropColumn.class, noOp);
        tasks.put(CreateMaterializedViewStatement.class, noOp);
        tasks.put(RefreshMaterializedViewStatement.class, noOp);
        tasks.put(DropMaterializedViewStatement.class, noOp);
        var factory = new MetadataExecutionFactory(tasks);
        var context = new StatementDispatcherContext(
                null, null, List.of(factory), List.of(), accessControl);
        return new StatementDispatcher(context);
    }

    private static PrivilegeGrant grant(Privilege privilege) {
        return PrivilegeGrant.of("role1", privilege, "TABLE", "cat", "sch", "tbl", null);
    }

    private static DeleteStatement deleteStmt() {
        return new DeleteStatement(TABLE, Optional.empty());
    }

    private static InsertStatement insertStmt() {
        return new InsertStatement(TABLE, Optional.of(List.of("id")),
                List.of(List.of(Literal.ofInteger(1))));
    }

    private static UpdateStatement updateStmt() {
        return new UpdateStatement(TABLE,
                List.of(new UpdateStatement.Assignment("id", Literal.ofInteger(2))),
                Optional.empty());
    }

    private static DropTableStatement dropTableStmt() {
        return new DropTableStatement(TABLE, false);
    }

    private static CreateTableStatement createTableStmt() {
        return new CreateTableStatement(TABLE,
                List.of(new CreateTableStatement.ColumnDefinition("id", org.pangrova.parser.build.type.RawSqlTypeParser.parse("INTEGER"))),
                false, List.of(), List.of(), Optional.empty(), Map.of());
    }

    private static AlterTableStatement alterTableStmt() {
        return new AlterTableStatement.DropColumn(TABLE, "col1");
    }

    private static CreateMaterializedViewStatement createMvStmt() {
        return (CreateMaterializedViewStatement) new AntlrSqlParser().parse(
                "CREATE MATERIALIZED VIEW cat.sch.tbl AS SELECT 1");
    }

    private static RefreshMaterializedViewStatement refreshMvStmt() {
        return (RefreshMaterializedViewStatement) new AntlrSqlParser().parse(
                "REFRESH MATERIALIZED VIEW cat.sch.tbl");
    }

    private static DropMaterializedViewStatement dropMvStmt() {
        return (DropMaterializedViewStatement) new AntlrSqlParser().parse(
                "DROP MATERIALIZED VIEW cat.sch.tbl");
    }
}
