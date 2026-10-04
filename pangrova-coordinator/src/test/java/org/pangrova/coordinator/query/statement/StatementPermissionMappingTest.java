package org.pangrova.coordinator.query.statement;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.*;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.InsertStatement;
import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.statement.AlterTableStatement;
import org.pangrova.parser.ast.statement.CreateTableStatement;
import org.pangrova.parser.ast.statement.DropTableStatement;
import org.pangrova.parser.ast.type.DataTypeSpec;
import org.pangrova.parser.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.ast.statement.dml.UpdateStatement;
import org.pangrova.security.authz.AuthorizationDeniedException;
import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.DynamicRbacAccessControl;
import org.pangrova.security.authz.StaticGrantStore;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.mask.EmptyColumnMaskStore;
import org.pangrova.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.UserIdentity;
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
        var session = new CoordinatorSessionContext("q-perm", Map.of(), USER, null);
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
                List.of(new CreateTableStatement.ColumnDefinition("id", DataTypeSpec.fromRawTypeString("INTEGER"))),
                false, List.of(), List.of(), Optional.empty(), Map.of());
    }

    private static AlterTableStatement alterTableStmt() {
        return new AlterTableStatement.DropColumn(TABLE, "col1");
    }
}
