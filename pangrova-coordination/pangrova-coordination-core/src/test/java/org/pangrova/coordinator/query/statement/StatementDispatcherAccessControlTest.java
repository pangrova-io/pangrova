package org.pangrova.coordinator.query.statement;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.*;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.statement.DropTableStatement;
import org.pangrova.security.api.AccessControl;
import org.pangrova.security.api.AccessDeniedException;
import org.pangrova.security.api.UserIdentity;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies StatementDispatcher rejects operations when AccessControl denies permission.
 */
class StatementDispatcherAccessControlTest {

    private static final UserIdentity TEST_USER = new UserIdentity(
            "alice", Set.of("analysts"), Map.of(), Instant.now());

    private static final CoordinatorSessionContext SESSION_WITH_USER =
            new CoordinatorSessionContext("q-ac", Map.of(), TEST_USER, null);

    private static final CoordinatorSessionContext SESSION_NO_USER =
            new CoordinatorSessionContext("q-ac-null", Map.of(), null, null);

    @Test
    void dropTableDeniedByAccessControl() {
        StatementDispatcher dispatcher = buildDispatcher(new DenyAllAccessControl());

        DropTableStatement drop = new DropTableStatement(
                Identifier.of("catalog", "schema", "orders"), false);

        assertThatThrownBy(() -> dispatcher.executeDirect(
                drop, null, "q-drop", SESSION_WITH_USER))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void executeWithoutAccessControlSkipsPermissionCheck() {
        StatementDispatcher dispatcher = buildDispatcher(null);

        DropTableStatement drop = new DropTableStatement(
                Identifier.of("catalog", "schema", "orders"), false);

        // No AccessControl — permission check skipped, factory executes normally
        dispatcher.executeDirect(drop, null, "q-no-ac", SESSION_WITH_USER);
    }

    @Test
    void executeWithNullUserSkipsPermissionCheck() {
        StatementDispatcher dispatcher = buildDispatcher(new DenyAllAccessControl());

        DropTableStatement drop = new DropTableStatement(
                Identifier.of("catalog", "schema", "orders"), false);

        // Null user — permission check skipped, factory executes normally
        dispatcher.executeDirect(drop, null, "q-null-user", SESSION_NO_USER);
    }

    private static StatementDispatcher buildDispatcher(AccessControl accessControl) {
        MetadataTask<Statement> noOp = (stmt, ctx) -> new StatementResult(
                List.of(), List.of(), null, StatementCategory.DDL);
        var tasks = new HashMap<Class<? extends Statement>, MetadataTask<?>>();
        tasks.put(DropTableStatement.class, noOp);
        var factory = new MetadataExecutionFactory(tasks);
        var context = new StatementDispatcherContext(
                null, null, List.of(factory), List.of(), accessControl);
        return new StatementDispatcher(context);
    }

    private static final class DenyAllAccessControl implements AccessControl {
        @Override public void checkCanUpdateTable(UserIdentity u, String c, String s, String t) {
            throw new AccessDeniedException("denied");
        }
        @Override public void checkCanAccessCatalog(UserIdentity u, String c) {
            throw new AccessDeniedException("denied");
        }
        @Override public void checkCanAccessSchema(UserIdentity u, String c, String s) {
            throw new AccessDeniedException("denied");
        }
        @Override public void checkCanReadTableMetadata(UserIdentity u, String c, String s, String t) {
            throw new AccessDeniedException("denied");
        }
        @Override public void checkCanSelectFromTable(UserIdentity u, String c, String s, String t) {
            throw new AccessDeniedException("denied");
        }
        @Override public void checkCanInsertIntoTable(UserIdentity u, String c, String s, String t) {
            throw new AccessDeniedException("denied");
        }
        @Override public void checkCanDeleteFromTable(UserIdentity u, String c, String s, String t) {
            throw new AccessDeniedException("denied");
        }
        @Override public void checkCanCreateTable(UserIdentity u, String c, String s, String t) {
            throw new AccessDeniedException("denied");
        }
        @Override public void checkCanDropTable(UserIdentity u, String c, String s, String t) {
            throw new AccessDeniedException("denied");
        }
        @Override public void checkCanAlterTable(UserIdentity u, String c, String s, String t) {
            throw new AccessDeniedException("denied");
        }
        @Override public void checkCanRenameTable(UserIdentity u, String c, String s, String ot,
                                                  String nc, String ns, String nt) {
            throw new AccessDeniedException("denied");
        }
        @Override public void checkCanCreateSchema(UserIdentity u, String c, String s) {
            throw new AccessDeniedException("denied");
        }
        @Override public void checkCanDropSchema(UserIdentity u, String c, String s) {
            throw new AccessDeniedException("denied");
        }
        @Override public void checkCanAlterSchema(UserIdentity u, String c, String s) {
            throw new AccessDeniedException("denied");
        }
        @Override public void checkCanShowSchemas(UserIdentity u, String c) {
            throw new AccessDeniedException("denied");
        }
        @Override public void checkCanShowTables(UserIdentity u, String c, String s) {
            throw new AccessDeniedException("denied");
        }
        @Override public void checkCanShowColumns(UserIdentity u, String c, String s, String t) {
            throw new AccessDeniedException("denied");
        }
        @Override public void checkCanSelectFromColumns(UserIdentity u, String c, String s, String t, Set<String> cols) {
            throw new AccessDeniedException("denied");
        }
        @Override public Optional<String> getRowFilter(UserIdentity u, String c, String s, String t) {
            return Optional.empty();
        }
        @Override public Optional<String> getColumnMask(UserIdentity u, String c, String s, String t, String col) {
            return Optional.empty();
        }
    }
}
