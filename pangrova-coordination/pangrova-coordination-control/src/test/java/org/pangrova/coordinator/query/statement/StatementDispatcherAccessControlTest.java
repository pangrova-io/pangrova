package org.pangrova.coordinator.query.statement;
import org.pangrova.query.dispatch.StatementResult;

import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.coordinator.query.execution.dispatch.*;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.DropTableStatement;
import org.pangrova.security.api.authorization.AccessControl;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.security.api.identity.UserIdentity;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies StatementDispatcher rejects operations when AccessControl denies permission.
 */
class StatementDispatcherAccessControlTest {

    private static final UserIdentity TEST_USER = new UserIdentity(
            "alice", Set.of("analysts"), Map.of(), Instant.now());

    private static final QuerySession SESSION_WITH_USER =
            new QuerySession("q-ac", Map.of(), TEST_USER, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

    private static final QuerySession SESSION_NO_USER =
            new QuerySession("q-ac-null", Map.of(), null, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

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

    @Test
    void staleCoordinatorGrantRejectsStatementBeforeFactoryInvocation() {
        var invoked = new java.util.concurrent.atomic.AtomicBoolean();
        MetadataTask<Statement> task = (statement, context) -> {
            invoked.set(true);
            return new StatementResult(List.of(), List.of(), null, StatementCategory.DDL);
        };
        var factory = new MetadataExecutionFactory(Map.of(DropTableStatement.class, task));
        var context = new StatementDispatcherContext(null, List.of(factory), List.of(), null);
        var now = Instant.now();
        var grant = new ExecutionAnchor(SESSION_WITH_USER.queryId(), "coord", "incarnation", 4, now, now.plusSeconds(30));
        var observed = new java.util.concurrent.atomic.AtomicReference<ExecutionAnchor>();
        var dispatcher = new StatementDispatcher(context, proposed -> {
            observed.set(proposed);
            return false;
        });
        var state = new QueryStateMachine();
        state.bindJournalGrant(grant);
        var statement = new DropTableStatement(Identifier.of("catalog", "schema", "orders"), false);

        assertThatThrownBy(() -> dispatcher.tryDispatch(state, statement, grant.executionId(), SESSION_WITH_USER))
                .hasMessageContaining("Coordinator statement grant no longer permits execution");
        assertThat(observed.get()).isSameAs(grant);
        assertThat(invoked.get()).isFalse();
    }

    private static StatementDispatcher buildDispatcher(AccessControl accessControl) {
        MetadataTask<Statement> noOp = (stmt, ctx) -> new StatementResult(
                List.of(), List.of(), null, StatementCategory.DDL);
        var tasks = new HashMap<Class<? extends Statement>, MetadataTask<?>>();
        tasks.put(DropTableStatement.class, noOp);
        var factory = new MetadataExecutionFactory(tasks);
        var context = new StatementDispatcherContext(
                null, List.of(factory), List.of(), accessControl);
        return new StatementDispatcher(context);
    }

    private static final class DenyAllAccessControl implements AccessControl {
        @Override
        public void checkCanCreateView(UserIdentity u, String c, String s, String v, boolean replace) {
            throw new org.pangrova.security.api.exception.AccessDeniedException("view creation denied");
        }
        @Override
        public void checkCanDropView(UserIdentity u, String c, String s, String v) {
            throw new org.pangrova.security.api.exception.AccessDeniedException("view drop denied");
        }
        @Override public void checkCanUpdateTable(UserIdentity u, String c, String s, String t) {
            throw new AccessDeniedException("denied");
        }
        @Override public void checkCanAccessCatalog(UserIdentity u, String c) {
            throw new AccessDeniedException("denied");
        }
        @Override public void checkCanCreateCatalog(UserIdentity u, String c) { throw new AccessDeniedException("denied"); }
        @Override public void checkCanAlterCatalog(UserIdentity u, String c) { throw new AccessDeniedException("denied"); }
        @Override public void checkCanDropCatalog(UserIdentity u, String c) { throw new AccessDeniedException("denied"); }
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
        @Override public org.pangrova.security.api.mask.TableColumnMasks getColumnMasks(
                UserIdentity u, String c, String s, String t) {
            return column -> Optional.empty();
        }
    }
}
