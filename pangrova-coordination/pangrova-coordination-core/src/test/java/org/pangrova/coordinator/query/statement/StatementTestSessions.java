package org.pangrova.coordinator.query.statement;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.security.api.UserIdentity;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

/** Explicit authenticated scope for statement and connector lifecycle unit fixtures. */
public final class StatementTestSessions {
    private StatementTestSessions() {}

    public static CoordinatorSessionContext context() {
        return new CoordinatorSessionContext("statement-query", Map.of("catalog", "test_cat", "schema", "default"),
                new UserIdentity("statement-user", Set.of(), Map.of(), Instant.EPOCH), "tenant-a");
    }

    public static ConnectorSession connector(String catalog, String schema) {
        return SessionContextConverter.toConnectorSession(context(), catalog, schema);
    }

    public static StatementExecutionContext executionContext() {
        var session = context();
        return new StatementExecutionContext(null, session.queryId(), session, null, null);
    }
}
