package org.pangrova.coordinator.ai.embedding;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.AuthorizedRelation;
import org.pangrova.spi.security.AuthorizationDeniedException;
import org.pangrova.spi.security.authz.DeferredAuthorizationScope;
import org.pangrova.spi.security.policy.Privilege;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.security.UserIdentity;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Exact readonly generated input compilation and observations from that same compiler invocation. */
final class GeneratedSourceReads {
    private GeneratedSourceReads() {}

    static ObservedRead compile(GeneratedSourceCompiler compiler, QualifiedTableName table,
                               List<String> columns, ConnectorSession session) {
        if (compiler == null || columns.isEmpty()) {
            throw new AuthorizationDeniedException("Generated input requires the trusted protected relation compiler");
        }
        String tenant = session.getProperties().get("tenant_id");
        requireActor(session, tenant);
        String sql = "SELECT " + columns.stream().map(GeneratedSourceReads::quote)
                .collect(java.util.stream.Collectors.joining(",")) + " FROM "
                + quote(table.catalog()) + "." + quote(table.schema()) + "." + quote(table.table());
        var actor = new CoordinatorSessionContext("generated-input-" + UUID.randomUUID(),
                Map.of("catalog", table.catalog(), "schema", table.schema()),
                new UserIdentity(session.getUser(), Set.of(), Map.of("tenant_id", tenant), Instant.EPOCH), tenant);
        AuthorizedRelation compiled = compiler.compile(sql, actor);
        return observe(compiled, table, columns);
    }

    private static ObservedRead observe(AuthorizedRelation compiled, QualifiedTableName table, List<String> columns) {
        if (!compiled.splitIndependent() || compiled.sources().size() != 1
                || !compiled.sources().get(0).qualifiedName().equals(table.toString())
                || !compiled.outputNames().equals(columns)) {
            throw new AuthorizationDeniedException("Generated input must compile its exact source and columns");
        }
        var observed = compiled.observedPolicies().get(table.toString());
        Set<String> readColumns = Set.copyOf(compiled.sources().get(0).tableColumns());
        if (observed == null || !observed.columnMasks().keySet().equals(readColumns)) {
            throw new AuthorizationDeniedException("Generated input has incomplete actual policy observations");
        }
        return new ObservedRead(compiled, new DeferredAuthorizationScope.TablePermission(table, readColumns,
                Privilege.SELECT, DeferredAuthorizationScope.ReadMode.SECURITY_TRANSFORMED, observed));
    }

    private static void requireActor(ConnectorSession session, String tenant) {
        if (tenant == null || tenant.isBlank() || session.getUser() == null || session.getUser().isBlank()) {
            throw new AuthorizationDeniedException("Generated input requires its actual writer and tenant");
        }
    }

    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    record ObservedRead(AuthorizedRelation relation, DeferredAuthorizationScope.TablePermission permission) {}
}
