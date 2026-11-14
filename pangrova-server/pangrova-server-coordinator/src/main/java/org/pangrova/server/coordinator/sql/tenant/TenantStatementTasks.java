package org.pangrova.server.coordinator.sql.tenant;
import org.pangrova.security.api.exception.AccessDeniedException;

import java.util.List;
import java.util.Optional;

import org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.statement.tenant.GrantTenantStatement;
import org.pangrova.parser.api.ast.statement.tenant.RevokeTenantStatement;
import org.pangrova.persistence.security.api.tenant.TenantStore;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.security.api.exception.AuthorizationDeniedException;
import org.pangrova.security.api.tenant.TenantMembershipMutation;
import org.pangrova.types.value.TypedRow;

/** SQL adapter only; current membership authorization and mutation belong to the atomic store operation. */
public final class TenantStatementTasks {
    private final Optional<TenantStore> store;

    public TenantStatementTasks(Optional<TenantStore> store) {
        this.store = java.util.Objects.requireNonNull(store, "store");
    }

    /** Registers tenant membership statement handlers. */
    public void register(MetadataExecutionFactory factory) {
        factory.registerTask(GrantTenantStatement.class, (statement, context) -> execute(context,
                statement.tenantName(), statement.principal(), TenantMembershipMutation.Operation.GRANT));
        factory.registerTask(RevokeTenantStatement.class, (statement, context) -> execute(context,
                statement.tenantName(), statement.principal(), TenantMembershipMutation.Operation.REVOKE));
    }

    private StatementResult execute(StatementExecutionContext context, Identifier tenant, Identifier principal,
            TenantMembershipMutation.Operation operation) {
        var actor = context.session().userIdentity();
        if (actor == null) throw new AuthorizationDeniedException("Authenticated tenant administrator is required");
        if (tenant.parts().size() != 1 || principal.parts().size() != 1) {
            throw new IllegalArgumentException("Tenant ID and principal must each be a single identifier");
        }
        var mutation = new TenantMembershipMutation(tenant.parts().get(0), principal.parts().get(0), operation);
        var authority = store.orElseThrow(() -> new AccessDeniedException(
                "Tenant membership administration requires authentication and configured persistence"));
        boolean changed = authority.mutateMembership(actor, mutation);
        return new StatementResult(List.of(TypedRow.builder().putString("tenant_id", mutation.tenantId())
                .putString("principal", mutation.principal()).putRaw("changed", changed).build()),
                List.of("tenant_id", "principal", "changed"), null, StatementCategory.TENANT);
    }
}
