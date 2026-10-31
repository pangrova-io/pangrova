package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.parser.ast.statement.GrantTenantStatement;
import org.pangrova.security.core.tenant.TenantLifecycleManager;
import org.pangrova.connector.spi.data.value.TypedRow;

/**
 * Executes GRANT TENANT by delegating to {@link TenantLifecycleManager#grantTenant}.
 */
public final class GrantTenantTask implements MetadataTask<GrantTenantStatement> {

    private final TenantLifecycleManager manager;

    public GrantTenantTask(TenantLifecycleManager manager) {
        this.manager = manager;
    }

    @Override
    public StatementResult execute(GrantTenantStatement statement, StatementExecutionContext context) {
        String tenantName = statement.tenantName().parts().get(statement.tenantName().parts().size() - 1);
        String principal = statement.principal().parts().get(statement.principal().parts().size() - 1);
        manager.grantTenant(tenantName, principal);
        List<TypedRow> rows = List.of(TypedRow.builder().putString("result", "GRANT TENANT succeeded").build());
        return new StatementResult(rows, extractColumnNames(rows), null, StatementCategory.TENANT);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
