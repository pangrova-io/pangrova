package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.parser.ast.statement.RevokeTenantStatement;
import org.pangrova.security.tenant.TenantLifecycleManager;
import org.pangrova.spi.connector.data.value.TypedRow;

/**
 * Executes REVOKE TENANT by delegating to {@link TenantLifecycleManager#revokeTenant}.
 */
public final class RevokeTenantTask implements MetadataTask<RevokeTenantStatement> {

    private final TenantLifecycleManager manager;

    public RevokeTenantTask(TenantLifecycleManager manager) {
        this.manager = manager;
    }

    @Override
    public StatementResult execute(RevokeTenantStatement statement, StatementExecutionContext context) {
        String tenantName = statement.tenantName().parts().get(statement.tenantName().parts().size() - 1);
        String principal = statement.principal().parts().get(statement.principal().parts().size() - 1);
        manager.revokeTenant(tenantName, principal);
        List<TypedRow> rows = List.of(TypedRow.builder().putString("result", "REVOKE TENANT succeeded").build());
        return new StatementResult(rows, extractColumnNames(rows), null, StatementCategory.TENANT);
    }

    private static List<String> extractColumnNames(List<TypedRow> rows) {
        return rows.isEmpty() ? List.of() : List.copyOf(rows.get(0).columnNames());
    }
}
