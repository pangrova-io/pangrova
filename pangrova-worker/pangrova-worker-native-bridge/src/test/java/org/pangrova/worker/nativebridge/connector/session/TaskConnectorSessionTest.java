package org.pangrova.worker.nativebridge.connector.session;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.TaskAttemptId;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TaskConnectorSessionTest {
    @Test
    void canonicalTaskSecurityOverwritesClientTenantPropertiesAndRemovesAlias() {
        var security = SecurityContext.newBuilder().setTenantId("authenticated-tenant")
                .setPrincipal("authenticated-user").build();
        var attempt = TaskAttemptId.newBuilder().setTaskId("task-1").setQueryOrJobId("query-1").build();
        var session = TaskConnectorSession.from(security, attempt, "lance_catalog", Map.of(
                "tenant_id", "spoofed-tenant", "pangrova.tenant_id", "spoofed-alias", "application", "test"));
        assertThat(session.getProperties()).containsEntry("tenant_id", "authenticated-tenant")
                .containsEntry("application", "test").doesNotContainKey("pangrova.tenant_id");
        assertThat(session.getUser()).isEqualTo("authenticated-user");
        assertThat(session.tenantId()).isEqualTo("authenticated-tenant");
    }
}
