package org.pangrova.coordinator.query.statement;

import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.query.core.session.SessionTableNames;
import org.junit.jupiter.api.Test;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class QualifiedTableSessionTest {
    @Test
    void resolvesShortNamesUsingQueryDefaultsAndQualifiedNamesUsingTheirTarget() {
        var original = StatementTestSessions.context();
        var context = new QuerySession(original.queryId(),
                Map.of("catalog", "actual_catalog", "schema", "actual_schema"),
                original.userIdentity(), original.tenantId(), java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
        assertThat(SessionTableNames.resolveTableRef(List.of("docs"), context))
                .isEqualTo(new QualifiedTableName("actual_catalog", "actual_schema", "docs"));
        assertThat(SessionTableNames.resolveTableRef(List.of("target_schema", "docs"), context))
                .isEqualTo(new QualifiedTableName("actual_catalog", "target_schema", "docs"));
        assertThat(SessionTableNames.resolveTableRef(List.of("other", "target_schema", "docs"), context))
                .isEqualTo(new QualifiedTableName("other", "target_schema", "docs"));
    }

    @Test
    void rebindsTheExactTargetWithoutElevatingClientIdentityOrTenantProperties() {
        var original = StatementTestSessions.context();
        var context = new QuerySession(original.queryId(), Map.of(
                "catalog", "wrong", "schema", "wrong", "user", "spoofed-user",
                "tenant_id", "spoofed-tenant", "pangrova.tenant_id", "spoofed-alias"),
                original.userIdentity(), original.tenantId(), java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
        var session = SessionContextConverter.toConnectorSession(context, "target_catalog", "target_schema");
        assertThat(session.getUser()).isEqualTo("statement-user");
        assertThat(session.getCatalog()).contains("target_catalog");
        assertThat(session.getSchema()).contains("target_schema");
        assertThat(session.getProperties()).containsEntry("tenant_id", "tenant-a")
                .containsEntry("user", "statement-user")
                .containsEntry("catalog", "target_catalog").containsEntry("schema", "target_schema")
                .doesNotContainKey("pangrova.tenant_id");
    }
}
