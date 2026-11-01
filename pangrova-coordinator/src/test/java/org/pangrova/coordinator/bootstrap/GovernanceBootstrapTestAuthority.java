package org.pangrova.coordinator.bootstrap;

import org.pangrova.metadata.core.catalog.CatalogRegistry;
import org.pangrova.metadata.core.control.JdbcTableControlStore;
import org.pangrova.metadata.core.control.TableControlService;
import org.pangrova.metadata.store.jdbc.JdbcMetadataStore;

import javax.sql.DataSource;

/** Technical metadata wiring matching production startup for governance lifecycle tests. */
final class GovernanceBootstrapTestAuthority {
    private GovernanceBootstrapTestAuthority() {}

    static DataSource bind(ModuleIntegrationBootstrap modules, DataSource source) {
        modules.setMetadataSyncDeps(null, registry(source));
        return source;
    }

    static DataSource bind(ModuleIntegrationWiring modules, DataSource source) {
        modules.setMetadataSyncDeps(null, registry(source));
        return source;
    }

    private static CatalogRegistry registry(DataSource source) {
        var metadata = new JdbcMetadataStore(source);
        var registry = new CatalogRegistry(metadata);
        registry.setTableControlService(new TableControlService(
                new JdbcTableControlStore(source), registry::requireCurrentCatalogIdentity));
        registry.register(metadata.getCatalogDescriptor("quality_catalog").orElseGet(() ->
                new org.pangrova.metadata.api.catalog.CatalogDescriptor("quality_catalog",
                        org.pangrova.metadata.api.catalog.CatalogType.INTERNAL, "internal",
                        org.pangrova.metadata.api.catalog.CatalogState.ACTIVE,
                        java.util.Map.of(), "", java.util.Set.of())));
        return registry;
    }

    /** Exercises the executor chosen by production bootstrap using real durable ownership and audit. */
    static void assertMissingRunnerBlocks(org.pangrova.governance.quality.gate.WriteQualityGate gate,
            DataSource source) throws Exception {
        var metadata = new JdbcMetadataStore(source);
        var descriptor = metadata.getCatalogDescriptor("quality_catalog").orElseThrow();
        var control = new TableControlService(new JdbcTableControlStore(source),
                name -> new org.pangrova.metadata.core.control.CatalogDeploymentIdentity(
                        descriptor.incarnationId(), descriptor.revision()));
        var owner = control.registerCreated("quality_catalog", "default", "quality_source", "quality-owner",
                java.util.Optional.of(new org.pangrova.spi.connector.metadata.table.TableIdentity(
                        "ICEBERG", "quality-bootstrap-owner", "s3://test/quality-source", "schema")),
                java.util.Optional.empty()).orElseThrow();
        var store = new org.pangrova.governance.quality.store.JdbcQualityRuleStore(source);
        var now = java.time.Instant.now();
        store.saveRule(new org.pangrova.governance.quality.rule.DataQualityRule(1, "required_positive",
                "quality_catalog", "default", "quality_source", "id > 0",
                org.pangrova.governance.quality.alert.Severity.ERROR, "msg", true, null, now, now,
                new org.pangrova.governance.quality.rule.QualityExecutionIdentity("quality-owner", "quality-tenant"),
                owner.tableId()));

        var result = gate.validate("quality_catalog", "default", "quality_source");
        org.assertj.core.api.Assertions.assertThat(result.allowed()).isFalse();
        org.assertj.core.api.Assertions.assertThat(result.blockReason()).contains("was not evaluated");
        try (var connection = source.getConnection(); var query = connection.createStatement();
                var audit = query.executeQuery("SELECT passed,table_id,error_message FROM quality_check_results")) {
            org.assertj.core.api.Assertions.assertThat(audit.next()).isTrue();
            org.assertj.core.api.Assertions.assertThat(audit.getBoolean("passed")).isFalse();
            org.assertj.core.api.Assertions.assertThat(audit.getObject("table_id", java.util.UUID.class))
                    .isEqualTo(owner.tableId());
            org.assertj.core.api.Assertions.assertThat(audit.getString("error_message")).contains("was not evaluated");
            org.assertj.core.api.Assertions.assertThat(audit.next()).isFalse();
        }
    }
}
