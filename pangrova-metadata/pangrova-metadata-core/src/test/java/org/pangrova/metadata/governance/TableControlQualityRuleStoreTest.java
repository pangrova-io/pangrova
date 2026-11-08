package org.pangrova.metadata.governance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.governance.api.quality.QualityCheckResult;
import org.pangrova.governance.api.quality.Severity;
import org.pangrova.governance.api.quality.DataQualityRule;
import org.pangrova.governance.api.quality.QualityExecutionIdentity;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.table.api.identity.CatalogDeploymentIdentity;
import org.pangrova.persistence.table.postgresql.control.JdbcTableControlStore;
import org.pangrova.table.core.ownership.TableControlService;
import org.pangrova.persistence.governance.postgresql.JdbcQualityRuleStore;
import org.pangrova.metadata.service.RegistryTablePolicyAuthority;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.pangrova.connector.api.metadata.table.TableIdentity;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableControlQualityRuleStoreTest {
    private DataSource source;
    private PostgresCatalogDeploymentRepository metadata;
    private TableControlService control;
    private JdbcQualityRuleStore persisted;
    private TableControlQualityRuleStore rules;

    @BeforeEach
    void setup() throws Exception {
        source = SharedPostgresContainer.migratedDataSource("stable_quality");
        metadata = new PostgresCatalogDeploymentRepository(source);
        metadata.saveCatalogDescriptor(descriptor());
        var catalogs = new CatalogRegistry(metadata);
        control = new TableControlService(new JdbcTableControlStore(source), name -> {
            var descriptor = metadata.getCatalogDescriptor(name).orElseThrow();
            return new CatalogDeploymentIdentity(descriptor.incarnationId(), descriptor.revision());
        });
        catalogs.setTableControlService(control);
        persisted = new JdbcQualityRuleStore(source);
        rules = new TableControlQualityRuleStore(new RegistryTablePolicyAuthority(catalogs), persisted);
        create("orders", "original");
    }

    @Test
    void renameAndNameReuseKeepIndependentPoliciesWithoutRewritingRuleNames() {
        rules.saveRule(rule(1, "orders"));
        var original = rules.getRule(1).orElseThrow();
        var pending = control.rename(control.find("cat", "db", "orders").orElseThrow(), "db", "renamed");
        assertThatThrownBy(() -> rules.getRule(1)).hasMessageContaining("ACTIVE");
        assertThat(rules.getAllEnabledRules()).extracting(DataQualityRule::ruleId).containsExactly(1L);
        control.completeRename(pending, Optional.of(identity("original")), Optional.empty());
        assertThat(rules.getRule(1)).hasValueSatisfying(value -> {
            assertThat(value.tableId()).isEqualTo(original.tableId());
            assertThat(value.tableName()).isEqualTo("renamed");
        });
        create("orders", "replacement");
        assertThat(rules.getRulesForTable("cat", "db", "orders")).isEmpty();
        rules.saveRule(rule(2, "orders"));
        assertThatThrownBy(() -> rules.saveRule(original)).hasMessageContaining("replaced");
        assertThatThrownBy(() -> rules.saveRule(rule(1, "orders")))
                .hasMessageContaining("another stable table identity");
        persisted.deleteRule(1);
        assertThatThrownBy(() -> rules.saveRule(rule(1, "orders")))
                .hasMessageContaining("another stable table identity");
        assertThat(rules.getRulesForTable("cat", "db", "orders")).singleElement()
                .satisfies(value -> assertThat(value.ruleId()).isEqualTo(2));
    }

    @Test
    void unprovenEnabledRulesCannotExecuteOrBeSilentlyAdopted() {
        persisted.saveRule(rule(1, "orders"));
        assertThatThrownBy(() -> rules.getRule(1)).hasMessageContaining("no proven stable table binding");
        assertThatThrownBy(() -> rules.getRulesForTable("cat", "db", "orders"))
                .hasMessageContaining("no proven stable table binding");
        assertThatThrownBy(rules::getAllEnabledRules).hasMessageContaining("no proven stable table binding");
        assertThatThrownBy(() -> rules.saveRule(rule(1, "orders")))
                .hasMessageContaining("another stable table identity");
        assertThat(persisted.getRule(1)).hasValueSatisfying(value -> {
            assertThat(value.tableId()).isNull();
            assertThat(value.checkExpression()).isEqualTo("id > 0");
        });
    }

    @Test
    void catalogReplacementCannotScheduleOrExecuteAnEarlierCatalogsRule() {
        rules.saveRule(rule(1, "orders"));
        metadata.deleteCatalogDescriptor("cat");
        metadata.saveCatalogDescriptor(descriptor());
        assertThatThrownBy(() -> rules.getRule(1)).hasMessageContaining("catalog deployment");
        assertThatThrownBy(rules::getAllEnabledRules).hasMessageContaining("catalog deployment");
    }

    @Test
    void auditResultsKeepTheCapturedTableIdentityAfterRetirementAndCannotClaimAnotherRule() throws Exception {
        rules.saveRule(rule(1, "orders"));
        var original = rules.getRule(1).orElseThrow();
        var record = control.findById(original.tableId()).orElseThrow();
        control.markDropped(record);
        create("orders", "replacement");
        rules.saveRule(rule(2, "orders"));
        var replacement = rules.getRule(2).orElseThrow();
        assertThat(rules.getAllEnabledRules()).extracting(DataQualityRule::ruleId).containsExactly(2L);
        rules.saveResult(new QualityCheckResult(0, 1, "positive", "orders", Severity.ERROR, Instant.EPOCH,
                false, 2, 1, 1, "captured evaluation", original.tableId()));
        assertThatThrownBy(() -> rules.saveResult(new QualityCheckResult(0, 1, "positive", "orders", Severity.ERROR,
                Instant.EPOCH, true, 2, 0, 1, null, replacement.tableId())))
                .hasMessageContaining("another stable table identity");
        persisted.deleteRule(1);
        rules.saveResult(new QualityCheckResult(0, 1, "positive", "orders", Severity.ERROR, Instant.EPOCH,
                false, 2, 1, 1, "late evaluation after rule deletion", original.tableId()));
        try (var connection = source.getConnection(); var query = connection.prepareStatement(
                "SELECT table_id,failed_rows FROM quality_check_results WHERE rule_id=1"); var row = query.executeQuery()) {
            assertThat(row.next()).isTrue();
            assertThat(row.getObject(1, java.util.UUID.class)).isEqualTo(original.tableId());
            assertThat(row.getLong(2)).isEqualTo(1);
            assertThat(row.next()).isTrue();
            assertThat(row.getObject(1, java.util.UUID.class)).isEqualTo(original.tableId());
            assertThat(row.next()).isFalse();
        }
    }

    private void create(String table, String id) {
        control.registerCreated("cat", "db", table, "alice", Optional.of(identity(id)), Optional.empty());
    }

    private static TableIdentity identity(String id) {
        return new TableIdentity("TEST", id, "file:/" + id, "schema");
    }

    private static CatalogDescriptor descriptor() {
        return new CatalogDescriptor("cat", CatalogType.CONNECTOR, "test", CatalogState.ACTIVE, Map.of(), "", Set.of());
    }

    private static DataQualityRule rule(long id, String table) {
        return new DataQualityRule(id, "positive", "cat", "db", table, "id > 0", Severity.ERROR, "positive values",
                true, "0 0 * * * ?", Instant.EPOCH, Instant.EPOCH,
                new QualityExecutionIdentity("alice", "tenant-a"), null);
    }
}
