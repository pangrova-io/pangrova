package org.pangrova.metadata.adapter.governance;

import org.pangrova.governance.api.quality.QualityCheckResult;
import org.pangrova.governance.api.quality.DataQualityRule;
import org.pangrova.governance.core.quality.IdentityQualityRuleStore;
import org.pangrova.governance.api.quality.QualityRuleStore;
import org.pangrova.table.api.control.TableControlRecord;
import org.pangrova.table.api.policy.TablePolicyAuthority;
import org.pangrova.table.api.control.TableControlState;
import org.pangrova.security.api.AccessDeniedException;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Enforces stable ownership on actual quality-rule reads and writes, including scheduled reloads. */
public final class TableControlQualityRuleStore implements QualityRuleStore {
    private final TablePolicyAuthority tables;
    private final IdentityQualityRuleStore storage;

    public TableControlQualityRuleStore(TablePolicyAuthority tables, IdentityQualityRuleStore storage) {
        this.tables = Objects.requireNonNull(tables, "tables");
        this.storage = Objects.requireNonNull(storage, "storage");
    }

    @Override
    public void saveRule(DataQualityRule rule) {
        var current = tables.requireBindable(
                rule.catalogName(), rule.schemaName(), rule.tableName());
        if (rule.tableId() != null && !current.tableId().equals(rule.tableId())) {
            throw new AccessDeniedException("Quality rule belongs to a replaced table identity");
        }
        storage.saveRule(at(rule, current));
    }

    @Override
    public Optional<DataQualityRule> getRule(long ruleId) {
        return storage.getRule(ruleId).map(rule -> {
            if (rule.tableId() == null && !rule.enabled()) return rule;
            return at(rule, current(rule.tableId(), true));
        });
    }

    @Override
    public List<DataQualityRule> getRulesForTable(String catalog, String schema, String table) {
        if (storage.getRulesForTable(catalog, schema, table).stream()
                .anyMatch(rule -> rule.enabled() && rule.tableId() == null)) {
            throw unbound();
        }
        var record = tables.find(catalog, schema, table);
        if (record.isEmpty()) return List.of();
        var current = current(record.orElseThrow().tableId(), true);
        return storage.getRules(current.tableId()).stream().map(rule -> at(rule, current)).toList();
    }

    @Override
    public List<DataQualityRule> getAllEnabledRules() {
        // Scheduling retains rule IDs and reloads each one through getRule at execution time.
        // Transitional tables can recover; retired tables never enter a new schedule.
        return storage.getAllEnabledRules().stream().flatMap(rule -> {
            if (rule.tableId() == null) throw unbound();
            var record = tables.findById(rule.tableId())
                    .orElseThrow(TableControlQualityRuleStore::unbound);
            if (record.state() == TableControlState.DROPPED) return java.util.stream.Stream.empty();
            tables.requireCurrentCatalog(record, "Quality schedule read");
            return java.util.stream.Stream.of(at(rule, record));
        }).toList();
    }

    @Override
    public void saveResult(QualityCheckResult result) {
        if (result.tableId() == null || tables.findById(result.tableId()).isEmpty()) throw unbound();
        storage.getRule(result.ruleId()).ifPresent(rule -> {
            if (!result.tableId().equals(rule.tableId())) {
                throw new AccessDeniedException("Quality result belongs to another stable table identity");
            }
        });
        storage.saveResult(result);
    }

    private TableControlRecord current(UUID tableId, boolean requireActive) {
        if (tableId == null) throw unbound();
        var record = tables.findById(tableId).orElseThrow(TableControlQualityRuleStore::unbound);
        tables.requireCurrentCatalog(record, "Quality rule read");
        if (requireActive && record.state() != TableControlState.ACTIVE) {
            throw new AccessDeniedException("Quality rule requires an ACTIVE controlled table");
        }
        return record;
    }

    private static AccessDeniedException unbound() {
        return new AccessDeniedException(
                "Quality rule/result has no proven stable table binding; explicitly rebind it");
    }

    private static DataQualityRule at(DataQualityRule rule, TableControlRecord current) {
        return new DataQualityRule(rule.ruleId(), rule.ruleName(),
                current.catalog(), current.schema(), current.table(),
                rule.checkExpression(), rule.severity(), rule.message(), rule.enabled(), rule.schedule(),
                rule.createdAt(), rule.updatedAt(), rule.executionIdentity(), current.tableId());
    }
}
