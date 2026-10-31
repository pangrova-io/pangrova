package org.pangrova.governance.core.classification;

import org.pangrova.security.api.mask.ColumnMaskRecord;
import org.pangrova.security.api.mask.ColumnMaskStore;
import org.pangrova.governance.api.classification.AuditConfigStore;
import org.pangrova.governance.api.classification.TagChangedEvent;
import org.pangrova.governance.api.classification.TagChangedEventListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies ColumnTagService emits TagChangedEvent on tag assign/remove,
 * and that the auto-mask pipeline (TagBasedSecurityPolicy) activates on tag changes.
 */
class ColumnTagServiceTest {
    private final java.util.Map<org.pangrova.connector.spi.metadata.table.QualifiedTableName, java.util.UUID>
            tableIds = new java.util.HashMap<>();

    private ColumnTagService tagService;
    private TagBasedSecurityPolicy securityPolicy;
    private InMemoryColumnMaskStore maskStore;

    @BeforeEach
    void setUp() {
        maskStore = new InMemoryColumnMaskStore();
        securityPolicy = new TagBasedSecurityPolicy(maskStore, new NoOpAuditConfigStore());
        tagService = new ColumnTagService(table -> tableIds.computeIfAbsent(table, ignored -> java.util.UUID.randomUUID()));
        tagService.addListener(securityPolicy);
    }

    @Test
    void assignPiiTagEmitsEventAndAppliesMask() {
        tagService.assignTag("catalog", "schema", "users", "email",
                "pii_type", null, "PII_EMAIL");

        List<ColumnMaskRecord> masks = maskStore.getMasks("catalog", "schema", "users", "email");
        assertThat(masks).hasSize(1);
        assertThat(masks.get(0).maskExpression()).isEqualTo("CASE WHEN {column} IS NULL THEN NULL ELSE '***@***' END");
        assertThat(masks.get(0).maskName()).isEqualTo("auto_pii_email");
    }

    @Test
    void removeTagEmitsEventAndRemovesMask() {
        // First assign
        tagService.assignTag("catalog", "schema", "users", "ssn",
                "pii_type", null, "PII_SSN");
        assertThat(maskStore.getMasks("catalog", "schema", "users", "ssn")).hasSize(1);

        // Then remove
        tagService.removeTag("catalog", "schema", "users", "ssn",
                "pii_type", "PII_SSN");

        assertThat(maskStore.getMasks("catalog", "schema", "users", "ssn")).isEmpty();
    }

    @Test
    void assignTagEmitsTagChangedEvent() {
        AtomicReference<TagChangedEvent> captured = new AtomicReference<>();
        tagService.addListener(captured::set);

        tagService.assignTag("cat", "sch", "tbl", "col",
                "pii_type", null, "PII_PHONE");

        TagChangedEvent event = captured.get();
        assertThat(event).isNotNull();
        assertThat(event.catalog()).isEqualTo("cat");
        assertThat(event.schema()).isEqualTo("sch");
        assertThat(event.table()).isEqualTo("tbl");
        assertThat(event.columnName()).isEqualTo("col");
        assertThat(event.tagKey()).isEqualTo("pii_type");
        assertThat(event.oldValue()).isNull();
        assertThat(event.newValue()).isEqualTo("PII_PHONE");
    }

    @Test
    void removeTagEmitsTagChangedEventWithNullNewValue() {
        AtomicReference<TagChangedEvent> captured = new AtomicReference<>();
        tagService.addListener(captured::set);

        tagService.removeTag("cat", "sch", "tbl", "col",
                "pii_type", "PII_NAME");

        TagChangedEvent event = captured.get();
        assertThat(event).isNotNull();
        assertThat(event.oldValue()).isEqualTo("PII_NAME");
        assertThat(event.newValue()).isNull();
    }

    @Test
    void listenerFailureDoesNotBlockOtherListeners() {
        AtomicReference<TagChangedEvent> captured = new AtomicReference<>();
        // Add a failing listener first
        tagService.addListener(event -> { throw new RuntimeException("boom"); });
        tagService.addListener(captured::set);

        assertThatThrownBy(() -> tagService.assignTag("cat", "sch", "tbl", "col",
                "pii_type", null, "PII_EMAIL"))
                .isInstanceOf(RuntimeException.class).hasMessage("boom");

        // Second listener still receives the event
        assertThat(captured.get()).isNotNull();
    }

    @Test
    void assignTagRejectsNullNewValue() {
        assertThatThrownBy(() ->
                tagService.assignTag("cat", "sch", "tbl", "col", "key", null, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void removeTagRejectsNullOldValue() {
        assertThatThrownBy(() ->
                tagService.removeTag("cat", "sch", "tbl", "col", "key", null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void removeListenerStopsNotifications() {
        AtomicReference<TagChangedEvent> captured = new AtomicReference<>();
        TagChangedEventListener listener = captured::set;
        tagService.addListener(listener);
        tagService.removeListener(listener);

        tagService.assignTag("cat", "sch", "tbl", "col",
                "pii_type", null, "PII_EMAIL");

        assertThat(captured.get()).isNull();
    }

    static class InMemoryColumnMaskStore implements ColumnMaskStore {
        private final List<ColumnMaskRecord> masks = new ArrayList<>();
        private long nextId = 1;

        @Override
        public List<ColumnMaskRecord> getMasks(String catalog, String schema, String table, String column) {
            return masks.stream()
                    .filter(m -> m.catalogName().equals(catalog)
                            && m.schemaName().equals(schema)
                            && m.tableName().equals(table)
                            && m.columnName().equals(column))
                    .toList();
        }

        @Override
        public void saveMask(ColumnMaskRecord mask) {
            masks.removeIf(m -> m.maskName().equals(mask.maskName())
                    && m.tableName().equals(mask.tableName())
                    && m.columnName().equals(mask.columnName()));
            masks.add(new ColumnMaskRecord(nextId++, mask.maskName(), mask.catalogName(),
                    mask.schemaName(), mask.tableName(), mask.columnName(),
                    mask.maskExpression(), mask.applyToRoles(), mask.exemptRoles(),
                    mask.enabled(), mask.tableId()));
        }

        @Override
        public void deleteMask(long maskId) {
            masks.removeIf(m -> m.maskId() == maskId);
        }
    }

    static class NoOpAuditConfigStore implements AuditConfigStore {
        @Override public void enableDetailedAudit(String cat, String schema, String table) { }
        @Override public void disableDetailedAudit(String cat, String schema, String table) { }
        @Override public boolean isDetailedAuditEnabled(String cat, String schema, String table) {
            return false;
        }
    }
}
