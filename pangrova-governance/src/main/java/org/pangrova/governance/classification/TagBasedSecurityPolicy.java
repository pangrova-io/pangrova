package org.pangrova.governance.classification;

import org.pangrova.spi.security.ColumnMaskRecord;
import org.pangrova.spi.security.ColumnMaskStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Event-driven security policy that auto-applies column masks based on PII tags.
 * Listens for {@link TagChangedEvent} and activates masking rules accordingly.
 */
public class TagBasedSecurityPolicy implements TagChangedEventListener {

    private static final Logger LOG = LoggerFactory.getLogger(TagBasedSecurityPolicy.class);
    private static final String PII_TYPE_TAG = "pii_type";
    private static final String SENSITIVITY_TAG = "sensitivity_level";

    private static final Map<String, String> PII_MASK_MAP = Map.of(
        "PII_EMAIL", "mask_email",
        "PII_PHONE", "mask_phone",
        "PII_SSN", "partial",
        "PII_NAME", "mask_name"
    );

    private final ColumnMaskStore columnMaskStore;
    private final AuditConfigStore auditConfigStore;

    public TagBasedSecurityPolicy(ColumnMaskStore columnMaskStore,
                                   AuditConfigStore auditConfigStore) {
        this.columnMaskStore = columnMaskStore;
        this.auditConfigStore = auditConfigStore;
    }

    @Override
    public void onTagChanged(TagChangedEvent event) {
        if (event.columnName() != null) {
            handleColumnTagChange(event);
        } else {
            handleTableTagChange(event);
        }
    }

    private void handleColumnTagChange(TagChangedEvent event) {
        if (!PII_TYPE_TAG.equals(event.tagKey())) return;
        if (event.newValue() == null) {
            removeMask(event);
            return;
        }
        applyMask(event);
    }

    private void handleTableTagChange(TagChangedEvent event) {
        if (!SENSITIVITY_TAG.equals(event.tagKey())) return;
        boolean sensitive = isSensitiveLevel(event.newValue());
        toggleAudit(event, sensitive);
    }

    private boolean isSensitiveLevel(String level) {
        return "CONFIDENTIAL".equals(level) || "RESTRICTED".equals(level);
    }

    private void toggleAudit(TagChangedEvent event, boolean enable) {
        if (enable) {
            auditConfigStore.enableDetailedAudit(event.catalog(), event.schema(), event.table());
        } else {
            auditConfigStore.disableDetailedAudit(event.catalog(), event.schema(), event.table());
        }
    }

    private void applyMask(TagChangedEvent event) {
        String maskExpr = piiTypeToMaskExpression(event.newValue());
        if (maskExpr == null) {
            LOG.warn("Unknown PII type: {}, skipping mask", event.newValue());
            return;
        }
        ColumnMaskRecord mask = new ColumnMaskRecord(
            0, "auto_pii_" + event.columnName(),
            event.catalog(), event.schema(), event.table(),
            event.columnName(), maskExpr, List.of(), List.of(), true);
        columnMaskStore.saveMask(mask);
    }

    private void removeMask(TagChangedEvent event) {
        List<ColumnMaskRecord> masks = columnMaskStore.getMasks(
            event.catalog(), event.schema(), event.table(), event.columnName());
        masks.stream()
            .filter(m -> m.maskName().startsWith("auto_pii_"))
            .forEach(m -> columnMaskStore.deleteMask(m.maskId()));
    }

    String piiTypeToMaskExpression(String piiType) {
        return PII_MASK_MAP.get(piiType);
    }
}
