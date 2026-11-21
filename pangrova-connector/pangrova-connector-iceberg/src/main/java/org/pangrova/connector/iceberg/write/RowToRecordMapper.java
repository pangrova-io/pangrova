package org.pangrova.connector.iceberg.write;

import org.pangrova.types.value.ColumnValue;
import org.pangrova.types.value.TypedRow;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Validates and applies the mapping from upstream page columns to a target
 * Iceberg write schema, emitting one {@link GenericRecord} per row.
 *
 * <p>Historically the page sink built records by iterating target schema fields
 * and silently filling NULL whenever a field name was missing in the source row
 * (see F-ICEBERG-08). That hid common authoring bugs — typo'd column names,
 * accidentally dropped projections, schema drift after evolution — behind
 * silent NULL writes. This mapper instead validates the column set <em>once</em>
 * (at the first page) and rejects clearly-broken inputs early.
 *
 * <p>Validation rules (depend on {@link IcebergWriteOptions#strictColumnMapping()}):
 * <ul>
 *   <li><b>Strict (default):</b> extra source columns → fail; missing required
 *       (NOT NULL) target columns → fail; missing nullable target columns →
 *       warn once, fill NULL.</li>
 *   <li><b>Lenient:</b> extra source columns → warn once and ignore; missing
 *       required target columns → still fail (the writer cannot encode NULL
 *       into a required field); missing nullable target columns → warn once
 *       and fill NULL.</li>
 * </ul>
 *
 * <p>Field IDs from the target schema are recorded for diagnostics so that
 * post-rename schemas surface clearly in error messages even though source
 * rows do not currently carry IDs.
 *
 * <p>The mapper is single-threaded (one per page sink instance).
 */
final class RowToRecordMapper {

    private static final Logger LOG = LoggerFactory.getLogger(RowToRecordMapper.class);

    private final Schema writeSchema;
    private final Schema parquetSafeSchema;
    private final IcebergWriteOptions options;
    private final String schemaName;
    private final String tableName;

    /** Target field plan, computed at first appendPage. {@code null} entries mean "fill NULL". */
    private List<Types.NestedField> targetFields;
    /** Whether the column mapping has been validated. */
    private boolean validated;

    RowToRecordMapper(Schema writeSchema,
                      IcebergWriteOptions options,
                      String schemaName,
                      String tableName) {
        this.writeSchema = writeSchema;
        this.parquetSafeSchema = IcebergValueCoercion.sanitizeSchemaForParquet(writeSchema);
        this.options = options == null ? IcebergWriteOptions.DEFAULT : options;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    /**
     * Validates the source column list against the target schema. Idempotent —
     * the first non-empty call wins; later calls are no-ops as long as the
     * column list is identical.
     *
     * @param sourceColumns the columns advertised by the incoming page
     */
    void validateColumns(List<String> sourceColumns) {
        if (validated) {
            return;
        }
        Set<String> sourceSet = new LinkedHashSet<>(sourceColumns != null ? sourceColumns : List.of());
        Set<String> targetNames = new HashSet<>();
        List<String> missingRequired = new ArrayList<>();
        List<String> missingNullable = new ArrayList<>();
        classifyMissingColumns(sourceSet, targetNames, missingRequired, missingNullable);

        if (!missingRequired.isEmpty()) {
            throw new IllegalArgumentException(
                    "Iceberg write rejected for table %s.%s: required column(s) missing from source page: %s. "
                            .formatted(schemaName, tableName, missingRequired)
                            + "Source columns=" + sourceSet
                            + ", target schema columns=" + targetNames);
        }

        checkExtraColumns(sourceSet, targetNames);

        if (!missingNullable.isEmpty()) {
            LOG.warn("event=iceberg_write_nullable_columns_filled_null table={}.{} columns={}",
                    schemaName, tableName, missingNullable);
        }

        this.targetFields = List.copyOf(writeSchema.columns());
        this.validated = true;
    }

    private void classifyMissingColumns(Set<String> sourceSet, Set<String> targetNames,
                                        List<String> missingRequired, List<String> missingNullable) {
        for (Types.NestedField field : writeSchema.columns()) {
            targetNames.add(field.name());
            if (sourceSet.contains(field.name()) || field.writeDefault() != null) {
                continue;
            }
            if (field.isRequired()) {
                missingRequired.add(formatField(field));
            } else {
                missingNullable.add(formatField(field));
            }
        }
    }

    private void checkExtraColumns(Set<String> sourceSet, Set<String> targetNames) {
        List<String> extraColumns = new ArrayList<>();
        for (String src : sourceSet) {
            if (!targetNames.contains(src)) {
                extraColumns.add(src);
            }
        }
        if (extraColumns.isEmpty()) {
            return;
        }
        if (options.strictColumnMapping()) {
            throw new IllegalArgumentException(
                    "Iceberg write rejected for table %s.%s: source page contains column(s) "
                            .formatted(schemaName, tableName)
                            + extraColumns + " not present in target schema "
                            + targetNames + ". "
                            + "Disable '" + IcebergWriteOptions.PROP_STRICT_COLUMN_MAPPING
                            + "' to allow lenient matching (extra columns will be dropped).");
        }
        LOG.warn("event=iceberg_write_extra_columns_dropped table={}.{} extra={} target={}",
                schemaName, tableName, extraColumns, targetNames);
    }

    /**
     * Returns whether {@link #validateColumns(List)} has succeeded.
     */
    boolean isValidated() {
        return validated;
    }

    /**
     * Converts a single {@link TypedRow} into a {@link GenericRecord}, applying
     * the per-field type coercion in {@link IcebergValueCoercion#coerceValue}.
     *
     * <p>Per-row safety net: if a row drops a column that was present in the
     * validating page (e.g. a malformed batch from a buggy upstream), that
     * column is treated as missing — required → fail, nullable → NULL.
     */
    GenericRecord toRecord(TypedRow row) {
        if (!validated) {
            throw new IllegalStateException(
                    "RowToRecordMapper used before validateColumns() — internal contract violation");
        }
        GenericRecord record = GenericRecord.create(parquetSafeSchema);
        for (Types.NestedField field : targetFields) {
            ColumnValue cv;
            if (row.containsColumn(field.name())) {
                cv = row.get(field.name());
            } else if (field.isRequired() && field.writeDefault() == null) {
                throw new IllegalArgumentException(
                        "Iceberg write rejected for table %s.%s: row missing required column '%s' (field id %d)"
                                .formatted(schemaName, tableName, field.name(), field.fieldId()));
            } else {
                cv = new ColumnValue.NullValue();
            }
            Object value = ColumnValue.toRaw(cv);
            Object coerced = IcebergValueCoercion.coerceValue(value, field.type());
            if (field.type().typeId() != org.apache.iceberg.types.Type.TypeID.UNKNOWN) {
                record.setField(field.name(), coerced);
            }
        }
        return record;
    }

    private static String formatField(Types.NestedField field) {
        return field.name() + "(id=" + field.fieldId() + ")";
    }
}
