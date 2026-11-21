package org.pangrova.connector.iceberg.write;

import org.pangrova.types.value.TypedRow;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link RowToRecordMapper}, the component that validates the
 * source-page columns against the target Iceberg write schema and converts
 * rows to {@link GenericRecord}s.
 *
 * <p>Pinned by F-ICEBERG-08: the legacy converter silently filled NULL when a
 * target column was missing from the source row, masking schema-drift bugs.
 * The mapper now rejects extra source columns (strict mode), missing required
 * target columns (always), and emits a warning for missing nullable target
 * columns.
 */
class RowToRecordMapperTest {

    @Test
    void nativeAnonymousRowFieldsBindToTargetStructByOrdinal() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "info", Types.StructType.of(
                        Types.NestedField.optional(3, "name", Types.StringType.get()),
                        Types.NestedField.optional(4, "age", Types.IntegerType.get()))));
        RowToRecordMapper mapper = new RowToRecordMapper(
                schema, IcebergWriteOptions.DEFAULT, "db", "t");
        mapper.validateColumns(List.of("id", "info"));

        GenericRecord row = mapper.toRecord(TypedRow.builder()
                .putLong("id", 1L)
                .putRaw("info", Map.of(
                        FunctionBindingDeclarations.anonymousComputedField(0), "Alice",
                        FunctionBindingDeclarations.anonymousComputedField(1), 30))
                .build());
        GenericRecord info = (GenericRecord) row.getField("info");
        assertThat(info.getField("name")).isEqualTo("Alice");
        assertThat(info.getField("age")).isEqualTo(30);

        assertThatThrownBy(() -> mapper.toRecord(TypedRow.builder()
                .putLong("id", 2L)
                .putRaw("info", Map.of("wrong_0", "Bob", "wrong_1", 25))
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("STRUCT source fields");

        assertThatThrownBy(() -> mapper.toRecord(TypedRow.builder()
                .putLong("id", 3L)
                .putRaw("info", Map.of("name", "Carol", "age", 27, "unexpected", true))
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("STRUCT source fields");
    }

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "tag", Types.StringType.get()));

    // ---- Strict mode (default) ----

    @Test
    void strict_exactColumnMatch_validatesAndConverts() {
        RowToRecordMapper mapper = new RowToRecordMapper(
                SCHEMA, IcebergWriteOptions.DEFAULT, "db", "t");

        mapper.validateColumns(List.of("id", "name", "tag"));

        assertThat(mapper.isValidated()).isTrue();
        TypedRow row = TypedRow.builder()
                .putLong("id", 1L)
                .putString("name", "alice")
                .putString("tag", "vip")
                .build();
        GenericRecord rec = mapper.toRecord(row);
        assertThat(rec.getField("id")).isEqualTo(1L);
        assertThat(rec.getField("name")).isEqualTo("alice");
        assertThat(rec.getField("tag")).isEqualTo("vip");
    }

    @Test
    void strict_extraSourceColumn_rejected() {
        RowToRecordMapper mapper = new RowToRecordMapper(
                SCHEMA, IcebergWriteOptions.DEFAULT, "db", "t");

        assertThatThrownBy(() ->
                mapper.validateColumns(List.of("id", "name", "tag", "ghost")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ghost")
                .hasMessageContaining("not present in target schema")
                .hasMessageContaining(IcebergWriteOptions.PROP_STRICT_COLUMN_MAPPING);
    }

    @Test
    void strict_missingRequiredColumn_rejected_messageNamesField() {
        RowToRecordMapper mapper = new RowToRecordMapper(
                SCHEMA, IcebergWriteOptions.DEFAULT, "db", "t");

        // 'name' is required (not nullable) — this must NOT silently fill NULL.
        assertThatThrownBy(() -> mapper.validateColumns(List.of("id", "tag")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("required column(s) missing")
                .hasMessageContaining("name");
    }

    @Test
    void strict_missingOptionalColumn_acceptedAndFilledWithNull() {
        RowToRecordMapper mapper = new RowToRecordMapper(
                SCHEMA, IcebergWriteOptions.DEFAULT, "db", "t");

        mapper.validateColumns(List.of("id", "name"));

        TypedRow row = TypedRow.builder()
                .putLong("id", 1L)
                .putString("name", "bob")
                .build();
        GenericRecord rec = mapper.toRecord(row);
        assertThat(rec.getField("id")).isEqualTo(1L);
        assertThat(rec.getField("name")).isEqualTo("bob");
        assertThat(rec.getField("tag")).isNull();
    }

    @Test
    void strict_typoedSourceColumnName_rejected() {
        // Realistic scenario: 'naem' instead of 'name'. Old code silently dropped
        // the value into NULL on the right column.
        RowToRecordMapper mapper = new RowToRecordMapper(
                SCHEMA, IcebergWriteOptions.DEFAULT, "db", "t");

        assertThatThrownBy(() -> mapper.validateColumns(List.of("id", "naem", "tag")))
                .isInstanceOf(IllegalArgumentException.class)
                // 'naem' is the extra one — strict mode trips on that first
                .hasMessageContaining("naem");
    }

    // ---- Lenient mode ----

    @Test
    void lenient_extraSourceColumn_droppedWithWarning() {
        IcebergWriteOptions lenient = new IcebergWriteOptions(false);
        RowToRecordMapper mapper = new RowToRecordMapper(
                SCHEMA, lenient, "db", "t");

        // Should NOT throw — extra columns are dropped silently in lenient mode.
        mapper.validateColumns(List.of("id", "name", "tag", "ghost"));

        TypedRow row = TypedRow.builder()
                .putLong("id", 5L)
                .putString("name", "carol")
                .putString("tag", "x")
                .putString("ghost", "ignored")
                .build();
        GenericRecord rec = mapper.toRecord(row);
        assertThat(rec.getField("id")).isEqualTo(5L);
        assertThat(rec.getField("name")).isEqualTo("carol");
        assertThat(rec.getField("tag")).isEqualTo("x");
        // Extra source column never appears in the record's schema —
        // only schema fields are emitted.
        assertThat(rec.struct().fields()).extracting("name")
                .containsExactly("id", "name", "tag");
    }

    @Test
    void lenient_missingRequiredColumn_stillRejected() {
        // Required-column gap is ALWAYS rejected — the underlying writer
        // would NPE/fail anyway, so failing fast is strictly better.
        IcebergWriteOptions lenient = new IcebergWriteOptions(false);
        RowToRecordMapper mapper = new RowToRecordMapper(
                SCHEMA, lenient, "db", "t");

        assertThatThrownBy(() -> mapper.validateColumns(List.of("id", "tag")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("required column(s) missing")
                .hasMessageContaining("name");
    }

    // ---- Per-row safety net ----

    @Test
    void perRow_missingRequiredField_rejected() {
        RowToRecordMapper mapper = new RowToRecordMapper(
                SCHEMA, IcebergWriteOptions.DEFAULT, "db", "t");
        mapper.validateColumns(List.of("id", "name", "tag"));

        // Validation passed — but a buggy upstream sends a row that drops
        // 'name'. Per-row check catches it before the writer NPEs.
        TypedRow row = TypedRow.builder().putLong("id", 1L).build();
        assertThatThrownBy(() -> mapper.toRecord(row))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing required column 'name'")
                .hasMessageContaining("field id 2");
    }

    @Test
    void toRecord_beforeValidate_throws() {
        RowToRecordMapper mapper = new RowToRecordMapper(
                SCHEMA, IcebergWriteOptions.DEFAULT, "db", "t");
        TypedRow row = TypedRow.builder().putLong("id", 1L).build();
        assertThatThrownBy(() -> mapper.toRecord(row))
                .isInstanceOf(IllegalStateException.class);
    }

    // ---- Options parsing ----

    @Test
    void options_defaultIsStrict() {
        assertThat(IcebergWriteOptions.DEFAULT.strictColumnMapping()).isTrue();
    }

    @Test
    void options_fromProperties_explicitFalseDisablesStrict() {
        IcebergWriteOptions opts = IcebergWriteOptions.fromProperties(
                java.util.Map.of(IcebergWriteOptions.PROP_STRICT_COLUMN_MAPPING, "false"));
        assertThat(opts.strictColumnMapping()).isFalse();
    }

    @Test
    void options_fromProperties_caseInsensitive() {
        IcebergWriteOptions opts = IcebergWriteOptions.fromProperties(
                java.util.Map.of(IcebergWriteOptions.PROP_STRICT_COLUMN_MAPPING, "FALSE"));
        assertThat(opts.strictColumnMapping()).isFalse();
    }

    @Test
    void options_fromProperties_unknownValueDefaultsToStrict() {
        IcebergWriteOptions opts = IcebergWriteOptions.fromProperties(
                java.util.Map.of(IcebergWriteOptions.PROP_STRICT_COLUMN_MAPPING, "yes"));
        assertThat(opts.strictColumnMapping()).isTrue();
    }

    @Test
    void options_fromProperties_nullOrEmpty_returnsDefault() {
        assertThat(IcebergWriteOptions.fromProperties(null)).isSameAs(IcebergWriteOptions.DEFAULT);
        assertThat(IcebergWriteOptions.fromProperties(java.util.Map.of())).isSameAs(IcebergWriteOptions.DEFAULT);
    }
}
