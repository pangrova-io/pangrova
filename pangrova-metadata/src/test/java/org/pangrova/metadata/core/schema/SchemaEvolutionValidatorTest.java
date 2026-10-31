package org.pangrova.metadata.core.schema;

import org.pangrova.metadata.api.object.ColumnMetadata;
import org.pangrova.metadata.api.object.ColumnType;
import org.pangrova.metadata.api.object.TableMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link SchemaEvolutionValidator} covering safe promotions,
 * nullability constraints, column existence checks, and edge cases.
 */
class SchemaEvolutionValidatorTest {

    private SchemaEvolutionValidator validator;

    @BeforeEach
    void setUp() {
        validator = new SchemaEvolutionValidator();
    }

    // ── Helpers ──

    private static TableMetadata tableWith(ColumnMetadata... columns) {
        return new TableMetadata(
                "cat", "sch", "tbl",
                List.of(columns),
                Map.of(), List.of(), Map.of(),
                Instant.now(), Instant.now());
    }

    private static ColumnMetadata col(String name, ColumnType type, boolean nullable) {
        return new ColumnMetadata(name, type, nullable, "", Map.of());
    }

    // ── AddColumn ──

    @Test
    void addColumnProducesWarningAndIsCompatible() {
        TableMetadata table = tableWith(col("id", ColumnType.of("INT"), false));
        List<SchemaChange> changes = List.of(
                new SchemaChange.AddColumn("name", ColumnType.of("VARCHAR"), true));

        SchemaValidationResult result = validator.validate(table, changes);

        assertThat(result.compatible()).isTrue();
        assertThat(result.warnings()).hasSize(1);
        assertThat(result.warnings().get(0)).contains("name");
    }

    // ── DropColumn ──

    @Test
    void dropExistingColumnProducesWarningAndIsCompatible() {
        TableMetadata table = tableWith(col("id", ColumnType.of("INT"), false));
        List<SchemaChange> changes = List.of(new SchemaChange.DropColumn("id"));

        SchemaValidationResult result = validator.validate(table, changes);

        assertThat(result.compatible()).isTrue();
        assertThat(result.warnings()).anyMatch(w -> w.contains("id"));
    }

    @Test
    void dropNonExistentColumnFails() {
        TableMetadata table = tableWith(col("id", ColumnType.of("INT"), false));
        List<SchemaChange> changes = List.of(new SchemaChange.DropColumn("ghost"));

        SchemaValidationResult result = validator.validate(table, changes);

        assertThat(result.compatible()).isFalse();
        assertThat(result.errors()).anyMatch(e -> e.contains("ghost") && e.contains("does not exist"));
    }

    // ── RenameColumn ──

    @Test
    void renameColumnSucceeds() {
        TableMetadata table = tableWith(col("old_name", ColumnType.of("INT"), false));
        List<SchemaChange> changes = List.of(new SchemaChange.RenameColumn("old_name", "new_name"));

        SchemaValidationResult result = validator.validate(table, changes);

        assertThat(result.compatible()).isTrue();
    }

    @Test
    void renameColumnFailsWhenSourceMissing() {
        TableMetadata table = tableWith(col("id", ColumnType.of("INT"), false));
        List<SchemaChange> changes = List.of(new SchemaChange.RenameColumn("missing", "new_name"));

        SchemaValidationResult result = validator.validate(table, changes);

        assertThat(result.compatible()).isFalse();
        assertThat(result.errors()).anyMatch(e -> e.contains("missing"));
    }

    @Test
    void renameColumnFailsWhenTargetAlreadyExists() {
        TableMetadata table = tableWith(
                col("a", ColumnType.of("INT"), false),
                col("b", ColumnType.of("INT"), false));
        List<SchemaChange> changes = List.of(new SchemaChange.RenameColumn("a", "b"));

        SchemaValidationResult result = validator.validate(table, changes);

        assertThat(result.compatible()).isFalse();
        assertThat(result.errors()).anyMatch(e -> e.contains("already exists"));
    }

    // ── ChangeType: safe promotions ──

    @Test
    void intToBigintIsSafe() {
        TableMetadata table = tableWith(col("val", ColumnType.of("INT"), true));
        List<SchemaChange> changes = List.of(
                new SchemaChange.ChangeType("val", ColumnType.of("BIGINT")));

        SchemaValidationResult result = validator.validate(table, changes);

        assertThat(result.compatible()).isTrue();
    }

    @Test
    void floatToDoubleIsSafe() {
        TableMetadata table = tableWith(col("val", ColumnType.of("FLOAT"), true));
        List<SchemaChange> changes = List.of(
                new SchemaChange.ChangeType("val", ColumnType.of("DOUBLE")));

        SchemaValidationResult result = validator.validate(table, changes);

        assertThat(result.compatible()).isTrue();
    }

    @Test
    void decimalWideningIsSafe() {
        TableMetadata table = tableWith(col("val", ColumnType.decimal(10, 2), true));
        List<SchemaChange> changes = List.of(
                new SchemaChange.ChangeType("val", ColumnType.decimal(18, 4)));

        SchemaValidationResult result = validator.validate(table, changes);

        assertThat(result.compatible()).isTrue();
    }

    @Test
    void decimalNarrowingFails() {
        TableMetadata table = tableWith(col("val", ColumnType.decimal(18, 4), true));
        List<SchemaChange> changes = List.of(
                new SchemaChange.ChangeType("val", ColumnType.decimal(10, 2)));

        SchemaValidationResult result = validator.validate(table, changes);

        assertThat(result.compatible()).isFalse();
        assertThat(result.errors()).anyMatch(e -> e.contains("Unsafe type change"));
    }

    @Test
    void varcharWideningIsSafe() {
        TableMetadata table = tableWith(col("val", ColumnType.varchar(50), true));
        List<SchemaChange> changes = List.of(
                new SchemaChange.ChangeType("val", ColumnType.varchar(100)));

        SchemaValidationResult result = validator.validate(table, changes);

        assertThat(result.compatible()).isTrue();
    }

    @Test
    void varcharNarrowingFails() {
        TableMetadata table = tableWith(col("val", ColumnType.varchar(100), true));
        List<SchemaChange> changes = List.of(
                new SchemaChange.ChangeType("val", ColumnType.varchar(50)));

        SchemaValidationResult result = validator.validate(table, changes);

        assertThat(result.compatible()).isFalse();
    }

    // ── ChangeType: unsafe promotions ──

    @Test
    void intToVarcharIsUnsafe() {
        TableMetadata table = tableWith(col("val", ColumnType.of("INT"), true));
        List<SchemaChange> changes = List.of(
                new SchemaChange.ChangeType("val", ColumnType.of("VARCHAR")));

        SchemaValidationResult result = validator.validate(table, changes);

        assertThat(result.compatible()).isFalse();
        assertThat(result.errors()).anyMatch(e -> e.contains("Unsafe type change"));
    }

    @Test
    void changeTypeOnNonExistentColumnFails() {
        TableMetadata table = tableWith(col("id", ColumnType.of("INT"), false));
        List<SchemaChange> changes = List.of(
                new SchemaChange.ChangeType("ghost", ColumnType.of("BIGINT")));

        SchemaValidationResult result = validator.validate(table, changes);

        assertThat(result.compatible()).isFalse();
        assertThat(result.errors()).anyMatch(e -> e.contains("ghost") && e.contains("does not exist"));
    }

    // ── ChangeNullability ──

    @Test
    void nonNullableToNullableIsAllowed() {
        TableMetadata table = tableWith(col("id", ColumnType.of("INT"), false));
        List<SchemaChange> changes = List.of(new SchemaChange.ChangeNullability("id", true));

        SchemaValidationResult result = validator.validate(table, changes);

        assertThat(result.compatible()).isTrue();
    }

    @Test
    void nullableToNonNullableIsRejected() {
        TableMetadata table = tableWith(col("id", ColumnType.of("INT"), true));
        List<SchemaChange> changes = List.of(new SchemaChange.ChangeNullability("id", false));

        SchemaValidationResult result = validator.validate(table, changes);

        assertThat(result.compatible()).isFalse();
        assertThat(result.errors()).anyMatch(e -> e.contains("nullable to non-nullable"));
    }

    @Test
    void changeNullabilityOnNonExistentColumnFails() {
        TableMetadata table = tableWith(col("id", ColumnType.of("INT"), false));
        List<SchemaChange> changes = List.of(new SchemaChange.ChangeNullability("ghost", true));

        SchemaValidationResult result = validator.validate(table, changes);

        assertThat(result.compatible()).isFalse();
        assertThat(result.errors()).anyMatch(e -> e.contains("ghost"));
    }

    // ── Multiple changes ──

    @Test
    void multipleValidChangesAllSucceed() {
        TableMetadata table = tableWith(
                col("id", ColumnType.of("INT"), false),
                col("val", ColumnType.of("FLOAT"), true));
        List<SchemaChange> changes = List.of(
                new SchemaChange.AddColumn("name", ColumnType.of("VARCHAR"), true),
                new SchemaChange.ChangeType("val", ColumnType.of("DOUBLE")));

        SchemaValidationResult result = validator.validate(table, changes);

        assertThat(result.compatible()).isTrue();
    }

    @Test
    void mixedValidAndInvalidChangesReportsAllErrors() {
        TableMetadata table = tableWith(
                col("id", ColumnType.of("INT"), false),
                col("val", ColumnType.of("INT"), true));
        List<SchemaChange> changes = List.of(
                new SchemaChange.DropColumn("ghost"),
                new SchemaChange.ChangeType("val", ColumnType.of("VARCHAR")));

        SchemaValidationResult result = validator.validate(table, changes);

        assertThat(result.compatible()).isFalse();
        assertThat(result.errors()).hasSize(2);
    }

    // ── Edge cases ──

    @Test
    void emptyChangesListIsCompatible() {
        TableMetadata table = tableWith(col("id", ColumnType.of("INT"), false));

        SchemaValidationResult result = validator.validate(table, List.of());

        assertThat(result.compatible()).isTrue();
        assertThat(result.errors()).isEmpty();
    }
}
