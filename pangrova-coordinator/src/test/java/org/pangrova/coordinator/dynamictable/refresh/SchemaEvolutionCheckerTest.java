package org.pangrova.coordinator.dynamictable.refresh;

import org.pangrova.coordinator.dynamictable.refresh.schema.RefreshSchemaChange;
import org.pangrova.coordinator.dynamictable.refresh.schema.SchemaCheckResult;
import org.pangrova.coordinator.dynamictable.refresh.schema.SchemaEvolutionChecker;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SchemaEvolutionChecker}.
 *
 * <p><b>Validates: Requirements 14</b></p>
 */
class SchemaEvolutionCheckerTest {

    private final SchemaEvolutionChecker checker = new SchemaEvolutionChecker();

    @Test
    void noChanges_compatible() {
        Map<String, String> schema = Map.of("id", "BIGINT", "name", "VARCHAR");
        SchemaCheckResult result = checker.check(schema, schema);
        assertThat(result.compatible()).isTrue();
        assertThat(result.changes()).isEmpty();
    }

    @Test
    void columnAdded_compatible() {
        Map<String, String> recorded = Map.of("id", "BIGINT");
        Map<String, String> current = new LinkedHashMap<>(recorded);
        current.put("name", "VARCHAR");

        SchemaCheckResult result = checker.check(recorded, current);

        assertThat(result.compatible()).isTrue();
        assertThat(result.changes()).hasSize(1);
        assertThat(result.changes().get(0)).isInstanceOf(RefreshSchemaChange.ColumnAdded.class);
        RefreshSchemaChange.ColumnAdded added = (RefreshSchemaChange.ColumnAdded) result.changes().get(0);
        assertThat(added.columnName()).isEqualTo("name");
        assertThat(added.columnType()).isEqualTo("VARCHAR");
    }

    @Test
    void columnRemoved_incompatible() {
        Map<String, String> recorded = new LinkedHashMap<>();
        recorded.put("id", "BIGINT");
        recorded.put("name", "VARCHAR");
        Map<String, String> current = Map.of("id", "BIGINT");

        SchemaCheckResult result = checker.check(recorded, current);

        assertThat(result.compatible()).isFalse();
        assertThat(result.changes()).anyMatch(c ->
                c instanceof RefreshSchemaChange.ColumnRemoved r && r.columnName().equals("name"));
    }

    @Test
    void columnTypeChanged_incompatible() {
        Map<String, String> recorded = Map.of("id", "BIGINT");
        Map<String, String> current = Map.of("id", "VARCHAR");

        SchemaCheckResult result = checker.check(recorded, current);

        assertThat(result.compatible()).isFalse();
        assertThat(result.changes()).anyMatch(c ->
                c instanceof RefreshSchemaChange.ColumnTypeChanged tc
                        && tc.oldType().equals("BIGINT")
                        && tc.newType().equals("VARCHAR"));
    }

    @Test
    void mixedAddAndRemove_incompatible() {
        Map<String, String> recorded = new LinkedHashMap<>();
        recorded.put("id", "BIGINT");
        recorded.put("old_col", "INT");
        Map<String, String> current = new LinkedHashMap<>();
        current.put("id", "BIGINT");
        current.put("new_col", "VARCHAR");

        SchemaCheckResult result = checker.check(recorded, current);

        assertThat(result.compatible()).isFalse();
        assertThat(result.changes()).hasSize(2);
    }

    @Test
    void mixedAddAndTypeChange_incompatible() {
        Map<String, String> recorded = Map.of("id", "BIGINT");
        Map<String, String> current = new LinkedHashMap<>();
        current.put("id", "VARCHAR");
        current.put("new_col", "INT");

        SchemaCheckResult result = checker.check(recorded, current);

        assertThat(result.compatible()).isFalse();
    }

    @Test
    void multipleAdditions_compatible() {
        Map<String, String> recorded = Map.of("id", "BIGINT");
        Map<String, String> current = new LinkedHashMap<>(recorded);
        current.put("name", "VARCHAR");
        current.put("age", "INT");
        current.put("email", "VARCHAR");

        SchemaCheckResult result = checker.check(recorded, current);

        assertThat(result.compatible()).isTrue();
        assertThat(result.changes()).hasSize(3);
        assertThat(result.changes()).allMatch(c -> c instanceof RefreshSchemaChange.ColumnAdded);
    }

    @Test
    void emptySchemas_compatible() {
        SchemaCheckResult result = checker.check(Map.of(), Map.of());
        assertThat(result.compatible()).isTrue();
        assertThat(result.changes()).isEmpty();
    }

    @Test
    void nullRecordedSchema_throwsNpe() {
        org.junit.jupiter.api.Assertions.assertThrows(NullPointerException.class,
                () -> checker.check(null, Map.of()));
    }

    @Test
    void nullCurrentSchema_throwsNpe() {
        org.junit.jupiter.api.Assertions.assertThrows(NullPointerException.class,
                () -> checker.check(Map.of(), null));
    }
}
