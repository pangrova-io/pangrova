package org.pangrova.coordinator.property.dynamictable.refresh;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.AlphaChars;
import net.jqwik.api.constraints.StringLength;

import org.pangrova.coordinator.dynamictable.refresh.schema.SchemaEvolutionChecker;
import org.pangrova.coordinator.dynamictable.refresh.schema.RefreshSchemaChange;
import org.pangrova.coordinator.dynamictable.refresh.schema.SchemaCheckResult;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for {@link SchemaEvolutionChecker}.
 *
 * <p><b>Validates: Requirements 14</b></p>
 */
class SchemaEvolutionCheckerPropertyTest {

    private final SchemaEvolutionChecker checker = new SchemaEvolutionChecker();

    /** Property 1: Only column additions are always compatible. */
    @Property(tries = 10)
    void onlyColumnAdditions_alwaysCompatible(
            @ForAll @AlphaChars @StringLength(min = 1, max = 20) String colName,
            @ForAll @AlphaChars @StringLength(min = 1, max = 10) String colType) {
        Map<String, String> recorded = Map.of("id", "BIGINT");
        Map<String, String> current = new LinkedHashMap<>(recorded);
        current.put(colName.equals("id") ? colName + "_new" : colName, colType);

        SchemaCheckResult result = checker.check(recorded, current);

        assertThat(result.compatible()).isTrue();
    }

    /** Property 2: Any column removal is always incompatible. */
    @Property(tries = 10)
    void columnRemoval_alwaysIncompatible(
            @ForAll @AlphaChars @StringLength(min = 1, max = 20) String colName,
            @ForAll @AlphaChars @StringLength(min = 1, max = 10) String colType) {
        String safeName = colName.equals("id") ? colName + "_extra" : colName;
        Map<String, String> recorded = new LinkedHashMap<>();
        recorded.put("id", "BIGINT");
        recorded.put(safeName, colType);
        Map<String, String> current = Map.of("id", "BIGINT");

        SchemaCheckResult result = checker.check(recorded, current);

        assertThat(result.compatible()).isFalse();
        assertThat(result.changes()).anyMatch(c -> c instanceof RefreshSchemaChange.ColumnRemoved);
    }

    /** Property 3: Any type change is always incompatible. */
    @Property(tries = 10)
    void typeChange_alwaysIncompatible(
            @ForAll @AlphaChars @StringLength(min = 1, max = 10) String newType) {
        String safeNewType = newType.equals("BIGINT") ? newType + "X" : newType;
        Map<String, String> recorded = Map.of("id", "BIGINT");
        Map<String, String> current = Map.of("id", safeNewType);

        SchemaCheckResult result = checker.check(recorded, current);

        assertThat(result.compatible()).isFalse();
        assertThat(result.changes()).anyMatch(c -> c instanceof RefreshSchemaChange.ColumnTypeChanged);
    }

    /** Property 4: Identical schemas are always compatible with no changes. */
    @Property(tries = 10)
    void identicalSchemas_alwaysCompatible(
            @ForAll @AlphaChars @StringLength(min = 1, max = 20) String colName,
            @ForAll @AlphaChars @StringLength(min = 1, max = 10) String colType) {
        Map<String, String> schema = Map.of(colName, colType);

        SchemaCheckResult result = checker.check(schema, schema);

        assertThat(result.compatible()).isTrue();
        assertThat(result.changes()).isEmpty();
    }
}
