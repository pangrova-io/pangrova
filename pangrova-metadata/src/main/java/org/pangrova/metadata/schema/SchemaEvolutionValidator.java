package org.pangrova.metadata.schema;

import org.pangrova.metadata.api.object.ColumnMetadata;
import org.pangrova.metadata.api.object.ColumnType;
import org.pangrova.metadata.api.object.TableMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Validates schema evolution changes against safe promotion rules and nullability constraints.
 * Supports configurable strictness levels to control which changes are allowed.
 */
public class SchemaEvolutionValidator {

    private static final Map<String, Set<String>> SAFE_PROMOTIONS = Map.of(
            "INT", Set.of("BIGINT"),
            "FLOAT", Set.of("DOUBLE"),
            "DECIMAL", Set.of("DECIMAL"),
            "VARCHAR", Set.of("VARCHAR")
    );

    private final StrictnessLevel strictnessLevel;

    public SchemaEvolutionValidator() {
        this(StrictnessLevel.STANDARD);
    }

    public SchemaEvolutionValidator(StrictnessLevel strictnessLevel) {
        this.strictnessLevel = strictnessLevel;
    }

    /** Validates the given schema changes against the current table metadata. */
    public SchemaValidationResult validate(TableMetadata current, List<SchemaChange> changes) {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        for (SchemaChange change : changes) {
            validateChange(current, change, errors, warnings);
        }
        return errors.isEmpty()
                ? SchemaValidationResult.success(warnings)
                : SchemaValidationResult.failure(errors, warnings);
    }

    public StrictnessLevel strictnessLevel() {
        return strictnessLevel;
    }

    private void validateChange(
            TableMetadata current, SchemaChange change,
            List<String> errors, List<String> warnings) {
        if (change instanceof SchemaChange.AddColumn add) {
            validateAddColumn(current, add, warnings);
        } else if (change instanceof SchemaChange.DropColumn drop) {
            validateDropColumn(current, drop, errors, warnings);
        } else if (change instanceof SchemaChange.RenameColumn rename) {
            validateRenameColumn(current, rename, errors);
        } else {
            validateTypeOrNullability(current, change, errors);
        }
    }

    private void validateTypeOrNullability(
            TableMetadata current, SchemaChange change, List<String> errors) {
        if (change instanceof SchemaChange.ChangeType ct) {
            validateChangeType(current, ct, errors);
        } else if (change instanceof SchemaChange.ChangeNullability cn) {
            validateChangeNullability(current, cn, errors);
        }
    }

    private void validateAddColumn(
            TableMetadata current, SchemaChange.AddColumn add, List<String> warnings) {
        warnings.add("Adding column '" + add.name() + "'");
    }

    private void validateDropColumn(
            TableMetadata current, SchemaChange.DropColumn drop,
            List<String> errors, List<String> warnings) {
        Optional<ColumnMetadata> col = findColumn(current, drop.name());
        if (col.isEmpty()) {
            errors.add("Column '" + drop.name() + "' does not exist");
            return;
        }
        if (strictnessLevel == StrictnessLevel.STRICT) {
            errors.add("Column drop not allowed in STRICT mode: '" + drop.name() + "'");
            return;
        }
        warnings.add("Dropping column '" + drop.name() + "'");
    }

    private void validateRenameColumn(
            TableMetadata current, SchemaChange.RenameColumn rename, List<String> errors) {
        if (findColumn(current, rename.oldName()).isEmpty()) {
            errors.add("Column '" + rename.oldName() + "' does not exist");
            return;
        }
        if (findColumn(current, rename.newName()).isPresent()) {
            errors.add("Column '" + rename.newName() + "' already exists");
            return;
        }
        if (strictnessLevel == StrictnessLevel.STRICT) {
            errors.add("Column rename not allowed in STRICT mode: '"
                    + rename.oldName() + "' -> '" + rename.newName() + "'");
        }
    }

    private void validateChangeType(
            TableMetadata current, SchemaChange.ChangeType ct, List<String> errors) {
        Optional<ColumnMetadata> col = findColumn(current, ct.name());
        if (col.isEmpty()) {
            errors.add("Column '" + ct.name() + "' does not exist");
            return;
        }
        if (!isPromotionSafe(col.get().type(), ct.newType())) {
            errors.add("Unsafe type change for '" + ct.name()
                    + "': " + col.get().type().baseType() + " -> " + ct.newType().baseType());
        }
    }

    private void validateChangeNullability(
            TableMetadata current, SchemaChange.ChangeNullability cn, List<String> errors) {
        Optional<ColumnMetadata> col = findColumn(current, cn.name());
        if (col.isEmpty()) {
            errors.add("Column '" + cn.name() + "' does not exist");
            return;
        }
        // nullable -> non-nullable is rejected (data loss risk)
        if (col.get().nullable() && !cn.nullable()) {
            errors.add("Cannot change '" + cn.name() + "' from nullable to non-nullable");
        }
    }

    private boolean isPromotionSafe(ColumnType from, ColumnType to) {
        Set<String> allowed = SAFE_PROMOTIONS.get(from.baseType());
        if (allowed == null || !allowed.contains(to.baseType())) {
            return false;
        }
        return isParameterWidening(from, to);
    }

    private boolean isParameterWidening(ColumnType from, ColumnType to) {
        if ("DECIMAL".equals(from.baseType()) && "DECIMAL".equals(to.baseType())) {
            return to.precision() >= from.precision() && to.scale() >= from.scale();
        }
        if ("VARCHAR".equals(from.baseType()) && "VARCHAR".equals(to.baseType())) {
            return to.length() >= from.length();
        }
        return true;
    }

    private Optional<ColumnMetadata> findColumn(TableMetadata metadata, String name) {
        return metadata.columns().stream()
                .filter(c -> c.name().equals(name))
                .findFirst();
    }
}
