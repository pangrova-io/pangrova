package org.pangrova.connector.paimon;

import org.pangrova.spi.connector.metadata.Constraint;
import org.pangrova.spi.connector.metadata.Domain;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Converts SPI Constraint domains into Paimon Predicates for partition/bucket pruning.
 */
final class PaimonFilterBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonFilterBuilder.class);

    private PaimonFilterBuilder() {}

    /**
     * Builds a Paimon Predicate from SPI constraint domains.
     * Returns null if no applicable predicates can be derived.
     */
    static Predicate build(RowType rowType, Constraint constraint) {
        List<Predicate> predicates = collectPredicates(rowType, constraint);
        if (predicates == null || predicates.isEmpty()) {
            return null;
        }
        return predicates.size() == 1
                ? predicates.get(0)
                : PredicateBuilder.and(predicates);
    }

    /**
     * Collects applicable predicates from constraint domains.
     * Returns null if constraint is empty or on error.
     */
    private static List<Predicate> collectPredicates(RowType rowType, Constraint constraint) {
        if (constraint == null || constraint == Constraint.EMPTY
                || constraint.getDomains().isEmpty()) {
            return null;
        }
        try {
            PredicateBuilder builder = new PredicateBuilder(rowType);
            List<Predicate> predicates = new ArrayList<>();
            for (var entry : constraint.getDomains().entrySet()) {
                addPredicateIfApplicable(builder, rowType, entry, predicates);
            }
            return predicates;
        } catch (Exception e) {
            LOG.warn("event=paimon_predicate_build_failed error={}", e.getMessage());
            return null;
        }
    }

    private static void addPredicateIfApplicable(
            PredicateBuilder builder, RowType rowType,
            Map.Entry<ColumnHandle, Domain> entry, List<Predicate> predicates) {
        String columnName = extractColumnName(entry.getKey());
        int fieldIndex = findFieldIndex(rowType, columnName);
        if (fieldIndex < 0) {
            return;
        }
        DataType fieldType = rowType.getFields().get(fieldIndex).type();
        Predicate pred = domainToPredicate(builder, fieldIndex, entry.getValue(), fieldType);
        if (pred != null) {
            predicates.add(pred);
        }
    }

    private static Predicate domainToPredicate(
            PredicateBuilder builder, int fieldIndex, Domain domain, DataType fieldType) {
        if (domain instanceof Domain.IsNull) {
            return builder.isNull(fieldIndex);
        }
        if (domain instanceof Domain.IsNotNull) {
            return builder.isNotNull(fieldIndex);
        }
        return convertBoundedDomain(builder, fieldIndex, domain, fieldType);
    }

    private static Predicate convertBoundedDomain(
            PredicateBuilder builder, int fieldIndex, Domain domain, DataType fieldType) {
        if (domain instanceof Domain.Range range) {
            return rangeToPredicate(builder, fieldIndex, range, fieldType);
        }
        if (domain instanceof Domain.InList inList) {
            List<Object> coerced = inList.values().stream()
                    .map(v -> coerceToPaimonType(v, fieldType))
                    .collect(Collectors.toCollection(ArrayList::new));
            return builder.in(fieldIndex, coerced);
        }
        return null;
    }

    private static Predicate rangeToPredicate(
            PredicateBuilder builder, int fieldIndex, Domain.Range range, DataType fieldType) {
        if (isEqualityRange(range)) {
            return builder.equal(fieldIndex, coerceToPaimonType(range.low(), fieldType));
        }
        List<Predicate> parts = collectRangeBounds(builder, fieldIndex, range, fieldType);
        return combineParts(parts);
    }

    private static boolean isEqualityRange(Domain.Range range) {
        return range.low() != null && range.high() != null
                && range.low().equals(range.high())
                && range.lowInclusive() && range.highInclusive();
    }

    private static List<Predicate> collectRangeBounds(
            PredicateBuilder builder, int fieldIndex, Domain.Range range, DataType fieldType) {
        List<Predicate> parts = new ArrayList<>(2);
        if (range.low() != null) {
            Object low = coerceToPaimonType(range.low(), fieldType);
            parts.add(range.lowInclusive()
                    ? builder.greaterOrEqual(fieldIndex, low)
                    : builder.greaterThan(fieldIndex, low));
        }
        if (range.high() != null) {
            Object high = coerceToPaimonType(range.high(), fieldType);
            parts.add(range.highInclusive()
                    ? builder.lessOrEqual(fieldIndex, high)
                    : builder.lessThan(fieldIndex, high));
        }
        return parts;
    }

    private static Object coerceToPaimonType(Object value, DataType fieldType) {
        if (value == null) {
            return null;
        }
        if (fieldType.getTypeRoot().getFamilies().contains(DataTypeFamily.CHARACTER_STRING)) {
            if (value instanceof BinaryString) {
                return value;
            }
            return BinaryString.fromString(value.toString());
        }
        return value;
    }

    private static Predicate combineParts(List<Predicate> parts) {
        if (parts.isEmpty()) {
            return null;
        }
        return parts.size() == 1 ? parts.get(0) : PredicateBuilder.and(parts);
    }

    private static String extractColumnName(ColumnHandle handle) {
        return handle.columnName();
    }

    private static int findFieldIndex(RowType rowType, String columnName) {
        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equals(columnName)) {
                return i;
            }
        }
        return -1;
    }
}
