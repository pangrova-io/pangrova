package org.pangrova.connector.iceberg;

import org.apache.iceberg.NullOrder;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.ZOrderByteUtils;
import org.pangrova.connector.api.enums.ClusteringMode;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/** Stores one sort declaration and computes the physical keys for its rewrite. */
record IcebergClusteringOrder(ClusteringMode mode, List<NestedField> fields, String generation) {
    private static final String MODE = "pangrova.clustering.mode";
    private static final String FIELDS = "pangrova.clustering.fields";
    private static final String GENERATION = "pangrova.clustering.generation";
    private static final int MAX_COLUMNS = 8;
    private static final int KEY_BYTES = Long.BYTES;
    private static final Set<TypeID> INTERLEAVE_TYPES = Set.of(
            TypeID.INTEGER, TypeID.LONG, TypeID.FLOAT, TypeID.DOUBLE, TypeID.STRING, TypeID.BOOLEAN);

    static void set(Table table, List<String> names, ClusteringMode mode) {
        if (names.isEmpty() || names.size() > MAX_COLUMNS || new HashSet<>(names).size() != names.size()) {
            throw new IllegalArgumentException("Clustering requires one to eight distinct columns");
        }
        if (mode == ClusteringMode.INTERLEAVE && names.size() < 2) {
            throw new IllegalArgumentException("INTERLEAVE requires at least two columns");
        }
        var fields = names.stream().map(name -> column(table, name, mode)).toList();
        var transaction = table.newTransaction();
        var replacement = transaction.replaceSortOrder();
        if (mode == ClusteringMode.LINEAR) {
            names.forEach(name -> replacement.asc(name, NullOrder.NULLS_LAST));
        }
        replacement.commit();
        transaction.updateProperties()
                .set(MODE, mode.name())
                .set(FIELDS, fields.stream().map(field -> Integer.toString(field.fieldId()))
                        .collect(Collectors.joining(",")))
                .set(GENERATION, UUID.randomUUID().toString())
                .commit();
        transaction.commitTransaction();
    }

    private static NestedField column(Table table, String name, ClusteringMode mode) {
        var field = table.schema().findField(name);
        if (field == null || !table.schema().columns().contains(field) || !field.type().isPrimitiveType()) {
            throw new IllegalArgumentException("Clustering column must be a top-level primitive: " + name);
        }
        requireSupportedType(field, mode);
        return field;
    }

    private static void requireSupportedType(NestedField field, ClusteringMode mode) {
        if (mode == ClusteringMode.LINEAR) {
            Comparators.forType(field.type().asPrimitiveType());
        } else if (!INTERLEAVE_TYPES.contains(field.type().typeId())) {
            throw new IllegalArgumentException("INTERLEAVE does not support " + field.type());
        }
    }

    static Optional<IcebergClusteringOrder> read(Table table) {
        var properties = table.properties();
        String configuredMode = properties.get(MODE);
        if (configuredMode == null) return Optional.empty();
        String fieldIds = properties.get(FIELDS);
        String generation = properties.get(GENERATION);
        if (fieldIds == null || generation == null || generation.isBlank()) {
            throw new IllegalStateException("Iceberg clustering declaration is incomplete");
        }
        var mode = ClusteringMode.valueOf(configuredMode);
        var fields = Arrays.stream(fieldIds.split(","))
                .map(Integer::parseInt)
                .map(table.schema()::findField)
                .toList();
        if (fields.stream().anyMatch(field -> field == null || !table.schema().columns().contains(field)
                || !field.type().isPrimitiveType())) {
            throw new IllegalStateException("Clustering declaration references an unavailable column");
        }
        fields.forEach(field -> requireSupportedType(field, mode));
        var order = new IcebergClusteringOrder(mode, fields, generation);
        if (order.mode() == ClusteringMode.LINEAR && !order.matchesSortOrder(table.sortOrder())) {
            throw new IllegalStateException("Iceberg sort order changed since clustering was configured");
        }
        return Optional.of(order);
    }

    private boolean matchesSortOrder(SortOrder order) {
        return order.fields().stream().allMatch(field -> field.transform().isIdentity()
                && field.direction() == SortDirection.ASC && field.nullOrder() == NullOrder.NULLS_LAST)
                && order.fields().stream().map(field -> field.sourceId()).toList()
                        .equals(fields.stream().map(NestedField::fieldId).toList());
    }

    List<String> columnNames() {
        return fields.stream().map(NestedField::name).toList();
    }

    void linear(List<Record> rows) {
        Comparator<Record> order = (left, right) -> 0;
        for (var field : fields) {
            if (field.type().typeId() == TypeID.FIXED) {
                order = order.thenComparing(row -> (byte[]) row.getField(field.name()),
                        Comparator.nullsLast(Arrays::compareUnsigned));
                continue;
            }
            Comparator<Object> values = Comparator.nullsLast(Comparators.forType(field.type().asPrimitiveType()));
            order = order.thenComparing(row -> row.getField(field.name()), values);
        }
        rows.sort(order);
    }

    void interleave(List<Record> rows) {
        var keys = new IdentityHashMap<Record, byte[]>(rows.size());
        for (Record row : rows) {
            byte[][] axes = new byte[fields.size()][];
            for (int index = 0; index < fields.size(); index++) {
                var field = fields.get(index);
                axes[index] = orderedAxis(row.getField(field.name()), field.type().typeId());
            }
            int width = axes.length * (KEY_BYTES + 1);
            keys.put(row, ZOrderByteUtils.interleaveBits(axes, width, ByteBuffer.allocate(width)));
        }
        rows.sort(Comparator.comparing(keys::get, Arrays::compareUnsigned));
    }

    private static byte[] orderedAxis(Object value, TypeID type) {
        var axis = ByteBuffer.allocate(KEY_BYTES + 1);
        if (value == null) {
            axis.put((byte) 1);
            return axis.array();
        }
        axis.put((byte) 0);
        switch (type) {
            case BOOLEAN -> axis.put((byte) (Boolean.TRUE.equals(value) ? 1 : 0));
            case INTEGER, LONG -> axis.putLong(((Number) value).longValue() ^ Long.MIN_VALUE);
            case FLOAT, DOUBLE -> {
                long bits = Double.doubleToLongBits(((Number) value).doubleValue());
                axis.putLong(bits < 0 ? ~bits : bits ^ Long.MIN_VALUE);
            }
            case STRING -> {
                byte[] text = value.toString().getBytes(StandardCharsets.UTF_8);
                axis.put(text, 0, Math.min(KEY_BYTES, text.length));
            }
            default -> throw new IllegalStateException("Unsupported INTERLEAVE key type: " + type);
        }
        return axis.array();
    }
}
