package org.pangrova.coordinator.result;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.pangrova.types.value.ColumnValue;
import org.pangrova.types.value.TypedRow;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Fills the Arrow vectors sent directly by the bounded control-plane result server. */
public final class CoordinatorResultWriter {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorResultWriter.class);

    private final BufferAllocator allocator;

    public CoordinatorResultWriter(BufferAllocator allocator) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
    }

    private static final long BATCH_LIMIT_BYTES = 128L * 1024 * 1024;

    /** Transfers the new batch to the caller only after all of its vectors have been filled. */
    public CoordinatorResultBatch buildBatch(List<TypedRow> rows, List<String> columnNames) {
        Objects.requireNonNull(rows, "rows");
        Objects.requireNonNull(columnNames, "columnNames");
        var schema = buildSchemaFromTypedRows(rows, columnNames);
        var child = allocator.newChildAllocator("control-result", 0, BATCH_LIMIT_BYTES);
        VectorSchemaRoot root = null;
        try {
            root = VectorSchemaRoot.create(schema, child);
            for (var vector : root.getFieldVectors()) {
                vector.setInitialCapacity(Math.max(1, rows.size()));
            }
            root.allocateNew();
            for (int col = 0; col < columnNames.size(); col++) {
                fillVector(root.getVector(col), rows, columnNames.get(col));
            }
            root.setRowCount(rows.size());
            return new CoordinatorResultBatch(root, child);
        } catch (RuntimeException | Error failure) {
            var allocated = root;
            org.pangrova.common.lifecycle.ResourceCloser.closeAll(failure, () -> {
                if (allocated != null) allocated.close();
            }, child::close);
            throw failure;
        }
    }

    // ── Schema builders ──

    private Schema buildSchemaFromTypedRows(List<TypedRow> rows, List<String> columnNames) {
        List<Field> arrowFields = new ArrayList<>(columnNames.size());
        TypedRow sample = rows.isEmpty() ? null : rows.get(0);
        for (String name : columnNames) {
            ArrowType type = sample == null || sample.get(name) == null
                    ? ArrowType.Utf8.INSTANCE : columnValueToArrowType(sample.get(name));
            arrowFields.add(new Field(name, FieldType.nullable(type), null));
        }
        return new Schema(arrowFields);
    }

    private void fillVector(org.apache.arrow.vector.FieldVector vector, List<TypedRow> rows, String column) {
        VectorSetter setter = resolveVectorSetter(vector);
        for (int row = 0; row < rows.size(); row++) {
            ColumnValue value = rows.get(row).get(column);
            if (value == null || value instanceof ColumnValue.NullValue) {
                vector.setNull(row);
                continue;
            }
            Object raw = ColumnValue.toRaw(value);
            if (raw == null) {
                vector.setNull(row);
            } else {
                setter.set(vector, row, raw);
            }
        }
    }

    @FunctionalInterface
    private interface VectorSetter {
        void set(org.apache.arrow.vector.FieldVector vector, int row, Object raw);
    }

    private static final Map<Class<?>, VectorSetter> VECTOR_SETTERS = Map.of(
            IntVector.class, (v, r, raw) -> ((IntVector) v).setSafe(r, ((Number) raw).intValue()),
            BigIntVector.class, (v, r, raw) -> ((BigIntVector) v).setSafe(r, ((Number) raw).longValue()),
            Float8Vector.class, (v, r, raw) -> ((Float8Vector) v).setSafe(r, ((Number) raw).doubleValue()),
            Float4Vector.class, (v, r, raw) -> ((Float4Vector) v).setSafe(r, ((Number) raw).floatValue()),
            BitVector.class, (v, r, raw) -> ((BitVector) v).setSafe(r, ((Boolean) raw) ? 1 : 0),
            VarBinaryVector.class, (v, r, raw) -> ((VarBinaryVector) v).setSafe(r, (byte[]) raw),
            VarCharVector.class, (v, r, raw) -> ((VarCharVector) v)
                    .setSafe(r, raw.toString().getBytes(StandardCharsets.UTF_8))
    );

    private static final VectorSetter FALLBACK_VARCHAR_SETTER =
            (v, r, raw) -> ((VarCharVector) v).setSafe(r, raw.toString().getBytes(StandardCharsets.UTF_8));

    private static final VectorSetter NULL_SETTER =
            (v, r, raw) -> v.setNull(r);

    private VectorSetter resolveVectorSetter(org.apache.arrow.vector.FieldVector vector) {
        VectorSetter setter = VECTOR_SETTERS.get(vector.getClass());
        if (setter != null) {
            return setter;
        }
        if (vector instanceof VarCharVector) {
            return FALLBACK_VARCHAR_SETTER;
        }
        LOG.warn("event=unsupported_vector_type vectorClass={} fallback=null",
                vector.getClass().getSimpleName());
        return NULL_SETTER;
    }

    // ── Type mapping ──

    private static final int DECIMAL_PRECISION = 38;

    private static final Map<Class<? extends ColumnValue>, java.util.function.Function<ColumnValue, ArrowType>>
            CV_TO_ARROW = Map.ofEntries(
            Map.entry(ColumnValue.IntValue.class, cv -> new ArrowType.Int(32, true)),
            Map.entry(ColumnValue.LongValue.class, cv -> new ArrowType.Int(64, true)),
            Map.entry(ColumnValue.FloatValue.class, cv -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
            Map.entry(ColumnValue.DoubleValue.class, cv -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            Map.entry(ColumnValue.BooleanValue.class, cv -> ArrowType.Bool.INSTANCE),
            Map.entry(ColumnValue.StringValue.class, cv -> ArrowType.Utf8.INSTANCE),
            Map.entry(ColumnValue.BytesValue.class, cv -> ArrowType.Binary.INSTANCE),
            Map.entry(ColumnValue.DecimalValue.class, cv -> new ArrowType.Decimal(DECIMAL_PRECISION, 10, 128)),
            Map.entry(ColumnValue.DateValue.class, cv -> new ArrowType.Date(DateUnit.DAY)),
            Map.entry(ColumnValue.TimestampValue.class, cv -> new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
            Map.entry(ColumnValue.VectorValue.class, cv -> {
                int dim = ((ColumnValue.VectorValue) cv).value().length;
                return new ArrowType.FixedSizeList(dim);
            })
    );

    private static ArrowType columnValueToArrowType(ColumnValue cv) {
        var mapper = CV_TO_ARROW.get(cv.getClass());
        return mapper != null ? mapper.apply(cv) : ArrowType.Utf8.INSTANCE;
    }
}
