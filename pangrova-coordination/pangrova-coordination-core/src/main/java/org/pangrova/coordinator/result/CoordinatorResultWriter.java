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
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.pangrova.connector.spi.data.value.ColumnValue;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.connector.spi.data.page.NativePage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts bounded control-plane statement rows to columnar IPC bytes.
 *
 * <p>Supports three result paths:</p>
 * <ul>
 *   <li>Direct VectorSchemaRoot serialization (zero-copy from columnar-native sources)</li>
 *   <li>NativePage passthrough (pre-serialized IPC)</li>
 *   <li>DDL/DML: TypedRow list → columnar fill → IPC</li>
 * </ul>
 *
 * <p>The allocator is injected from outside to comply with runtime resource
 * governance — no private allocator pools.</p>
 */
public final class CoordinatorResultWriter {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorResultWriter.class);

    private final BufferAllocator allocator;

    public CoordinatorResultWriter(BufferAllocator allocator) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
    }

    /**
     * Serializes a pre-built VectorSchemaRoot directly to columnar IPC bytes.
     * This is the zero-overhead path for columnar-native result sources.
     */
    public byte[] serializeFromRoot(VectorSchemaRoot root) {
        Objects.requireNonNull(root, "root");
        return serializeRoot(root);
    }

    /**
     * Serializes an ArrowPage to columnar IPC bytes.
     * Delegates to the ArrowPage's underlying IPC data if already serialized.
     */
    public byte[] serializeFromNativePage(NativePage page) {
        Objects.requireNonNull(page, "page");
        return page.serializedBatch();
    }

    /**
     * Converts a list of TypedRow to columnar IPC bytes.
     * Column order follows the provided columnNames list.
     * Pre-extracts column data into a column-major array to avoid per-cell map lookups.
     */
    public byte[] serializeTypedRows(List<TypedRow> rows, List<String> columnNames) {
        Objects.requireNonNull(rows, "rows");
        Objects.requireNonNull(columnNames, "columnNames");
        Schema schema = buildSchemaFromTypedRows(rows, columnNames);
        int colCount = columnNames.size();
        int rowCount = rows.size();
        Object[][] columnData = new Object[colCount][rowCount];
        for (int r = 0; r < rowCount; r++) {
            TypedRow row = rows.get(r);
            for (int c = 0; c < colCount; c++) {
                columnData[c][r] = row.get(columnNames.get(c));
            }
        }
        return writeSerializedBatch(schema, colCount, rowCount,
                (col, row) -> columnData[col][row]);
    }

    // ── Schema builders ──

    private Schema buildSchemaFromTypedRows(List<TypedRow> rows, List<String> columnNames) {
        List<Field> arrowFields = new ArrayList<>(columnNames.size());
        TypedRow sample = rows.isEmpty() ? null : rows.get(0);
        for (String name : columnNames) {
            ArrowType type = (sample != null)
                    ? columnValueToArrowType(sample.get(name))
                    : ArrowType.Utf8.INSTANCE;
            arrowFields.add(new Field(name, FieldType.nullable(type), null));
        }
        return new Schema(arrowFields);
    }

    // ── IPC writer ──

    private static final long CHILD_ALLOCATOR_LIMIT = 128L * 1024 * 1024; // 128 MB

    private byte[] writeSerializedBatch(Schema schema, int colCount, int rowCount,
                                 CellAccessor accessor) {
        try (BufferAllocator child = allocator.newChildAllocator("result-writer", 0, CHILD_ALLOCATOR_LIMIT);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, child)) {
            root.allocateNew();
            for (int col = 0; col < colCount; col++) {
                batchFillVector(root, col, rowCount, accessor);
            }
            root.setRowCount(rowCount);
            return serializeRoot(root);
        }
    }

    private static final int INITIAL_IPC_BUFFER_SIZE = 64 * 1024;

    private static final ThreadLocal<ByteArrayOutputStream> REUSABLE_BAOS =
            ThreadLocal.withInitial(() -> new ByteArrayOutputStream(INITIAL_IPC_BUFFER_SIZE));

    private byte[] serializeRoot(VectorSchemaRoot root) {
        ByteArrayOutputStream baos = REUSABLE_BAOS.get();
        baos.reset();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
            writer.start();
            writer.writeBatch();
            writer.end();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize columnar IPC", e);
        }
        return baos.toByteArray();
    }

    // ── Batch columnar vector fill ──

    private void batchFillVector(VectorSchemaRoot root, int col, int rowCount,
                                 CellAccessor accessor) {
        var vector = root.getFieldVectors().get(col);
        VectorSetter setter = resolveVectorSetter(vector);
        for (int row = 0; row < rowCount; row++) {
            Object value = accessor.get(col, row);
            if (value == null || value instanceof ColumnValue.NullValue) {
                vector.setNull(row);
                continue;
            }
            Object raw = (value instanceof ColumnValue cv) ? ColumnValue.toRaw(cv) : value;
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

    /** Functional interface for accessing cell values by (col, row) index. */
    @FunctionalInterface
    private interface CellAccessor {
        Object get(int col, int row);
    }
}
