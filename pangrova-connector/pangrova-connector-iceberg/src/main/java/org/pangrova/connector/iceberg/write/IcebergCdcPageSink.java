package org.pangrova.connector.iceberg.write;

import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type.TypeID;
import org.pangrova.batch.api.page.Page;
import org.pangrova.connector.api.data.ConnectorCdcPageSink;
import org.pangrova.connector.api.enums.CdcRowKind;
import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandleFactory;
import org.pangrova.types.value.TypedRow;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Stages ordered before/after images as a single Iceberg v2 position-delete and append fragment. */
public final class IcebergCdcPageSink implements ConnectorCdcPageSink {
    private static final Set<TypeID> TYPES = Set.of(TypeID.BOOLEAN, TypeID.INTEGER, TypeID.LONG, TypeID.STRING);
    private static final long MAX_SCAN_BYTES = 64L * 1024 * 1024;
    private static final long MAX_SCAN_ROWS = 100_000;
    private static final int MAX_SCAN_FILES = 1_024;
    private static final int MAX_IMAGES = 20_000;

    private final Table table;
    private final IcebergOutputTableHandle handle;
    private final Schema schema;
    private final List<Change> changes = new ArrayList<>();
    private final List<DeleteFile> deleteFiles = new ArrayList<>();
    private boolean awaitingAfter;
    private boolean stopped;
    private boolean staged;

    IcebergCdcPageSink(Table table, IcebergOutputTableHandle handle) {
        requireSupported(table, handle);
        this.table = table;
        this.handle = handle;
        this.schema = IcebergTableHandleFactory.parseSchema(handle);
    }

    /** Checks the declaration supported by the mutation sink before source offsets are admitted. */
    public static void requireSupported(Table table, IcebergOutputTableHandle handle) {
        if (handle.formatVersion() != 2 || table.specs().values().stream().anyMatch(spec -> spec.isPartitioned())
                || table.schema().columns().stream().anyMatch(field -> !TYPES.contains(field.type().typeId()))) {
            throw new UnsupportedOperationException(
                    "Iceberg CDC mutations require unpartitioned v2 tables with boolean, integer or string columns");
        }
        if (!handle.schemaJson().equals(SchemaParser.toJson(table.schema()))
                || !handle.partitionSpecJson().equals(org.apache.iceberg.PartitionSpecParser.toJson(table.spec()))) {
            throw new IllegalStateException("Iceberg CDC target declaration changed after admission");
        }
    }

    @Override
    public void appendRow(TypedRow row, CdcRowKind kind) {
        if (stopped || changes.size() >= MAX_IMAGES) {
            throw new IllegalStateException("Iceberg CDC sink is closed or exceeds the row-image limit");
        }
        if (awaitingAfter != (kind == CdcRowKind.UPDATE_AFTER)) {
            throw new IllegalArgumentException("Iceberg CDC UPDATE requires adjacent before and after images");
        }
        awaitingAfter = kind == CdcRowKind.UPDATE_BEFORE;
        boolean removing = kind == CdcRowKind.DELETE || awaitingAfter;
        if (row.columnCount() != schema.columns().size()) {
            throw new IllegalArgumentException("Iceberg CDC requires a complete row image");
        }
        List<Object> key = schema.columns().stream().map(field -> {
            if (!row.containsColumn(field.name())) {
                throw new IllegalArgumentException("Iceberg CDC row image lacks " + field.name());
            }
            Object raw = row.getRaw(field.name());
            if (raw == null && field.isOptional()) return null;
            return switch (field.type().typeId()) {
                case BOOLEAN -> raw instanceof Boolean value ? value : invalidValue(field.name());
                case INTEGER -> raw instanceof Integer || raw instanceof Long
                        ? Math.toIntExact(((Number) raw).longValue()) : invalidValue(field.name());
                case LONG -> raw instanceof Integer || raw instanceof Long
                        ? ((Number) raw).longValue() : invalidValue(field.name());
                case STRING -> raw instanceof String value ? value : invalidValue(field.name());
                default -> throw new IllegalStateException("Unsupported Iceberg CDC column: " + field.name());
            };
        }).toList();
        changes.add(new Change(key, removing ? null : row));
    }

    private static Object invalidValue(String column) {
        throw new IllegalArgumentException("Iceberg CDC image has an invalid value for " + column);
    }

    @Override
    public void appendPage(Page page) {
        page.rows().forEach(row -> appendRow(row, CdcRowKind.INSERT));
    }

    @Override
    public byte[] finishWithMetadata() {
        if (stopped || awaitingAfter) {
            throw new IllegalStateException("Iceberg CDC sink cannot finish a partial or stopped batch");
        }
        stopped = true;
        var matches = new LinkedHashMap<List<Object>, List<Match>>();
        changes.forEach(change -> matches.computeIfAbsent(change.key(), ignored -> new ArrayList<>()));
        if (changes.stream().anyMatch(change -> change.after() == null)) {
            loadMatches(matches);
        }
        var deletions = applyChanges(matches);
        for (var entry : deletions.entrySet()) {
            deleteFiles.add(IcebergDeleteWriter.buildPositionDeleteFile(
                    table, entry.getKey(), entry.getValue().stream().sorted().toList()));
        }
        List<TypedRow> inserts = matches.values().stream().flatMap(List::stream)
                .map(Match::after).filter(java.util.Objects::nonNull).toList();
        String deletes = deleteFiles.stream().map(file -> ContentFileParser.toJson(file, table.spec()))
                .collect(Collectors.joining(",", "[", "]"));
        String append = stageInserts(inserts);
        staged = true;
        return ("{\"operation\":\"CDC_MERGE\",\"dataFiles\":" + append + ",\"deleteFiles\":" + deletes + "}")
                .getBytes(StandardCharsets.UTF_8);
    }

    private Map<String, List<Long>> applyChanges(Map<List<Object>, List<Match>> matches) {
        Map<String, List<Long>> deletes = new TreeMap<>();
        for (Change change : changes) {
            List<Match> candidates = matches.get(change.key());
            if (change.after() != null) {
                candidates.add(new Match(null, -1, change.after()));
                continue;
            }
            if (candidates.size() != 1) {
                throw new IllegalStateException("Iceberg CDC before-image must identify exactly one live row");
            }
            Match match = candidates.remove(0);
            if (match.path() != null) {
                deletes.computeIfAbsent(match.path(), ignored -> new ArrayList<>()).add(match.position());
            }
        }
        return deletes;
    }

    private void loadMatches(Map<List<Object>, List<Match>> matches) {
        if (handle.baseSnapshotId() == IcebergOutputTableHandle.NO_SNAPSHOT) return;
        var budget = new ScanBudget();
        try (var tasks = table.newScan().useSnapshot(handle.baseSnapshotId()).planFiles()) {
            for (var task : tasks) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new IllegalStateException("Iceberg CDC snapshot scan interrupted");
                }
                if (task.spec().specId() != table.spec().specId() || task.start() != 0
                        || task.file().format() != FileFormat.PARQUET) {
                    throw new UnsupportedOperationException("Iceberg CDC requires current-spec whole Parquet files");
                }
                budget.add(task.file());
                for (var delete : task.deletes()) {
                    if (delete.format() != FileFormat.PARQUET || delete.content() != FileContent.POSITION_DELETES) {
                        throw new UnsupportedOperationException("Iceberg CDC requires Parquet position deletes");
                    }
                    budget.add(delete);
                }
                String path = task.file().path().toString();
                IcebergCompactionExecutor.forEachSurvivingRow(table, schema, task,
                        IcebergCompactionExecutor.collectDeletedPositions(table, task), (row, position) -> {
                            List<Object> key = schema.columns().stream().map(field -> row.getField(field.name()))
                                    .map(value -> value instanceof CharSequence chars ? chars.toString() : value)
                                    .toList();
                            var candidates = matches.get(key);
                            if (candidates != null) candidates.add(new Match(path, position, null));
                        });
            }
        } catch (IOException failure) {
            throw new UncheckedIOException("Failed to scan the Iceberg CDC base snapshot", failure);
        }
    }

    private String stageInserts(List<TypedRow> inserts) {
        if (inserts.isEmpty()) return "[]";
        var context = new IcebergWriteContext(table, schema, handle.schemaName(), handle.tableName());
        try (var sink = new IcebergPageSink(context)) {
            try {
                sink.appendPage(new Page(schema.columns().stream().map(field -> field.name()).toList(), inserts));
                return new String(sink.finishWithMetadata(), StandardCharsets.UTF_8);
            } catch (RuntimeException | Error failure) {
                try {
                    sink.abort();
                } catch (RuntimeException cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        }
    }

    @Override
    public void abort() {
        stopped = true;
        if (!staged) {
            deleteFiles.forEach(file -> table.io().deleteFile(file.path().toString()));
            deleteFiles.clear();
        }
    }

    @Override
    public void close() {
        abort();
    }

    private record Change(List<Object> key, TypedRow after) {}

    private record Match(String path, long position, TypedRow after) {}

    private static final class ScanBudget {
        private long bytes;
        private long rows;
        private int files;

        private void add(ContentFile<?> file) {
            bytes = Math.addExact(bytes, file.fileSizeInBytes());
            rows = Math.addExact(rows, file.recordCount());
            if (++files > MAX_SCAN_FILES || rows > MAX_SCAN_ROWS || bytes > MAX_SCAN_BYTES) {
                throw new IllegalArgumentException("Iceberg CDC base exceeds the bounded mutation scan limit");
            }
        }
    }
}
