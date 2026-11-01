package org.pangrova.coordinator.query.statement;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.data.ConnectorPageSource;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.spi.connector.data.page.NativePage;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorSplit;
import org.pangrova.spi.connector.metadata.ConnectorSplitSource;
import org.pangrova.spi.connector.metadata.Constraint;
import org.pangrova.spi.connector.metadata.DynamicFilter;
import org.pangrova.spi.connector.metadata.SplitProperties;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes the engine-level {@code warmup_table} procedure (G-CACHE-06).
 *
 * <p>This is a connector-agnostic implementation invoked through {@code CALL
 * <catalog>.warmup_table(...)}. It mirrors Trino's {@code CACHE SELECT}: list
 * the data files (splits) for a table, drive each split through the connector
 * page-source path (no projection beyond the table's declared columns, no
 * result materialization), and return a single summary row reporting splits
 * processed, rows touched, bytes pre-fetched, and elapsed time.</p>
 *
 * <p><b>Cache layers warmed:</b>
 * <ul>
 *   <li>Connector-side metadata caches (Iceberg/Paimon manifest readers,
 *       table-handle cache).</li>
 *   <li>OS page cache for the underlying data files (the page-source reads
 *       file bytes through the same I/O layer used by real scans).</li>
 *   <li>Any in-process file/block cache layered under the connector page
 *       source (e.g. Hadoop {@code BufferedFSInputStream}, S3A read-ahead).</li>
 * </ul>
 *
 * <p><b>Out of scope (intentional):</b>
 * <ul>
 *   <li>Native data cache priming — that lives on workers
 *       and would require dispatching synthetic scan tasks. A future
 *       enhancement (overlaps with F-CHAIN-CACHE-02 cache-locality
 *       scheduling) can route warmup through the worker scan path.</li>
 *   <li>Hot-table pinning. Pinning requires deeper integration into native
 *       data cache eviction policy and is deliberately deferred.</li>
 * </ul>
 *
 * <p><b>Argument shapes</b> accepted (matching the existing connector
 * procedure conventions in {@code CallProcedureTask}):
 * <ul>
 *   <li>1 arg — {@code arg0 = "schema.table"} (matches Paimon procedures).</li>
 *   <li>2 args — {@code arg0 = "schema"}, {@code arg1 = "table"}.</li>
 * </ul>
 *
 * <p>Failure semantics: any single-split failure is logged and counted but
 * does not abort the warmup. The summary row reports {@code splits_failed}
 * so callers can detect partial warmth. If <i>every</i> split fails the
 * procedure throws {@link PangrovaException} so callers see the error.</p>
 */
public final class TableWarmupExecutor {

    /** Public procedure name (case-insensitive at the call site). */
    public static final String PROCEDURE_NAME = "warmup_table";

    private static final Logger LOG = LoggerFactory.getLogger(TableWarmupExecutor.class);

    /** Hard cap on how many splits a single warmup invocation will read. */
    private static final int DEFAULT_MAX_SPLITS = 100_000;

    /** Page source batch fetch size (passed to the split source). */
    private static final int SPLIT_BATCH_SIZE = 256;

    private final CatalogRegistry catalogRegistry;
    private final int maxSplits;

    public TableWarmupExecutor(CatalogRegistry catalogRegistry) {
        this(catalogRegistry, DEFAULT_MAX_SPLITS);
    }

    /** Test-friendly constructor allowing the split cap to be overridden. */
    public TableWarmupExecutor(CatalogRegistry catalogRegistry, int maxSplits) {
        this.catalogRegistry = catalogRegistry;
        this.maxSplits = maxSplits;
    }

    /**
     * Runs the warmup. Returns a single-row result with columns
     * {@code (catalog, schema, table, splits_processed, splits_failed,
     * rows_touched, bytes_prefetched, duration_ms)}.
     */
    public List<TypedRow> execute(
            String catalog, Map<String, String> arguments, CoordinatorSessionContext context) {
        SchemaTable target = parseTarget(catalog, arguments);
        ResolvedTable resolved = resolveTable(catalog, target, context);

        long startNanos = System.nanoTime();
        WarmupCounters counters = new WarmupCounters();
        processSplits(catalog, target, resolved, counters);
        long durationMs = Duration.ofNanos(System.nanoTime() - startNanos).toMillis();

        checkWarmupResult(catalog, target, counters);
        LOG.info("event=warmup_table_completed catalog={} table={}.{} "
                        + "splits_processed={} splits_failed={} rows={} bytes={} duration_ms={}",
                catalog, target.schema, target.table,
                counters.splitsProcessed, counters.splitsFailed,
                counters.rowsTouched, counters.bytesPrefetched, durationMs);

        return List.of(buildResultRow(catalog, target, counters, durationMs));
    }

    private ResolvedTable resolveTable(String catalog, SchemaTable target, CoordinatorSessionContext context) {
        ConnectorSession session = TableResolver.buildSession(context, catalog, target.schema);
        CatalogHandle catalogHandle = catalogRegistry.getCatalog(catalog);
        ConnectorMetadata metadata = catalogHandle.getMetadata(session);
        ConnectorTableHandle tableHandle = metadata.getTableHandle(
                session, new SchemaTableName(target.schema, target.table));
        if (tableHandle == null) {
            throw new PangrovaException(PangrovaErrorCode.METADATA_TABLE_NOT_FOUND,
                    "warmup_table: table not found: "
                            + catalog + "." + target.schema + "." + target.table);
        }
        List<ColumnHandle> columns = List.copyOf(metadata.getColumnHandles(session, tableHandle).values());
        return new ResolvedTable(session, catalogHandle, tableHandle, columns);
    }

    private void processSplits(String catalog, SchemaTable target,
                               ResolvedTable resolved, WarmupCounters counters) {
        ConnectorSplitSource splitSource = resolved.catalogHandle.getSplitManager().getSplits(
                null, resolved.session, resolved.tableHandle, DynamicFilter.ALL, Constraint.EMPTY);
        try {
            ConnectorPageSourceProvider psp = resolved.catalogHandle.getPageSourceProvider();
            outer:
            while (!splitSource.isFinished()) {
                List<ConnectorSplit> batch = splitSource.getNextBatch(SPLIT_BATCH_SIZE);
                if (batch.isEmpty()) {
                    if (splitSource.isFinished()) break;
                    continue;
                }
                for (ConnectorSplit split : batch) {
                    if (counters.totalSplits() >= maxSplits) {
                        LOG.info("event=warmup_table_split_cap_reached catalog={} table={}.{} cap={}",
                                catalog, target.schema, target.table, maxSplits);
                        break outer;
                    }
                    warmupSplit(psp, resolved.session, split, resolved.tableHandle, resolved.columns, counters);
                }
            }
        } finally {
            try {
                splitSource.close();
            } catch (RuntimeException e) {
                LOG.debug("event=warmup_table_split_source_close_failed err={}", e.toString());
            }
        }
    }

    private static void checkWarmupResult(String catalog, SchemaTable target, WarmupCounters counters) {
        if (counters.totalSplits() == 0) {
            LOG.info("event=warmup_table_no_splits catalog={} table={}.{}", catalog, target.schema, target.table);
        } else if (counters.splitsFailed > 0 && counters.splitsProcessed == 0) {
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_READ_ERROR,
                    "warmup_table: all " + counters.splitsFailed + " split(s) failed for "
                            + catalog + "." + target.schema + "." + target.table);
        }
    }

    private static TypedRow buildResultRow(String catalog, SchemaTable target,
                                           WarmupCounters counters, long durationMs) {
        return TypedRow.builder()
                .putString("catalog", catalog)
                .putString("schema", target.schema)
                .putString("table", target.table)
                .putLong("splits_processed", counters.splitsProcessed)
                .putLong("splits_failed", counters.splitsFailed)
                .putLong("rows_touched", counters.rowsTouched)
                .putLong("bytes_prefetched", counters.bytesPrefetched)
                .putLong("duration_ms", durationMs)
                .build();
    }

    private record ResolvedTable(ConnectorSession session, CatalogHandle catalogHandle,
                                 ConnectorTableHandle tableHandle, List<ColumnHandle> columns) {}

    private void warmupSplit(ConnectorPageSourceProvider pageSourceProvider,
                             ConnectorSession session,
                             ConnectorSplit split,
                             ConnectorTableHandle tableHandle,
                             List<ColumnHandle> columns,
                             WarmupCounters counters) {
        long splitBytes = sizeHint(split);
        ConnectorPageSource source = null;
        try {
            source = pageSourceProvider.createPageSource(
                    null, session, split, tableHandle, columns, DynamicFilter.ALL);
            drain(source, counters);
            counters.splitsProcessed++;
            // Even if drain reported zero bytes (Java page source path), credit
            // the split's declared length so callers see meaningful pre-fetch
            // numbers from manifest stats.
            counters.bytesPrefetched += Math.max(0L, splitBytes - counters.lastSplitBytesObserved);
            counters.lastSplitBytesObserved = 0L;
        } catch (RuntimeException e) {
            counters.splitsFailed++;
            LOG.warn("event=warmup_table_split_failed err={}", e.toString());
        } finally {
            if (source != null) {
                try {
                    source.close();
                } catch (RuntimeException closeErr) {
                    LOG.debug("event=warmup_table_close_failed err={}", closeErr.toString());
                }
            }
        }
    }

    private static void drain(ConnectorPageSource source, WarmupCounters counters) {
        long observedBytes = 0L;
        while (!source.isFinished()) {
            // Try native page first (preferred, mirrors the real scan path);
            // fall through to row-format pages if the source emits those.
            NativePage np = source.getNextNativePage();
            if (np != null) {
                int rows = Math.max(0, np.getRowCount());
                counters.rowsTouched += rows;
                observedBytes += np.serializedBatch().length;
                np.close();
                continue;
            }
            Page page = source.getNextPage();
            if (page == null) {
                if (source.isFinished()) {
                    break;
                }
                // No data yet but not finished — yield to avoid a busy loop on
                // implementations that may briefly return null mid-stream.
                Thread.onSpinWait();
                continue;
            }
            counters.rowsTouched += page.getPositionCount();
            // Row pages don't expose serialized bytes; rely on split size hint.
        }
        counters.bytesPrefetched += observedBytes;
        counters.lastSplitBytesObserved = observedBytes;
    }

    private static long sizeHint(ConnectorSplit split) {
        try {
            SplitProperties props = split.typedSplitProperties();
            return Math.max(0L, props.length());
        } catch (RuntimeException ignored) {
            return 0L;
        }
    }

    private static SchemaTable parseTarget(String catalog, Map<String, String> arguments) {
        String arg0 = arguments.getOrDefault("arg0", "");
        String arg1 = arguments.getOrDefault("arg1", "");
        if (arg0.isEmpty()) {
            throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "warmup_table requires arguments: "
                            + "CALL " + catalog + ".warmup_table('schema.table') "
                            + "or CALL " + catalog + ".warmup_table('schema', 'table')");
        }
        if (!arg1.isEmpty()) {
            return new SchemaTable(arg0, arg1);
        }
        int dot = arg0.indexOf('.');
        if (dot <= 0 || dot >= arg0.length() - 1) {
            throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "warmup_table: single-argument form must be 'schema.table', got '" + arg0 + "'");
        }
        return new SchemaTable(arg0.substring(0, dot), arg0.substring(dot + 1));
    }

    private record SchemaTable(String schema, String table) { }

    private static final class WarmupCounters {
        long splitsProcessed;
        long splitsFailed;
        long rowsTouched;
        long bytesPrefetched;
        long lastSplitBytesObserved;

        long totalSplits() {
            return splitsProcessed + splitsFailed;
        }
    }

    /** Returns the column names emitted by the summary row (single row only). */
    public static List<String> resultColumns() {
        return List.of("catalog", "schema", "table", "splits_processed", "splits_failed",
                "rows_touched", "bytes_prefetched", "duration_ms");
    }

    @SuppressWarnings("unused") // for non-mutating ConnectorTransactionHandle param documentation
    private static ConnectorTransactionHandle nullTx() {
        return null;
    }
}
