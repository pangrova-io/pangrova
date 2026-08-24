package org.pangrova.coordinator.ai.training;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.data.ConnectorPageSource;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.data.page.NativePage;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.ConnectorSplitSource;
import org.pangrova.spi.connector.metadata.DynamicFilter;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Reads table data from connectors and produces columnar IPC serialized batches
 * for the training data serving endpoint.
 *
 * <p>Resolves table metadata via {@link CatalogRegistry}, fetches splits from
 * the connector's split manager, and reads data through {@link ConnectorPageSource}.
 * Produces columnar IPC stream format bytes consumable by the producer layer.</p>
 *
 * <p>Thread safety: instances are safe for concurrent use. Each {@link ScanSession}
 * is confined to a single thread.</p>
 */
final class TrainingDataScanner {

    private static final Logger LOG = LoggerFactory.getLogger(TrainingDataScanner.class);

    private final CatalogRegistry catalogRegistry;
    private final ObjectMapper objectMapper;

    TrainingDataScanner(CatalogRegistry catalogRegistry) {
        this.catalogRegistry = Objects.requireNonNull(catalogRegistry, "catalogRegistry must not be null");
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Resolves the columnar schema for the requested table and columns.
     * Returns a schema with column names and generic binary type (actual types
     * are determined by the columnar IPC stream from the connector).
     *
     * @param commandBytes JSON command from the descriptor
     * @return columnar schema for the requested projection
     */
    Schema resolveSchema(byte[] commandBytes) {
        TrainingDataRequest request = parseRequest(commandBytes);
        TableRef ref = parseTableRef(request.table());

        CatalogHandle catalog = catalogRegistry.getCatalog(ref.catalog());
        ConnectorSession session = new TrainingConnectorSession(ref.catalog());
        ConnectorTableHandle tableHandle = catalog.getMetadata(session)
                .getTableHandle(session, new SchemaTableName(ref.schema(), ref.table()));

        if (tableHandle == null) {
            return new Schema(List.of());
        }

        Map<String, ColumnHandle> columnMap = catalog.getMetadata(session)
                .getColumnHandles(session, tableHandle);
        List<String> requestedColumns = request.columns();

        List<Field> fields = new ArrayList<>();
        for (Map.Entry<String, ColumnHandle> entry : columnMap.entrySet()) {
            String colName = entry.getKey();
            if (requestedColumns == null || requestedColumns.contains(colName)) {
                fields.add(new Field(colName, FieldType.nullable(ArrowType.Binary.INSTANCE), null));
            }
        }

        return new Schema(fields);
    }

    /**
     * Opens a scan session that streams data from the connector as columnar IPC batches.
     *
     * @param ticketBytes serialized request (same JSON format as command)
     * @param allocator   buffer allocator for batch materialization
     * @return a scan session that produces columnar IPC byte arrays
     */
    ScanSession openScan(byte[] ticketBytes, BufferAllocator allocator) {
        TrainingDataRequest request = parseRequest(ticketBytes);
        TableRef ref = parseTableRef(request.table());

        CatalogHandle catalog = catalogRegistry.getCatalog(ref.catalog());
        ConnectorSession session = new TrainingConnectorSession(ref.catalog());
        ConnectorTableHandle tableHandle = catalog.getMetadata(session)
                .getTableHandle(session, new SchemaTableName(ref.schema(), ref.table()));

        if (tableHandle == null) {
            throw new IllegalArgumentException("Table not found: " + request.table());
        }

        Map<String, ColumnHandle> columnMap = catalog.getMetadata(session)
                .getColumnHandles(session, tableHandle);
        List<ColumnHandle> projectedColumns = projectColumns(columnMap, request.columns());

        ConnectorSplitManager splitManager = catalog.getSplitManager();
        ConnectorSplitSource splitSource = splitManager.getSplits(
                ConnectorTransactionHandle.EMPTY, session, tableHandle, DynamicFilter.ALL, null);
        List<org.pangrova.spi.connector.metadata.ConnectorSplit> splits = drainSplits(splitSource);

        ConnectorPageSourceProvider pageSourceProvider = catalog.getPageSourceProvider();

        LOG.info("event=training_scan_opened table={} splits={} columns={}",
                request.table(), splits.size(), projectedColumns.size());

        return new ScanSession(
                new ScanConfig(pageSourceProvider, session, tableHandle,
                        projectedColumns, splits, request.samplingRate()));
    }

    private List<ColumnHandle> projectColumns(Map<String, ColumnHandle> all, List<String> requested) {
        if (requested == null || requested.isEmpty()) {
            return List.copyOf(all.values());
        }
        return all.entrySet().stream()
                .filter(e -> requested.contains(e.getKey()))
                .map(Map.Entry::getValue)
                .toList();
    }

    private List<org.pangrova.spi.connector.metadata.ConnectorSplit> drainSplits(ConnectorSplitSource source) {
        List<org.pangrova.spi.connector.metadata.ConnectorSplit> splits = new ArrayList<>();
        List<org.pangrova.spi.connector.metadata.ConnectorSplit> batch = source.getNextBatch(100);
        while (!batch.isEmpty()) {
            splits.addAll(batch);
            batch = source.getNextBatch(100);
        }
        return splits;
    }

    private TrainingDataRequest parseRequest(byte[] bytes) {
        String json = new String(bytes, StandardCharsets.UTF_8);
        try {
            JsonNode node = objectMapper.readTree(json);
            String table = node.get("table").asText();
            String version = node.has("version") && !node.get("version").isNull()
                    ? node.get("version").asText() : null;
            String split = node.has("split") && !node.get("split").isNull()
                    ? node.get("split").asText() : null;
            List<String> columns = parseColumnList(node);
            Double samplingRate = node.has("samplingRate") && !node.get("samplingRate").isNull()
                    ? node.get("samplingRate").asDouble() : null;
            return new TrainingDataRequest(table, version, split, columns, samplingRate);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse training data request: " + e.getMessage(), e);
        }
    }

    private List<String> parseColumnList(JsonNode node) {
        if (!node.has("columns") || node.get("columns").isNull()) {
            return null;
        }
        JsonNode columnsNode = node.get("columns");
        if (!columnsNode.isArray()) {
            return null;
        }
        List<String> columns = new ArrayList<>(columnsNode.size());
        for (JsonNode col : columnsNode) {
            columns.add(col.asText());
        }
        return columns;
    }

    private TableRef parseTableRef(String qualifiedName) {
        String[] parts = qualifiedName.split("\\.");
        if (parts.length < 3) {
            throw new IllegalArgumentException(
                    "Table must be fully qualified (catalog.schema.table), got: " + qualifiedName);
        }
        return new TableRef(parts[0], parts[1], parts[2]);
    }

    private record TableRef(String catalog, String schema, String table) {}

    /**
     * Immutable configuration for a scan session, grouping constructor parameters.
     */
    record ScanConfig(
            ConnectorPageSourceProvider pageSourceProvider,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            List<org.pangrova.spi.connector.metadata.ConnectorSplit> splits,
            Double samplingRate) {

        ScanConfig {
            columns = List.copyOf(columns);
            splits = List.copyOf(splits);
        }
    }

    /**
     * A scan session that iterates over splits and produces columnar IPC batches.
     * Must be used from a single thread and closed after use.
     */
    static final class ScanSession implements Closeable {

        private final ScanConfig config;
        private int currentSplitIndex;
        private ConnectorPageSource currentSource;

        ScanSession(ScanConfig config) {
            this.config = config;
        }

        /**
         * Returns the next columnar IPC serialized batch, or null when all splits are exhausted.
         *
         * @return columnar IPC stream bytes, or null if no more data
         */
        byte[] nextBatch() {
            while (true) {
                if (currentSource == null) {
                    if (currentSplitIndex >= config.splits().size()) {
                        return null;
                    }
                    var split = config.splits().get(currentSplitIndex);
                    currentSource = config.pageSourceProvider().createPageSource(
                            ConnectorTransactionHandle.EMPTY, config.session(),
                            split, config.tableHandle(), config.columns(), DynamicFilter.ALL);
                    currentSplitIndex++;
                }

                if (currentSource.isFinished()) {
                    currentSource.close();
                    currentSource = null;
                    continue;
                }

                NativePage page = currentSource.getNextNativePage();
                if (page == null) {
                    currentSource.close();
                    currentSource = null;
                    continue;
                }

                byte[] data = page.serializedBatch();
                page.close();

                if (config.samplingRate() != null
                        && ThreadLocalRandom.current().nextDouble() > config.samplingRate()) {
                    continue;
                }

                return data;
            }
        }

        @Override
        public void close() {
            if (currentSource != null) {
                currentSource.close();
                currentSource = null;
            }
        }
    }
}
