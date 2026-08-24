package org.pangrova.coordinator.query.statement.dml;

import org.pangrova.coordinator.query.statement.TableResolver;
import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.parser.ast.statement.dml.InsertSelectStatement;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Consumer;

/**
 * Executes INSERT SELECT statements (write phase only).
 *
 * <p>Receives a pre-computed {@link Page} from the SELECT pipeline and writes
 * it into the target table, handling column alignment and overwrite semantics.
 */
final class InsertSelectExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(InsertSelectExecutor.class);

    private final CatalogRegistry catalogRegistry;
    private final Consumer<String> writeCallback;

    InsertSelectExecutor(CatalogRegistry catalogRegistry, Consumer<String> writeCallback) {
        this.catalogRegistry = catalogRegistry;
        this.writeCallback = writeCallback;
    }

    void writeInsertSelectRows(InsertSelectStatement stmt,
                               Page page,
                               List<String> selectOutputColumns) {
        QualifiedTableName ref = TableResolver.resolveTableRef(stmt.tableName().parts());
        ConnectorSession session = TableResolver.buildSession(ref.catalog());
        CatalogHandle catalogHandle = catalogRegistry.getCatalog(ref.catalog());
        ConnectorMetadata metadata = catalogHandle.getMetadata(session);

        ConnectorTableHandle tableHandle = TableResolver.requireTableHandle(metadata, session, ref);
        List<String> columns = resolveInsertSelectColumns(
                stmt, metadata, session, tableHandle, selectOutputColumns);
        DmlValidator.validateNoNanOrInfinity(page);
        Page aligned = alignPageColumns(page, columns);

        LOG.info("event=insert_select_start table={} rowCount={}", ref, page.getPositionCount());
        writeInsertSelectPage(stmt.overwrite(), catalogHandle, session,
                metadata, tableHandle, aligned);
        notifyWrite(ref);
        LOG.info("event=insert_select_complete table={} rowCount={} overwrite={}",
                ref, page.getPositionCount(), stmt.overwrite());
    }

    private static List<String> resolveInsertSelectColumns(
            InsertSelectStatement stmt, ConnectorMetadata metadata,
            ConnectorSession session, ConnectorTableHandle tableHandle,
            List<String> selectOutputColumns) {
        if (stmt.columns().isPresent()) {
            return stmt.columns().get();
        }
        ConnectorTableMetadata tableMeta = metadata.getTableMetadata(session, tableHandle);
        if (tableMeta.columns().size() == selectOutputColumns.size()) {
            return tableMeta.columns();
        }
        return selectOutputColumns;
    }

    /**
     * Remaps Page column names from SELECT output to target table column names.
     * Tries name-based matching first; falls back to positional if names don't overlap.
     */
    private static Page alignPageColumns(Page source, List<String> targetColumns) {
        if (source.columns().equals(targetColumns)) {
            return source;
        }
        boolean nameMatch = targetColumns.stream()
                .allMatch(col -> source.columns().contains(col));
        if (nameMatch) {
            return alignByName(source, targetColumns);
        }
        return alignByPosition(source, targetColumns);
    }

    private static Page alignByName(Page source, List<String> targetColumns) {
        List<TypedRow> remapped = source.rows().stream()
                .map(row -> {
                    TypedRow.Builder builder = TypedRow.builder();
                    for (String col : targetColumns) {
                        builder.put(col, row.get(col));
                    }
                    return builder.build();
                })
                .toList();
        return new Page(targetColumns, remapped);
    }

    private static Page alignByPosition(Page source, List<String> targetColumns) {
        if (source.columns().size() != targetColumns.size()) {
            throw new IllegalArgumentException(
                    "Column count mismatch: source=" + source.columns().size()
                            + " target=" + targetColumns.size());
        }
        List<TypedRow> remapped = source.rows().stream()
                .map(row -> remapRow(row, source.columns(), targetColumns))
                .toList();
        return new Page(targetColumns, remapped);
    }

    private static TypedRow remapRow(
            TypedRow row, List<String> sourceColumns, List<String> targetColumns) {
        TypedRow.Builder builder = TypedRow.builder();
        for (int i = 0; i < targetColumns.size() && i < sourceColumns.size(); i++) {
            builder.put(targetColumns.get(i), row.get(sourceColumns.get(i)));
        }
        return builder.build();
    }

    private static void writeInsertSelectPage(
            boolean overwrite, CatalogHandle catalogHandle, ConnectorSession session,
            ConnectorMetadata metadata, ConnectorTableHandle tableHandle, Page page) {
        if (overwrite) {
            ConnectorOutputTableHandle oh = metadata.beginOverwrite(session, tableHandle);
            DmlSinkOperations.writePageViaOverwriteSink(catalogHandle, session, oh, page);
        } else {
            ConnectorOutputTableHandle oh = metadata.beginInsert(session, tableHandle);
            DmlSinkOperations.writePageViaSink(catalogHandle, session, oh, page);
        }
    }

    private void notifyWrite(QualifiedTableName ref) {
        if (writeCallback != null) {
            writeCallback.accept(ref.toString());
        }
    }
}
