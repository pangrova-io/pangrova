package org.pangrova.connector.paimon;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.connector.api.error.ConnectorException;
import org.pangrova.connector.api.metadata.DeleteFilter;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.types.DataTypeRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Deletes table or partition metadata without opening data readers or writers.
 * Predicates requiring row evaluation must be executed by distributed workers.
 */
final class PaimonDeleteByFilterExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonDeleteByFilterExecutor.class);

    private PaimonDeleteByFilterExecutor() {}

    /** Executes a filter-based delete against the given fresh table. */
    static void execute(Table freshTable, DeleteFilter filter,
                        String schemaName, String tableName) {
        Objects.requireNonNull(filter, "filter");
        Map<String, String> partition = new LinkedHashMap<>();
        boolean deleteAll = filter instanceof DeleteFilter.All;
        if (!deleteAll && (!collectPartitionEqualities(freshTable, filter, partition)
                || partition.isEmpty())) {
            throw new UnsupportedOperationException(
                    "DELETE predicate requires distributed execution: " + schemaName + "." + tableName);
        }

        // Builder creation may reject a read-only table before any mutation.
        var builder = freshTable.newBatchWriteBuilder();
        try (BatchTableCommit commit = builder.newCommit()) {
            if (deleteAll) {
                commit.truncateTable();
            } else {
                commit.truncatePartitions(List.of(Map.copyOf(partition)));
            }
        } catch (Exception e) {
            // A commit/close failure is not a capability decision: never retry it as native DML.
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Failed to commit metadata DELETE: " + schemaName + "." + tableName, e);
        }
        LOG.info("event=paimon_metadata_delete schema={} table={} deleteAll={}",
                schemaName, tableName, deleteAll);
    }

    private static boolean collectPartitionEqualities(Table table, DeleteFilter filter,
                                                       Map<String, String> partition) {
        if (filter instanceof DeleteFilter.And and) {
            return !and.filters().isEmpty() && and.filters().stream()
                    .allMatch(child -> collectPartitionEqualities(table, child, partition));
        }
        if (!(filter instanceof DeleteFilter.Equal equal) || !(equal.value() instanceof String value)) {
            return false;
        }
        String column = table.partitionKeys().stream()
                .filter(key -> key.equalsIgnoreCase(equal.column())).findFirst().orElse(null);
        if (column == null) {
            return false;
        }
        // Other SQL types require coercion rules; leave them to the native expression evaluator.
        boolean stringPartition = table.rowType().getFields().stream().anyMatch(field ->
                field.name().equals(column) && field.type().getTypeRoot() == DataTypeRoot.VARCHAR);
        String nullPartition = table.options().getOrDefault(
                CoreOptions.PARTITION_DEFAULT_NAME.key(), CoreOptions.PARTITION_DEFAULT_NAME.defaultValue());
        if (!stringPartition || value.isEmpty() || value.equals(nullPartition)) {
            return false;
        }
        String previous = partition.putIfAbsent(column, value);
        return previous == null || previous.equals(value);
    }
}
