package org.pangrova.coordinator.http.jdbc;

import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Avatica Meta implementation that bridges JDBC wire protocol requests
 * to Pangrova's query execution engine via {@link JdbcQueryExecutor}.
 *
 * <p>Each Avatica connection maps to an Pangrova session. Statement execution
 * delegates to the coordinator's query lifecycle (submit, poll, fetch).</p>
 */
public final class PangrovaJdbcMeta extends MetaImpl {

    private static final Logger LOG = LoggerFactory.getLogger(PangrovaJdbcMeta.class);

    private final JdbcQueryExecutor queryExecutor;
    private final JdbcMetadataProvider metadataProvider;
    private final Map<String, ConnectionState> connections = new ConcurrentHashMap<>();
    private final AtomicInteger statementCounter = new AtomicInteger();

    public PangrovaJdbcMeta(JdbcQueryExecutor queryExecutor, JdbcMetadataProvider metadataProvider) {
        super(null);
        this.queryExecutor = queryExecutor;
        this.metadataProvider = metadataProvider;
    }

    @Override
    public StatementHandle createStatement(ConnectionHandle ch) {
        String connectionId = ch.id;
        ensureConnection(connectionId);
        int statementId = statementCounter.incrementAndGet();
        StatementHandle handle = new StatementHandle(connectionId, statementId, null);
        LOG.debug("event=create_statement connectionId={} statementId={}", connectionId, statementId);
        return handle;
    }

    @Override
    public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues,
                                  long maxRowCount) throws NoSuchStatementException {
        return execute(h, parameterValues, -1);
    }

    @Override
    public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues,
                                  int maxRowsInFirstFrame) throws NoSuchStatementException {
        String sql = h.signature != null ? h.signature.sql : null;
        if (sql == null || sql.isBlank()) {
            throw new NoSuchStatementException(h);
        }
        return executeQuery(h, sql);
    }

    @Override
    public ExecuteResult prepareAndExecute(StatementHandle h, String sql,
                                            long maxRowCount, PrepareCallback callback)
            throws NoSuchStatementException {
        return prepareAndExecute(h, sql, maxRowCount, -1, callback);
    }

    @Override
    public ExecuteResult prepareAndExecute(StatementHandle h, String sql,
                                            long maxRowCount, int maxRowsInFirstFrame,
                                            PrepareCallback callback)
            throws NoSuchStatementException {
        LOG.debug("event=prepare_and_execute connectionId={} sql={}", h.connectionId, sql);
        return executeQuery(h, sql);
    }

    @Override
    public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
        int statementId = statementCounter.incrementAndGet();
        Signature signature = new Signature(
                Collections.emptyList(),
                sql,
                Collections.emptyList(),
                Collections.emptyMap(),
                CursorFactory.LIST,
                Meta.StatementType.SELECT);
        return new StatementHandle(ch.id, statementId, signature);
    }

    @Override
    public Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount)
            throws NoSuchStatementException, MissingResultsException {
        // Results are fully materialized in the first frame during executeQuery.
        // Subsequent fetch calls return EMPTY to signal end-of-data.
        return Frame.EMPTY;
    }

    @Override
    public void openConnection(ConnectionHandle ch, Map<String, String> info) {
        String connectionId = ch.id;
        String catalog = info != null ? info.get("catalog") : null;
        connections.put(connectionId, new ConnectionState(connectionId, catalog));
        LOG.info("event=open_connection connectionId={} catalog={}", connectionId, catalog);
    }

    @Override
    public void closeConnection(ConnectionHandle ch) {
        connections.remove(ch.id);
        LOG.info("event=close_connection connectionId={}", ch.id);
    }

    @Override
    public void closeStatement(StatementHandle h) {
        LOG.debug("event=close_statement connectionId={} statementId={}", h.connectionId, h.id);
    }

    @Override
    public void commit(ConnectionHandle ch) {
        LOG.debug("event=commit connectionId={}", ch.id);
    }

    @Override
    public void rollback(ConnectionHandle ch) {
        LOG.debug("event=rollback connectionId={}", ch.id);
    }

    @Override
    public boolean syncResults(StatementHandle sh, QueryState state, long offset)
            throws NoSuchStatementException {
        return false;
    }

    @Override
    public ExecuteBatchResult executeBatch(StatementHandle h, List<List<TypedValue>> parameterValues)
            throws NoSuchStatementException {
        throw new UnsupportedOperationException("Batch execution is not supported by Pangrova JDBC");
    }

    @Override
    public ExecuteBatchResult prepareAndExecuteBatch(StatementHandle h, List<String> sqlCommands)
            throws NoSuchStatementException {
        throw new UnsupportedOperationException("Batch execution is not supported by Pangrova JDBC");
    }

    @Override
    public ConnectionProperties connectionSync(ConnectionHandle ch, ConnectionProperties connProps) {
        return connProps;
    }

    @Override
    public MetaResultSet getCatalogs(ConnectionHandle ch) {
        return metadataProvider.getCatalogs(ch);
    }

    @Override
    public MetaResultSet getSchemas(ConnectionHandle ch, String catalog, Pat schemaPattern) {
        return metadataProvider.getSchemas(ch, catalog, schemaPattern);
    }

    @Override
    public MetaResultSet getTables(ConnectionHandle ch, String catalog, Pat schemaPattern,
                                    Pat tableNamePattern, List<String> typeList) {
        return metadataProvider.getTables(ch, catalog, schemaPattern, tableNamePattern, typeList);
    }

    @Override
    public MetaResultSet getColumns(ConnectionHandle ch, String catalog, Pat schemaPattern,
                                     Pat tableNamePattern, Pat columnNamePattern) {
        return metadataProvider.getColumns(ch, catalog, schemaPattern,
                tableNamePattern, columnNamePattern);
    }

    @Override
    public MetaResultSet getTableTypes(ConnectionHandle ch) {
        return metadataProvider.getTableTypes(ch);
    }

    private ExecuteResult executeQuery(StatementHandle h, String sql) {
        ConnectionState connState = connections.get(h.connectionId);
        String catalog = connState != null ? connState.catalog() : null;

        JdbcQueryResult result = queryExecutor.execute(sql, catalog);

        List<ColumnMetaData> columns = result.columns();
        Signature signature = new Signature(
                columns,
                sql,
                Collections.emptyList(),
                Collections.emptyMap(),
                CursorFactory.LIST,
                Meta.StatementType.SELECT);

        Frame firstFrame = Frame.create(0, true, Collections.unmodifiableList(result.rows()));

        MetaResultSet resultSet = MetaResultSet.create(
                h.connectionId, h.id, false, signature, firstFrame);

        return new ExecuteResult(Collections.singletonList(resultSet));
    }

    private void ensureConnection(String connectionId) {
        connections.computeIfAbsent(connectionId, id -> new ConnectionState(id, null));
    }

    private record ConnectionState(String connectionId, String catalog) {}
}
