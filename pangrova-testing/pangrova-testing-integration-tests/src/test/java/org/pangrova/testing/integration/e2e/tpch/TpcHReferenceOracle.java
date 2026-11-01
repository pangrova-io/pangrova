package org.pangrova.testing.integration.e2e.tpch;

import org.pangrova.spi.connector.data.value.TypedRow;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Owns an isolated schema in the singleton PostgreSQL test container. */
final class TpcHReferenceOracle implements AutoCloseable {
    private final Connection connection;
    private final String schema = "tpch_oracle_" + UUID.randomUUID().toString().replace("-", "");

    TpcHReferenceOracle(Connection connection) throws SQLException {
        this.connection = connection;
        try (var statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
            statement.execute("SET search_path TO " + schema);
        } catch (SQLException failure) {
            try {
                connection.close();
            } catch (SQLException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    void executeInput(String sql) {
        try (var statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException failure) {
            throw new IllegalStateException("TPC-H reference input failed", failure);
        }
    }

    List<TypedRow> query(String sql) {
        try (var statement = connection.createStatement()) {
            statement.setQueryTimeout(30);
            try (ResultSet results = statement.executeQuery(sql)) {
                return readRows(results);
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("TPC-H independent reference query failed", failure);
        }
    }

    private static List<TypedRow> readRows(ResultSet results) throws SQLException {
        var columns = results.getMetaData();
        List<TypedRow> rows = new ArrayList<>();
        while (results.next()) {
            var row = TypedRow.builder();
            for (int i = 1; i <= columns.getColumnCount(); i++) {
                Object value = results.getObject(i);
                if (value instanceof java.sql.Date date) {
                    value = date.toLocalDate();
                }
                row.putRaw(columns.getColumnLabel(i), value);
            }
            rows.add(row.build());
        }
        return List.copyOf(rows);
    }

    @Override
    public void close() throws SQLException {
        try (connection; var statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA " + schema + " CASCADE");
        }
    }
}
