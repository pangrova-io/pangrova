/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.metadata.tag;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import javax.sql.DataSource;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC-backed {@link ColumnTagStore} that durably persists column governance
 * tags to the {@code column_tags} table, so tags survive coordinator restarts.
 *
 * <p>Each mutation runs in a short PostgreSQL transaction protected by a per-tag advisory lock.
 * This also serializes concurrent creation of a previously absent tag, for which a row lock does
 * not yet exist. No in-process mutable state or coordinator affinity is required.</p>
 */
@ThreadSafe
public final class JdbcColumnTagStore implements ColumnTagStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcColumnTagStore.class);

    private static final String DDL = """
            CREATE TABLE IF NOT EXISTS column_tags (
                store_key  VARCHAR(1024) NOT NULL,
                tag_key    VARCHAR(255)  NOT NULL,
                tag_value  TEXT          NOT NULL,
                PRIMARY KEY (store_key, tag_key)
            )
            """;
    private static final String SELECT_ONE =
            "SELECT tag_value FROM column_tags WHERE store_key = ? AND tag_key = ?";
    private static final String INSERT_ONE =
            "INSERT INTO column_tags (store_key, tag_key, tag_value) VALUES (?, ?, ?)";
    private static final String UPDATE_ONE =
            "UPDATE column_tags SET tag_value = ? WHERE store_key = ? AND tag_key = ?";
    private static final String DELETE_ONE =
            "DELETE FROM column_tags WHERE store_key = ? AND tag_key = ?";
    private static final String SELECT_ALL =
            "SELECT tag_key, tag_value FROM column_tags WHERE store_key = ?";
    private static final String LOCK_TAG =
            "SELECT pg_advisory_xact_lock(hashtextextended(?, 0))";

    private final DataSource dataSource;

    public JdbcColumnTagStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        initSchema();
    }

    /** Creates the column_tags table if it does not exist. */
    public void initSchema() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(DDL)) {
            ps.execute();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to initialize column_tags schema", e);
        }
    }

    @Override
    public String put(String storeKey, String tagKey, String value) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                lockTag(conn, storeKey, tagKey);
                String previous = selectOne(conn, storeKey, tagKey);
                if (previous == null) {
                    execUpdate(conn, INSERT_ONE, storeKey, tagKey, value);
                } else {
                    execUpdate(conn, UPDATE_ONE, value, storeKey, tagKey);
                }
                conn.commit();
                return previous;
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to persist column tag: " + tagKey, e);
        }
    }

    @Override
    public String remove(String storeKey, String tagKey) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                lockTag(conn, storeKey, tagKey);
                String previous = selectOne(conn, storeKey, tagKey);
                if (previous != null) {
                    execUpdate(conn, DELETE_ONE, storeKey, tagKey);
                }
                conn.commit();
                return previous;
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to remove column tag: " + tagKey, e);
        }
    }

    @Override
    public Map<String, String> list(String storeKey) {
        Map<String, String> result = new LinkedHashMap<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_ALL)) {
            ps.setString(1, storeKey);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.put(rs.getString(1), rs.getString(2));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to list column tags for: " + storeKey, e);
        }
        return Map.copyOf(result);
    }

    private static String selectOne(Connection conn, String storeKey, String tagKey)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SELECT_ONE)) {
            ps.setString(1, storeKey);
            ps.setString(2, tagKey);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    private static void lockTag(Connection connection, String storeKey, String tagKey)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(LOCK_TAG)) {
            statement.setString(1, storeKey + '\u001f' + tagKey);
            statement.executeQuery().close();
        }
    }

    private static void execUpdate(Connection conn, String sql, String... params)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                ps.setString(i + 1, params[i]);
            }
            ps.executeUpdate();
        }
    }
}
