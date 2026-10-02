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
package org.pangrova.testing.e2e.sql;

import org.pangrova.testing.E2ETest;
import org.pangrova.testing.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

/**
 * Full-stack E2E tests for COPY INTO SQL statement.
 *
 * <p>Validates SQL parsing and execution of COPY INTO for loading data
 * from external files/URIs and subqueries into tables.</p>
 */
@E2ETest
@DisplayName("COPY INTO Full-Stack E2E")
class CopyIntoE2ETest extends PangrovaE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";
    private String tableName;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void assignTableName(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_copy_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    @Nested
    @DisplayName("COPY INTO from URI")
    class CopyFromUri {

        @Test
        @DisplayName("COPY INTO from Parquet URI syntax is accepted")
        void copyFromParquetUri() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR, value DOUBLE)");

            String sql = "COPY INTO " + fqn()
                    + " FROM 's3://test-bucket/data/sample.parquet'"
                    + " FILE_FORMAT = 'PARQUET'";
            executeDdl(sql);

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("COPY INTO from CSV URI with options")
        void copyFromCsvWithOptions() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR)");

            String sql = "COPY INTO " + fqn()
                    + " FROM 's3://test-bucket/data/users.csv'"
                    + " FILE_FORMAT = 'CSV'"
                    + " WITH ('header'='true', 'delimiter'=',')";
            executeDdl(sql);

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("COPY INTO from JSON URI")
        void copyFromJsonUri() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, payload VARCHAR)");

            String sql = "COPY INTO " + fqn()
                    + " FROM 's3://test-bucket/events/events.json'"
                    + " FILE_FORMAT = 'JSON'";
            executeDdl(sql);

            executeDdl("DROP TABLE " + fqn());
        }
    }

    @Nested
    @DisplayName("COPY INTO from Subquery")
    class CopyFromSubquery {

        @Test
        @DisplayName("COPY INTO from SELECT subquery")
        void copyFromSubquery() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, name VARCHAR)");
            String sourceTable = fqn() + "_source";
            executeDdl("CREATE TABLE " + sourceTable + " (id BIGINT, name VARCHAR, active BOOLEAN)");
            executeDml("INSERT INTO " + sourceTable + " VALUES "
                    + "(1, 'Alice', true), (2, 'Bob', false), (3, 'Carol', true)");

            String sql = "COPY INTO " + fqn()
                    + " FROM (SELECT id, name FROM " + sourceTable + " WHERE active = true)";
            executeDdl(sql);
            assertQueryReturnsRows("SELECT * FROM " + fqn(), 2);

            executeDdl("DROP TABLE " + fqn());
            executeDdl("DROP TABLE " + sourceTable);
        }
    }

    @Nested
    @DisplayName("COPY INTO with Pattern Matching")
    class CopyWithPattern {

        @Test
        @DisplayName("COPY INTO with PATTERN clause for multiple files")
        void copyWithPattern() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, event VARCHAR)");

            String sql = "COPY INTO " + fqn()
                    + " FROM 's3://test-bucket/logs/'"
                    + " FILE_FORMAT = 'PARQUET'"
                    + " PATTERN = '*.parquet'";
            executeDdl(sql);

            executeDdl("DROP TABLE " + fqn());
        }
    }
}
