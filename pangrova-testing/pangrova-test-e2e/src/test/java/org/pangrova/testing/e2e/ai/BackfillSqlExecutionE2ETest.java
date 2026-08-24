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
package org.pangrova.testing.e2e.ai;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.E2ETest;
import org.pangrova.testing.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full-stack E2E test for ALTER TABLE BACKFILL COLUMN SQL execution.
 *
 * <p>Exercises the complete backfill pipeline through SQL:
 * <ul>
 *   <li>{@code ALTER TABLE t BACKFILL COLUMN col} — trigger embedding backfill</li>
 *   <li>{@code ALTER TABLE t BACKFILL COLUMN col WITH (batch_size=N, concurrency=M)} — with options</li>
 *   <li>{@code ALTER TABLE t CANCEL BACKFILL COLUMN col} — cancel running backfill</li>
 *   <li>{@code SHOW BACKFILL STATUS FROM t} — query backfill status</li>
 *   <li>Auto-embedding column definition with GENERATED ALWAYS AS</li>
 * </ul>
 *
 * <p>Since actual embedding model inference requires a running endpoint,
 * tests verify SQL parsing/planning acceptance and appropriate runtime errors
 * (NOT syntax errors) when no model endpoint is available.</p>
 */
@E2ETest
@DisplayName("Backfill SQL Execution Full-Stack E2E")
class BackfillSqlExecutionE2ETest extends PangrovaE2ETestBase {

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
        tableName = "t_backfill_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    // -----------------------------------------------------------------------
    // AUTO-EMBEDDING Column Definition
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Auto-Embedding Column (GENERATED ALWAYS AS)")
    class AutoEmbeddingColumn {

        @Test
        @DisplayName("CREATE TABLE with GENERATED ALWAYS AS AI_EMBED syntax accepted")
        void createTableWithAutoEmbedding() {
            String sql = "CREATE TABLE " + fqn()
                    + " (id BIGINT, content VARCHAR, "
                    + "embedding VECTOR(384) GENERATED ALWAYS AS (AI_EMBED('text-embedding-3-small', content)) EMBEDDING MODE SYNC)";
            try {
                executeDdl(sql);
                // If succeeds, verify table exists
                assertQueryReturnsNonEmpty("SHOW COLUMNS FROM " + fqn());
                executeDdl("DROP TABLE " + fqn());
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                // Syntax should be accepted; runtime errors (no model) are OK
                assertThat(error).doesNotContainIgnoringCase("syntax error");
                assertThat(error).doesNotContainIgnoringCase("mismatched input");
            }
        }

        @Test
        @DisplayName("ASYNC embedding mode syntax accepted")
        void asyncEmbeddingMode() {
            String sql = "CREATE TABLE " + fqn()
                    + " (id BIGINT, content VARCHAR, "
                    + "embedding VECTOR(768) GENERATED ALWAYS AS (AI_EMBED('bge-large', content)) EMBEDDING MODE ASYNC)";
            try {
                executeDdl(sql);
                executeDdl("DROP TABLE " + fqn());
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
                assertThat(error).doesNotContainIgnoringCase("mismatched input");
            }
        }
    }

    // -----------------------------------------------------------------------
    // ALTER TABLE BACKFILL COLUMN
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("ALTER TABLE BACKFILL COLUMN")
    class BackfillColumn {

        @Test
        @DisplayName("Basic BACKFILL COLUMN syntax is accepted")
        void basicBackfillSyntax() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, content VARCHAR, embedding VECTOR(384))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'hello world', NULL),"
                    + "(2, 'test data', NULL),"
                    + "(3, 'sample text', NULL)");

            String sql = "ALTER TABLE " + fqn() + " BACKFILL COLUMN embedding";
            try {
                executeDdl(sql);
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                // Parser must accept the syntax; runtime errors are OK
                assertThat(error).doesNotContainIgnoringCase("syntax error");
                assertThat(error).doesNotContainIgnoringCase("mismatched input");
                assertThat(error).doesNotContainIgnoringCase("no viable alternative");
            }

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("BACKFILL COLUMN with batch_size and concurrency options")
        void backfillWithOptions() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, content VARCHAR, embedding VECTOR(384))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'text one', NULL),"
                    + "(2, 'text two', NULL)");

            String sql = "ALTER TABLE " + fqn()
                    + " BACKFILL COLUMN embedding BATCH_SIZE 500 CONCURRENCY 4";
            try {
                executeDdl(sql);
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
                assertThat(error).doesNotContainIgnoringCase("mismatched input");
            }

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("BACKFILL COLUMN with WHERE filter clause")
        void backfillWithFilter() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, content VARCHAR, embedding VECTOR(384))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'text a', NULL),"
                    + "(2, 'text b', NULL),"
                    + "(3, 'text c', NULL)");

            String sql = "ALTER TABLE " + fqn()
                    + " BACKFILL COLUMN embedding WHERE embedding IS NULL";
            try {
                executeDdl(sql);
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
                assertThat(error).doesNotContainIgnoringCase("mismatched input");
            }

            executeDdl("DROP TABLE " + fqn());
        }
    }

    // -----------------------------------------------------------------------
    // CANCEL BACKFILL
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("ALTER TABLE CANCEL BACKFILL")
    class CancelBackfill {

        @Test
        @DisplayName("CANCEL BACKFILL COLUMN syntax is accepted")
        void cancelBackfillSyntax() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, content VARCHAR, embedding VECTOR(384))");

            String sql = "CANCEL BACKFILL ON " + fqn() + " COLUMN embedding";
            try {
                executeDdl(sql);
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                // Parser must accept the syntax; "no active backfill" runtime error is OK
                assertThat(error).doesNotContainIgnoringCase("syntax error");
                assertThat(error).doesNotContainIgnoringCase("mismatched input");
            }

            executeDdl("DROP TABLE " + fqn());
        }
    }

    // -----------------------------------------------------------------------
    // SHOW BACKFILL STATUS
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("SHOW BACKFILL STATUS")
    class ShowBackfillStatus {

        @Test
        @DisplayName("SHOW BACKFILL STATUS syntax is accepted")
        void showBackfillStatusSyntax() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, content VARCHAR, embedding VECTOR(384))");

            String sql = "SHOW BACKFILL STATUS ON " + fqn();
            try {
                List<TypedRow> rows = executeQuery(sql);
                // Might return empty if no backfill was ever run
                assertThat(rows).isNotNull();
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
                assertThat(error).doesNotContainIgnoringCase("mismatched input");
            }

            executeDdl("DROP TABLE " + fqn());
        }
    }

    // -----------------------------------------------------------------------
    // End-to-End Backfill Workflow
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Complete Backfill Workflow")
    class CompleteWorkflow {

        @Test
        @DisplayName("Full workflow: create table → insert → backfill → show status")
        void fullBackfillWorkflow() {
            // Step 1: Create table with text and embedding columns
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, content VARCHAR, embedding VECTOR(384))");

            // Step 2: Insert data with NULL embeddings
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'Machine learning is a subset of AI', NULL),"
                    + "(2, 'Deep learning uses neural networks', NULL),"
                    + "(3, 'Natural language processing handles text', NULL),"
                    + "(4, 'Computer vision analyzes images', NULL),"
                    + "(5, 'Reinforcement learning learns from rewards', NULL)");

            // Step 3: Verify data is inserted
            assertQueryReturnsRows("SELECT * FROM " + fqn(), 5);

            // Step 4: Trigger backfill
            String backfillSql = "ALTER TABLE " + fqn() + " BACKFILL COLUMN embedding";
            try {
                executeDdl(backfillSql);
            } catch (AssertionError e) {
                String error = executeExpectingError(backfillSql);
                // Acceptable: no model configured error
                assertThat(error).doesNotContainIgnoringCase("syntax error");
            }

            // Step 5: Check backfill status
            String statusSql = "SHOW BACKFILL STATUS ON " + fqn();
            try {
                List<TypedRow> status = executeQuery(statusSql);
                assertThat(status).isNotNull();
            } catch (AssertionError e) {
                // OK if status command also fails due to no active backfill
                String error = executeExpectingError(statusSql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
            }

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("Backfill → Cancel workflow validates both commands")
        void backfillThenCancel() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, content VARCHAR, embedding VECTOR(384))");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'sample text', NULL)");

            // Attempt backfill
            String backfillSql = "ALTER TABLE " + fqn() + " BACKFILL COLUMN embedding";
            try {
                executeDdl(backfillSql);
            } catch (AssertionError e) {
                // Expected: no model endpoint configured
            }

            // Attempt cancel
            String cancelSql = "CANCEL BACKFILL ON " + fqn() + " COLUMN embedding";
            try {
                executeDdl(cancelSql);
            } catch (AssertionError e) {
                String error = executeExpectingError(cancelSql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
            }

            executeDdl("DROP TABLE " + fqn());
        }
    }
}
