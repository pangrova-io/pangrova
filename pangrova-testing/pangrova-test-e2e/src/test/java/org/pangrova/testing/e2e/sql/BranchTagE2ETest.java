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
 * Full-stack E2E tests for Branch and Tag DDL operations.
 *
 * <p>Validates Iceberg-style data versioning through SQL:
 * CREATE/DROP/USE BRANCH, CREATE/DROP TAG, SHOW BRANCHES/TAGS.</p>
 */
@E2ETest
@DisplayName("Branch/Tag DDL Full-Stack E2E")
class BranchTagE2ETest extends PangrovaE2ETestBase {

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
        tableName = "t_branch_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    @Nested
    @DisplayName("CREATE BRANCH")
    class CreateBranch {

        @Test
        @DisplayName("CREATE BRANCH on table syntax accepted")
        void createBranch() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'v1')");

            executeDdl("ALTER TABLE " + fqn() + " CREATE BRANCH dev_branch");
            assertThat(executeQuery("SHOW BRANCHES FROM " + fqn()))
                    .filteredOn(row -> row.getString("name").equals("dev_branch"))
                    .singleElement().satisfies(row -> {
                        assertThat(row.getString("type")).isEqualTo("BRANCH");
                        assertThat(row.getLong("snapshot_id")).isPositive();
                    });

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("CREATE BRANCH with retention options")
        void createBranchWithRetention() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1)");

            executeDdl("ALTER TABLE " + fqn()
                    + " CREATE BRANCH feature_branch RETAIN 7 DAYS");
            assertThat(executeQuery("SHOW BRANCHES FROM " + fqn()))
                    .filteredOn(row -> row.getString("name").equals("feature_branch"))
                    .singleElement().satisfies(row -> assertThat(row.getLong("max_reference_age_in_ms"))
                            .isEqualTo(java.time.Duration.ofDays(7).toMillis()));

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        void createBranchFromHistoricalSnapshotDoesNotUseMovingMain() {
            assertHistoricalBranchSource(false);
        }

        @Test
        void createBranchFromTagDoesNotUseMovingMain() {
            assertHistoricalBranchSource(true);
        }

        private void assertHistoricalBranchSource(boolean fromTag) {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1)");
            executeDdl("ALTER TABLE " + fqn() + " CREATE TAG pinned_release");
            long pinned = executeQuery("SHOW TAGS FROM " + fqn()).get(0).getLong("snapshot_id");
            executeDml("INSERT INTO " + fqn() + " VALUES (2)");
            executeDdl("CREATE BRANCH historical ON " + fqn()
                    + (fromTag ? " FROM TAG pinned_release" : " FROM SNAPSHOT " + pinned));
            assertThat(executeQuery("SHOW BRANCHES FROM " + fqn()))
                    .filteredOn(row -> row.getString("name").equals("historical"))
                    .singleElement().satisfies(row -> assertThat(row.getLong("snapshot_id")).isEqualTo(pinned));
            assertThat(executeQuery("SHOW BRANCHES FROM " + fqn()))
                    .filteredOn(row -> row.getString("name").equals("main"))
                    .singleElement().satisfies(row -> assertThat(row.getLong("snapshot_id")).isNotEqualTo(pinned));
            executeDdl("DROP TABLE " + fqn());
        }
    }

    @Nested
    @DisplayName("CREATE TAG")
    class CreateTag {

        @Test
        @DisplayName("CREATE TAG on table syntax accepted")
        void createTag() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, val VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'release')");

            executeDdl("ALTER TABLE " + fqn() + " CREATE TAG v1_0");
            assertThat(executeQuery("SHOW TAGS FROM " + fqn()))
                    .singleElement().satisfies(row -> {
                        assertThat(row.getString("name")).isEqualTo("v1_0");
                        assertThat(row.getLong("snapshot_id")).isPositive();
                    });

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("CREATE TAG on current snapshot")
        void createTagAtSnapshot() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1)");

            executeDdl("ALTER TABLE " + fqn() + " CREATE TAG release_1");
            long pinned = executeQuery("SHOW TAGS FROM " + fqn()).get(0).getLong("snapshot_id");
            executeDml("INSERT INTO " + fqn() + " VALUES (2)");
            assertThat(executeQuery("SHOW TAGS FROM " + fqn()))
                    .singleElement().satisfies(row -> assertThat(row.getLong("snapshot_id")).isEqualTo(pinned));
            assertThat(executeQuery("SHOW BRANCHES FROM " + fqn()))
                    .filteredOn(row -> row.getString("name").equals("main"))
                    .singleElement().satisfies(row -> assertThat(row.getLong("snapshot_id")).isNotEqualTo(pinned));

            executeDdl("DROP TABLE " + fqn());
        }
    }

    @Nested
    @DisplayName("DROP BRANCH/TAG")
    class DropBranchTag {

        @Test
        @DisplayName("DROP BRANCH syntax accepted")
        void dropBranch() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1)");

            executeDdl("ALTER TABLE " + fqn() + " CREATE BRANCH test_branch");
            executeDdl("ALTER TABLE " + fqn() + " DROP BRANCH test_branch");
            assertThat(executeQuery("SHOW BRANCHES FROM " + fqn()))
                    .extracting(row -> row.getString("name")).doesNotContain("test_branch");

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("DROP TAG syntax accepted")
        void dropTag() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1)");

            executeDdl("ALTER TABLE " + fqn() + " CREATE TAG test_tag");
            executeDdl("ALTER TABLE " + fqn() + " DROP TAG test_tag");
            assertThat(executeQuery("SHOW TAGS FROM " + fqn())).isEmpty();

            executeDdl("DROP TABLE " + fqn());
        }
    }

    @Nested
    class BranchOptionValidation {
        @Test
        void standaloneMutationTasksPreservePinnedTagAndRemoveReferences() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1)");
            executeDdl("CREATE TAG pinned_release ON " + fqn());
            long pinned = executeQuery("SHOW TAGS FROM " + fqn()).get(0).getLong("snapshot_id");
            executeDml("INSERT INTO " + fqn() + " VALUES (2)");
            executeDdl("CREATE BRANCH historical ON " + fqn() + " FROM TAG pinned_release");
            assertThat(executeQuery("SHOW BRANCHES FROM " + fqn()))
                    .filteredOn(row -> row.getString("name").equals("historical"))
                    .singleElement().satisfies(row -> assertThat(row.getLong("snapshot_id")).isEqualTo(pinned));
            executeDdl("DROP BRANCH historical ON " + fqn());
            executeDdl("DROP TAG pinned_release ON " + fqn());
            assertThat(executeQuery("SHOW TAGS FROM " + fqn())).isEmpty();
            assertThat(executeQuery("SHOW BRANCHES FROM " + fqn()))
                    .extracting(row -> row.getString("name")).containsExactly("main");
            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        void absentTagCannotCreateBranch() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1)");
            assertQueryFailsWithMessage("CREATE BRANCH rejected ON " + fqn() + " FROM TAG absent_release",
                    "Branch source tag not found");
            assertThat(executeQuery("SHOW BRANCHES FROM " + fqn()))
                    .extracting(row -> row.getString("name")).containsExactly("main");
            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        void invalidAndOverflowingRetentionCannotCreateBranch() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1)");
            assertQueryFailsWithMessage("ALTER TABLE " + fqn() + " CREATE BRANCH rejected RETAIN 0 DAYS",
                    "Branch reference lifetime must be positive");
            assertQueryFailsWithMessage("ALTER TABLE " + fqn()
                    + " CREATE BRANCH rejected RETAIN 9223372036854775807 DAYS", "overflow");
            assertThat(executeQuery("SHOW BRANCHES FROM " + fqn()))
                    .extracting(row -> row.getString("name")).containsExactly("main");
            executeDdl("DROP TABLE " + fqn());
        }
    }

    @Nested
    @DisplayName("SHOW BRANCHES/TAGS")
    class ShowBranchesAndTags {

        @Test
        @DisplayName("SHOW BRANCHES FROM table")
        void showBranches() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1)");

            executeDdl("ALTER TABLE " + fqn() + " CREATE BRANCH audit_branch");
            List<TypedRow> rows = executeQuery("SHOW BRANCHES FROM " + fqn());
            assertThat(rows).extracting(row -> row.getString("name"))
                    .containsExactlyInAnyOrder("main", "audit_branch");
            assertThat(rows).allSatisfy(row -> {
                assertThat(row.getString("type")).isEqualTo("BRANCH");
                assertThat(row.getLong("snapshot_id")).isPositive();
                assertThat(row.columnNames()).doesNotContain("info");
            });

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("SHOW TAGS FROM table")
        void showTags() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1)");

            executeDdl("ALTER TABLE " + fqn() + " CREATE TAG release_tag");
            List<TypedRow> rows = executeQuery("SHOW TAGS FROM " + fqn());
            assertThat(rows).singleElement().satisfies(row -> {
                assertThat(row.getString("name")).isEqualTo("release_tag");
                assertThat(row.getString("type")).isEqualTo("TAG");
                assertThat(row.getLong("snapshot_id")).isPositive();
                assertThat(row.columnNames()).doesNotContain("info");
            });

            executeDdl("DROP TABLE " + fqn());
        }
    }
}
