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

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("CREATE BRANCH with retention options")
        void createBranchWithRetention() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1)");

            executeDdl("ALTER TABLE " + fqn()
                    + " CREATE BRANCH feature_branch RETAIN 7 DAYS");

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

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("CREATE TAG on current snapshot")
        void createTagAtSnapshot() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1)");

            executeDdl("ALTER TABLE " + fqn() + " CREATE TAG release_1");

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

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("DROP TAG syntax accepted")
        void dropTag() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1)");

            executeDdl("ALTER TABLE " + fqn() + " CREATE TAG test_tag");
            executeDdl("ALTER TABLE " + fqn() + " DROP TAG test_tag");

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

            List<TypedRow> rows = executeQuery("SHOW BRANCHES FROM " + fqn());
            assertThat(rows).isNotNull();

            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("SHOW TAGS FROM table")
        void showTags() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1)");

            List<TypedRow> rows = executeQuery("SHOW TAGS FROM " + fqn());
            assertThat(rows).isNotNull();

            executeDdl("DROP TABLE " + fqn());
        }
    }
}
