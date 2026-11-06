package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Iceberg Branching and Tagging E2E tests.
 *
 * <p>Covers B8 acceptance criteria:
 * <ul>
 *   <li>AC1: CREATE BRANCH</li>
 *   <li>AC2: CREATE TAG</li>
 *   <li>AC3: tag time travel</li>
 *   <li>AC4: DROP BRANCH/TAG</li>
 * </ul>
 *
 * <p>Requirements: B8
 */
@DisplayName("Iceberg Branching and Tagging E2E Tests")
class IcebergBranchTagTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    private String tableName;

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    @Nested
    @DisplayName("3.4.5 - CREATE TAG and Time Travel")
    class CreateTagTimeTravel {

        @Test
        @DisplayName("CREATE TAG then time travel to tag verifies data at that point")
        void createTagTimeTravelVerifiesData() {
            sqlRunner.execute("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR) WITH ('format-version' = '2')");

            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (1, 'alice'), (2, 'bob')");

            sqlRunner.executeDdl("ALTER TABLE " + fqn() + " CREATE TAG v1");

            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (3, 'charlie')");

            List<TypedRow> currentRows = sqlRunner.execute(
                    "SELECT id, name FROM " + fqn() + " ORDER BY id");
            assertThat(currentRows).hasSize(3);

            List<TypedRow> tagRows = sqlRunner.execute(
                    "SELECT id, name FROM " + fqn() + " VERSION AS OF TAG 'v1' ORDER BY id");
            assertThat(tagRows).hasSize(2);
            assertThat(((Number) tagRows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(tagRows.get(0).getRaw("name")).isEqualTo("alice");
            assertThat(((Number) tagRows.get(1).getRaw("id")).longValue()).isEqualTo(2L);
            assertThat(tagRows.get(1).getRaw("name")).isEqualTo("bob");
        }

        @Test
        @DisplayName("CREATE TAG on empty table after first insert captures initial state")
        void createTagCapturesInitialState() {
            sqlRunner.execute("CREATE TABLE " + fqn()
                    + " (id BIGINT, value DOUBLE) WITH ('format-version' = '2')");

            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (1, 10.0)");

            sqlRunner.executeDdl("ALTER TABLE " + fqn() + " CREATE TAG initial");

            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (2, 20.0), (3, 30.0)");

            List<TypedRow> tagRows = sqlRunner.execute(
                    "SELECT id FROM " + fqn() + " VERSION AS OF TAG 'initial' ORDER BY id");
            assertThat(tagRows).hasSize(1);
            assertThat(((Number) tagRows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
        }
    }

    @Nested
    @DisplayName("3.4.6 - CREATE BRANCH Independent Write")
    class CreateBranchIndependentWrite {

        @Test
        @DisplayName("CREATE BRANCH captures point-in-time snapshot independent of main")
        void branchWriteDoesNotAffectMain() {
            sqlRunner.execute("CREATE TABLE " + fqn()
                    + " (id BIGINT, name VARCHAR) WITH ('format-version' = '2')");

            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (1, 'alice'), (2, 'bob')");

            sqlRunner.executeDdl("ALTER TABLE " + fqn() + " CREATE BRANCH dev");

            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (3, 'charlie')");

            List<TypedRow> mainRows = sqlRunner.execute(
                    "SELECT id FROM " + fqn() + " ORDER BY id");
            assertThat(mainRows).hasSize(3);
            assertThat(((Number) mainRows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(((Number) mainRows.get(1).getRaw("id")).longValue()).isEqualTo(2L);
            assertThat(((Number) mainRows.get(2).getRaw("id")).longValue()).isEqualTo(3L);

            List<TypedRow> branchRows = sqlRunner.execute(
                    "SELECT id FROM " + fqn() + " VERSION AS OF BRANCH 'dev' ORDER BY id");
            assertThat(branchRows).hasSize(2);
            assertThat(((Number) branchRows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(((Number) branchRows.get(1).getRaw("id")).longValue()).isEqualTo(2L);
        }

        @Test
        @DisplayName("Main branch writes do not affect named branch")
        void mainWriteDoesNotAffectBranch() {
            sqlRunner.execute("CREATE TABLE " + fqn()
                    + " (id BIGINT, status VARCHAR) WITH ('format-version' = '2')");

            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (1, 'active')");

            sqlRunner.executeDdl("ALTER TABLE " + fqn() + " CREATE BRANCH snapshot_branch");

            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (2, 'new')");

            List<TypedRow> branchRows = sqlRunner.execute(
                    "SELECT id FROM " + fqn()
                            + " VERSION AS OF BRANCH 'snapshot_branch' ORDER BY id");
            assertThat(branchRows).hasSize(1);
            assertThat(((Number) branchRows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
        }
    }

    @Nested
    @DisplayName("3.4.7 - DROP BRANCH/TAG")
    class DropBranchTag {

        @Test
        @DisplayName("DROP TAG removes the tag reference")
        void dropTagRemovesReference() {
            sqlRunner.execute("CREATE TABLE " + fqn()
                    + " (id BIGINT) WITH ('format-version' = '2')");

            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (1)");

            sqlRunner.executeDdl("ALTER TABLE " + fqn() + " CREATE TAG to_drop");

            List<TypedRow> beforeDrop = sqlRunner.execute(
                    "SELECT id FROM " + fqn() + " VERSION AS OF TAG 'to_drop'");
            assertThat(beforeDrop).hasSize(1);

            sqlRunner.executeDdl("ALTER TABLE " + fqn() + " DROP TAG to_drop");

            var error = sqlRunner.executeExpectingError(
                    "SELECT id FROM " + fqn() + " VERSION AS OF TAG 'to_drop'");
            assertThat(error.message()).containsIgnoringCase("to_drop");
        }

        @Test
        @DisplayName("DROP BRANCH removes the branch reference")
        void dropBranchRemovesReference() {
            sqlRunner.execute("CREATE TABLE " + fqn()
                    + " (id BIGINT) WITH ('format-version' = '2')");

            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (1)");

            sqlRunner.executeDdl("ALTER TABLE " + fqn() + " CREATE BRANCH temp_branch");

            List<TypedRow> beforeDrop = sqlRunner.execute(
                    "SELECT id FROM " + fqn() + " VERSION AS OF BRANCH 'temp_branch'");
            assertThat(beforeDrop).hasSize(1);

            sqlRunner.executeDdl("ALTER TABLE " + fqn() + " DROP BRANCH temp_branch");

            var error = sqlRunner.executeExpectingError(
                    "SELECT id FROM " + fqn() + " VERSION AS OF BRANCH 'temp_branch'");
            assertThat(error.message()).containsIgnoringCase("temp_branch");
        }

        @Test
        @DisplayName("DROP TAG then CREATE TAG with same name succeeds")
        void dropThenRecreateTag() {
            sqlRunner.execute("CREATE TABLE " + fqn()
                    + " (id BIGINT) WITH ('format-version' = '2')");

            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (1)");
            sqlRunner.executeDdl("ALTER TABLE " + fqn() + " CREATE TAG reusable");

            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (2)");
            sqlRunner.executeDdl("ALTER TABLE " + fqn() + " DROP TAG reusable");

            sqlRunner.executeDdl("ALTER TABLE " + fqn() + " CREATE TAG reusable");

            List<TypedRow> tagRows = sqlRunner.execute(
                    "SELECT id FROM " + fqn() + " VERSION AS OF TAG 'reusable' ORDER BY id");
            assertThat(tagRows).hasSize(2);
        }
    }
}
