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
 * Full-stack E2E tests for Storage Profile management through SQL.
 *
 * <p>Validates CREATE STORAGE PROFILE, DROP STORAGE PROFILE,
 * and SHOW STORAGE PROFILES statements.</p>
 */
@E2ETest
@DisplayName("Storage Profile Full-Stack E2E")
class StorageProfileE2ETest extends PangrovaE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";
    private String profileName;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void assignProfileName(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        profileName = "sp_" + method + "_" + System.nanoTime();
    }

    @Nested
    @DisplayName("CREATE STORAGE PROFILE")
    class CreateStorageProfile {

        @Test
        @DisplayName("CREATE STORAGE PROFILE with S3 config syntax accepted")
        void createProfileS3() {
            String sql = "CREATE STORAGE PROFILE " + profileName
                    + " WITH ('type'='S3', 'bucket'='my-bucket', 'region'='us-east-1')";
            executeDdl(sql);
            // Verify profile appears in SHOW
            List<TypedRow> rows = executeQuery("SHOW STORAGE PROFILES");
            assertThat(rows).anyMatch(r -> profileName.equals(r.getRaw("name")));
            executeDdl("DROP STORAGE PROFILE IF EXISTS " + profileName);
        }

        @Test
        @DisplayName("CREATE STORAGE PROFILE with GCS config")
        void createProfileGcs() {
            String sql = "CREATE STORAGE PROFILE " + profileName
                    + " WITH ('type'='GCS', 'bucket'='my-gcs-bucket', 'project'='my-project')";
            executeDdl(sql);
            List<TypedRow> rows = executeQuery("SHOW STORAGE PROFILES");
            assertThat(rows).anyMatch(r -> profileName.equals(r.getRaw("name")));
            executeDdl("DROP STORAGE PROFILE IF EXISTS " + profileName);
        }
    }

    @Nested
    @DisplayName("DROP STORAGE PROFILE")
    class DropStorageProfile {

        @Test
        @DisplayName("DROP STORAGE PROFILE syntax accepted")
        void dropProfile() {
            // First create, then drop
            executeDdl("CREATE STORAGE PROFILE " + profileName
                    + " WITH ('type'='S3', 'bucket'='drop-test', 'region'='us-west-2')");
            executeDdl("DROP STORAGE PROFILE " + profileName);
            // Verify profile is gone
            List<TypedRow> rows = executeQuery("SHOW STORAGE PROFILES");
            assertThat(rows).noneMatch(r -> profileName.equals(r.getRaw("name")));
        }

        @Test
        @DisplayName("DROP STORAGE PROFILE IF EXISTS does not error")
        void dropProfileIfExists() {
            // DROP IF EXISTS for non-existent profile should succeed silently
            executeDdl("DROP STORAGE PROFILE IF EXISTS " + profileName);
        }
    }

    @Nested
    @DisplayName("SHOW STORAGE PROFILES")
    class ShowStorageProfiles {

        @Test
        @DisplayName("SHOW STORAGE PROFILES syntax accepted")
        void showProfiles() {
            List<TypedRow> rows = executeQuery("SHOW STORAGE PROFILES");
            assertThat(rows).isNotNull();
        }
    }
}
