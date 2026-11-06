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
package org.pangrova.testing.integration.e2e.sql;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full-stack E2E tests for session management.
 *
 * <p>Validates SET property = value and session property queries.</p>
 */
@E2ETest
@DisplayName("Session Management Full-Stack E2E")
class SessionManagementE2ETest extends PangrovaE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @Nested
    @DisplayName("SET Property")
    class SetProperty {

        @Test
        @DisplayName("SET session property syntax is accepted")
        void setSessionProperty() {
            String sql = "SET 'query.max-execution-time' = '300s'";
            executeDdl(sql);
        }

        @Test
        @DisplayName("SET catalog property")
        void setCatalogProperty() {
            String sql = "SET 'catalog.default' = '" + CATALOG + "'";
            executeDdl(sql);
        }

        @Test
        @DisplayName("SET multiple properties in sequence")
        void setMultipleProperties() {
            String sql1 = "SET 'query.max-memory' = '4GB'";
            String sql2 = "SET 'task.concurrency' = '16'";
            executeDdl(sql1);
            executeDdl(sql2);
        }
    }

    @Nested
    @DisplayName("SHOW Session Properties")
    class ShowProperties {

        @Test
        @DisplayName("SHOW SESSION syntax is accepted")
        void showSession() {
            String sql = "SHOW SESSION";
            List<TypedRow> rows = executeQuery(sql);
            assertThat(rows).isNotNull();
        }
    }
}
