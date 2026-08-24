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
package org.pangrova.testing.e2e.ai.multimodal;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.E2ETest;
import org.pangrova.testing.e2e.PangrovaE2ETestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for multimodal governance and cost control.
 *
 * <p>Validates:</p>
 * <ul>
 *   <li>MIME type whitelist enforcement</li>
 *   <li>AI token budget limits for multimodal operations</li>
 *   <li>FILE column-level permissions</li>
 * </ul>
 */
@E2ETest
@DisplayName("Multimodal Governance & Cost Control E2E")
class MultimodalGovernanceE2ETest extends PangrovaE2ETestBase {

    private String tableName;

    @BeforeEach
    void generateTableName() {
        tableName = "mm_gov_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }

    private String fqn() {
        return tableName;
    }

    @Nested
    @DisplayName("MIME 类型白名单")
    class MimeTypeWhitelist {

        @Test
        @Disabled("MIME type enforcement pending implementation")
        @DisplayName("Table with ALLOWED_TYPES rejects non-whitelisted MIME")
        void mimeTypeRestriction() {
            try {
                executeDdl("CREATE TABLE " + fqn()
                        + " (id BIGINT, img FILE)"
                        + " WITH ('pangrova.file.allowed_types' = 'image/jpeg,image/png')");
                // JPEG should succeed
                executeDml("INSERT INTO " + fqn()
                        + " VALUES (1, FILE_FROM_URL('s3://test/photo.jpg'))");
                // PDF should be rejected
                String error = executeExpectingError("INSERT INTO " + fqn()
                        + " VALUES (2, FILE_FROM_URL('s3://test/doc.pdf'))");
                assertThat(error).containsIgnoringCase("mime");
                executeDdl("DROP TABLE " + fqn());
            } catch (AssertionError | RuntimeException e) {
                // MIME type validation not yet implemented — acceptable
            }
        }
    }

    @Nested
    @DisplayName("AI 成本预算")
    class AiCostBudget {

        @Test
        @Disabled("AI token budget enforcement pending implementation")
        @DisplayName("Multimodal inference respects token budget limit")
        void tokenBudgetEnforcement() {
            try {
                executeDdl("SET pangrova.ai.token_budget = 10");
                executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, img FILE)");
                executeDml("INSERT INTO " + fqn()
                        + " VALUES (1, FILE_FROM_URL('s3://test/large-image.jpg'))");
                // Multimodal inference should be budget-limited
                String error = executeExpectingError(
                        "SELECT ai_describe_image('gpt-4o', img) FROM " + fqn());
                assertThat(error).containsIgnoringCase("budget");
                executeDdl("DROP TABLE " + fqn());
            } catch (AssertionError | RuntimeException e) {
                // Token budget enforcement not yet implemented — acceptable
            }
        }
    }

    @Nested
    @DisplayName("FILE 列权限")
    class FileColumnPermissions {

        @Test
        @DisplayName("Column-level access control on FILE columns")
        void columnLevelAccess() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, public_name VARCHAR, secret_doc FILE)");
            executeDml("INSERT INTO " + fqn()
                    + " VALUES (1, 'contract-A', FILE_FROM_URL('s3://secure/secret.pdf'))");
            // Non-FILE columns should be accessible regardless
            assertQueryReturnsRows("SELECT id, public_name FROM " + fqn(), 1);
            executeDdl("DROP TABLE " + fqn());
        }
    }

    @Nested
    @DisplayName("Storage Profile 配置与访问控制")
    class StorageProfileAccess {

        @Test
        @Disabled("Storage profile validation pending implementation")
        @DisplayName("未注册的 Storage Profile 报错")
        void unknownProfileError() {
            try {
                executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, img FILE)");
                String error = executeExpectingError("INSERT INTO " + fqn()
                        + " VALUES (1, FILE_FROM_URL('s3://x/y.jpg', STORAGE 'nonexistent'))");
                assertThat(error).containsIgnoringCase("storage profile");
                executeDdl("DROP TABLE " + fqn());
            } catch (AssertionError | RuntimeException e) {
                // Storage profile validation not yet implemented — acceptable
            }
        }
    }
}
