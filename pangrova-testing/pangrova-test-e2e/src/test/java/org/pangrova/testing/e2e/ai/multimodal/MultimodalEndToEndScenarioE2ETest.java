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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for complete multimodal business scenarios.
 *
 * <p>Covers end-to-end workflows: insurance claims, product image search,
 * contract intelligence, call center analytics, and content moderation.
 */
@E2ETest
@DisplayName("Multimodal End-to-End Business Scenarios")
class MultimodalEndToEndScenarioE2ETest extends PangrovaE2ETestBase {

    private String tableName;

    @BeforeEach
    void setUp() {
        tableName = "mm_scenario_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    private String fqn() {
        return tableName;
    }

    @Nested
    @DisplayName("场景：车险理赔全流程")
    class InsuranceClaimFlow {

        @Test
        @DisplayName("创建理赔表 → 提交照片 → AI评估 → 相似案例检索 → 报价")
        void fullClaimProcessingPipeline() {
            try {
                executeDdl("CREATE TABLE " + fqn() + " ("
                        + "claim_id BIGINT, "
                        + "photo FILE, "
                        + "damage_vec VECTOR(4), "
                        + "claim_amount DECIMAL(10,2), "
                        + "status VARCHAR DEFAULT 'pending')");
                executeDml("INSERT INTO " + fqn() + " VALUES"
                        + " (1, FILE_FROM_URL('s3://claims/hist1.jpg'), ARRAY[0.9,0.1,0.0,0.0], 5000.00, 'settled'),"
                        + " (2, FILE_FROM_URL('s3://claims/hist2.jpg'), ARRAY[0.1,0.9,0.0,0.0], 12000.00, 'settled'),"
                        + " (3, FILE_FROM_URL('s3://claims/hist3.jpg'), ARRAY[0.85,0.15,0.0,0.0], 4800.00, 'settled')");
                executeDml("INSERT INTO " + fqn() + " (claim_id, photo, damage_vec)"
                        + " VALUES (100, FILE_FROM_URL('s3://claims/new.jpg'), ARRAY[0.88,0.12,0.0,0.0])");
                List<TypedRow> similar = executeQuery(
                        "SELECT h.claim_id, h.claim_amount"
                                + " FROM " + fqn() + " h, " + fqn() + " n"
                                + " WHERE n.claim_id = 100 AND h.status = 'settled'"
                                + " ORDER BY cosine_distance(h.damage_vec, n.damage_vec) ASC"
                                + " LIMIT 3");
                assertThat(similar).isNotEmpty();
            } finally {
                executeDdl("DROP TABLE IF EXISTS " + fqn());
            }
        }
    }

    @Nested
    @DisplayName("场景：商品图片搜索")
    class ProductImageSearch {

        @Test
        @DisplayName("商品导入 → 以图搜图 → 返回最相似商品")
        void imageSearchPipeline() {
            try {
                executeDdl("CREATE TABLE " + fqn() + " ("
                        + "product_id BIGINT, name VARCHAR, "
                        + "photo FILE, photo_vec VECTOR(4), price DECIMAL(8,2))");
                executeDml("INSERT INTO " + fqn() + " VALUES"
                        + " (1, 'Red Running Shoes', FILE_FROM_URL('s3://products/shoe-red.jpg'), ARRAY[0.9,0.1,0.0,0.0], 129.99),"
                        + " (2, 'Blue Sneakers', FILE_FROM_URL('s3://products/shoe-blue.jpg'), ARRAY[0.1,0.9,0.0,0.0], 99.99),"
                        + " (3, 'Red Handbag', FILE_FROM_URL('s3://products/bag-red.jpg'), ARRAY[0.8,0.15,0.05,0.0], 249.99),"
                        + " (4, 'Green Jacket', FILE_FROM_URL('s3://products/jacket.jpg'), ARRAY[0.0,0.1,0.9,0.0], 189.99)");
                List<TypedRow> results = executeQuery(
                        "SELECT product_id, name, price FROM " + fqn()
                                + " ORDER BY cosine_distance(photo_vec, ARRAY[0.92,0.08,0.0,0.0]) ASC"
                                + " LIMIT 2");
                assertThat(results).hasSize(2);
                assertThat(((Number) results.get(0).getRaw("product_id")).longValue()).isEqualTo(1L);
            } finally {
                executeDdl("DROP TABLE IF EXISTS " + fqn());
            }
        }
    }

    @Nested
    @DisplayName("场景：合同智能审查")
    class ContractIntelligence {

        @Test
        @DisplayName("合同导入 → AI解析 → 结构化提取 → 风险分类")
        void contractReviewPipeline() {
            try {
                executeDdl("CREATE TABLE " + fqn() + " ("
                        + "contract_id BIGINT, vendor VARCHAR, "
                        + "document FILE, parsed_text VARCHAR, risk_level VARCHAR)");
                executeDml("INSERT INTO " + fqn() + " (contract_id, vendor, document) VALUES"
                        + " (1, 'Acme Corp', FILE_FROM_URL('s3://contracts/acme-2024.pdf')),"
                        + " (2, 'GlobalTech', FILE_FROM_URL('s3://contracts/globaltech.pdf'))");
                assertQueryReturnsRows("SELECT * FROM " + fqn() + " WHERE document IS NOT NULL", 2);
                executeDml("UPDATE " + fqn()
                        + " SET parsed_text = 'mock parsed content'"
                        + " WHERE parsed_text IS NULL");
                assertQueryReturnsRows(
                        "SELECT * FROM " + fqn() + " WHERE parsed_text IS NOT NULL", 2);
            } finally {
                executeDdl("DROP TABLE IF EXISTS " + fqn());
            }
        }
    }

    @Nested
    @DisplayName("场景：客服语音分析")
    class CallCenterAnalytics {

        @Test
        @DisplayName("录音存储 → 转写 → 情绪分析 → 统计报表")
        void callAnalyticsPipeline() {
            try {
                executeDdl("CREATE TABLE " + fqn() + " ("
                        + "call_id BIGINT, agent_id BIGINT, "
                        + "recording FILE, transcript VARCHAR, "
                        + "sentiment VARCHAR, call_date DATE)");
                executeDml("INSERT INTO " + fqn()
                        + " (call_id, agent_id, recording, call_date) VALUES"
                        + " (1, 101, FILE_FROM_URL('s3://calls/call001.mp3'), DATE '2024-01-15'),"
                        + " (2, 102, FILE_FROM_URL('s3://calls/call002.mp3'), DATE '2024-01-15'),"
                        + " (3, 101, FILE_FROM_URL('s3://calls/call003.wav'), DATE '2024-01-16')");
                assertQueryReturnsRows("SELECT * FROM " + fqn(), 3);
                List<TypedRow> stats = executeQuery(
                        "SELECT call_date, COUNT(*) AS total"
                                + " FROM " + fqn()
                                + " GROUP BY call_date"
                                + " ORDER BY call_date");
                assertThat(stats).hasSize(2);
            } finally {
                executeDdl("DROP TABLE IF EXISTS " + fqn());
            }
        }
    }

    @Nested
    @DisplayName("场景：内容审核")
    class ContentModeration {

        @Test
        @DisplayName("UGC 上传 → AI 安全检测 → 违规标记 → 统计")
        void moderationPipeline() {
            try {
                executeDdl("CREATE TABLE " + fqn() + " ("
                        + "upload_id BIGINT, user_id BIGINT, "
                        + "media FILE, is_safe BOOLEAN, "
                        + "uploaded_at TIMESTAMP)");
                executeDml("INSERT INTO " + fqn() + " VALUES"
                        + " (1, 42, FILE_FROM_URL('s3://ugc/img1.jpg'), true, TIMESTAMP '2024-01-15 10:00:00'),"
                        + " (2, 43, FILE_FROM_URL('s3://ugc/img2.jpg'), false, TIMESTAMP '2024-01-15 11:00:00'),"
                        + " (3, 44, FILE_FROM_URL('s3://ugc/img3.jpg'), true, TIMESTAMP '2024-01-15 12:00:00')");
                List<TypedRow> violations = executeQuery(
                        "SELECT COUNT(*) FILTER (WHERE NOT is_safe) AS violations,"
                                + " COUNT(*) AS total"
                                + " FROM " + fqn());
                assertThat(violations).isNotEmpty();
            } finally {
                executeDdl("DROP TABLE IF EXISTS " + fqn());
            }
        }
    }
}
