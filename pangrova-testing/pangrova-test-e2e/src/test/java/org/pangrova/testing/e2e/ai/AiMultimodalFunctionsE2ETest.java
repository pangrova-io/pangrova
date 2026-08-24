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
 * Full-stack E2E tests for AI Multimodal functions and FILE helper functions.
 *
 * <p>Validates SQL parsing, planning, and execution (or graceful error) for:
 * <ul>
 *   <li>{@code FILE_FROM_URL(url)} — construct a FILE struct from a URL</li>
 *   <li>{@code FILE_FROM_BYTES(bytes, mime_type)} — construct a FILE struct from raw bytes</li>
 *   <li>{@code AI_DESCRIBE_IMAGE(model, file)} — image description</li>
 *   <li>{@code AI_PARSE_DOCUMENT(model, file)} — document parsing</li>
 *   <li>{@code AI_TRANSCRIBE(model, file)} — audio transcription</li>
 *   <li>{@code AI_OCR(model, file)} — optical character recognition</li>
 * </ul>
 *
 * <p>Tests verify that the SQL parser and planner accept the function syntax.
 * Actual model inference may fail without a running inference endpoint, so
 * tests use {@code executeExpectingError} where appropriate and assert that
 * failures are NOT parser/syntax errors but rather runtime/model errors.</p>
 */
@E2ETest
@DisplayName("AI Multimodal Functions Full-Stack E2E")
class AiMultimodalFunctionsE2ETest extends PangrovaE2ETestBase {

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
        tableName = "t_multimodal_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    // -----------------------------------------------------------------------
    // FILE Helper Functions
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("FILE_FROM_URL Function")
    class FileFromUrl {

        @Test
        @DisplayName("FILE_FROM_URL syntax is accepted by parser and planner")
        void fileFromUrlSyntaxAccepted() {
            String sql = "SELECT FILE_FROM_URL('https://example.com/image.png')";
            try {
                List<TypedRow> rows = executeQuery(sql);
                assertThat(rows).isNotNull();
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                // Must NOT be a syntax error — runtime/model errors are acceptable
                assertThat(error).doesNotContainIgnoringCase("syntax error");
                assertThat(error).doesNotContainIgnoringCase("No match found for function signature");
            }
        }

        @Test
        @DisplayName("FILE_FROM_URL with S3 URI is accepted")
        void fileFromUrlS3UriAccepted() {
            String sql = "SELECT FILE_FROM_URL('s3://my-bucket/documents/report.pdf')";
            try {
                List<TypedRow> rows = executeQuery(sql);
                assertThat(rows).isNotNull();
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
                assertThat(error).doesNotContainIgnoringCase("No match found for function signature");
            }
        }

        @Test
        @DisplayName("FILE_FROM_URL in table context with INSERT/SELECT")
        void fileFromUrlInTableContext() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, doc_uri VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES (1, 'https://example.com/doc.pdf')");

            String sql = "SELECT id, FILE_FROM_URL(doc_uri) FROM " + fqn();
            try {
                List<TypedRow> rows = executeQuery(sql);
                assertThat(rows).isNotNull();
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
            }
            executeDdl("DROP TABLE " + fqn());
        }
    }

    @Nested
    @DisplayName("FILE_FROM_BYTES Function")
    class FileFromBytes {

        @Test
        @DisplayName("FILE_FROM_BYTES syntax is accepted by parser and planner")
        void fileFromBytesSyntaxAccepted() {
            String sql = "SELECT FILE_FROM_BYTES(X'89504E47', 'image/png')";
            try {
                List<TypedRow> rows = executeQuery(sql);
                assertThat(rows).isNotNull();
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
                assertThat(error).doesNotContainIgnoringCase("No match found for function signature");
            }
        }

        @Test
        @DisplayName("FILE_FROM_BYTES with application/pdf MIME type")
        void fileFromBytesPdfMimeType() {
            String sql = "SELECT FILE_FROM_BYTES(X'255044462D', 'application/pdf')";
            try {
                List<TypedRow> rows = executeQuery(sql);
                assertThat(rows).isNotNull();
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
            }
        }
    }

    // -----------------------------------------------------------------------
    // AI Multimodal Functions
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("AI_DESCRIBE_IMAGE Function")
    class AiDescribeImage {

        @Test
        @DisplayName("AI_DESCRIBE_IMAGE syntax is accepted by parser")
        void describeImageSyntaxAccepted() {
            String sql = "SELECT AI_DESCRIBE_IMAGE('gpt-4-vision', "
                    + "FILE_FROM_URL('https://example.com/photo.jpg'))";
            try {
                List<TypedRow> rows = executeQuery(sql);
                assertThat(rows).isNotNull();
                assertThat(rows).hasSize(1);
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
                assertThat(error).doesNotContainIgnoringCase("No match found for function signature");
            }
        }

        @Test
        @DisplayName("AI_DESCRIBE_IMAGE in SELECT with table data")
        void describeImageWithTableData() {
            executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, image_url VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'https://example.com/img1.jpg'), "
                    + "(2, 'https://example.com/img2.png')");

            String sql = "SELECT id, AI_DESCRIBE_IMAGE('gpt-4-vision', "
                    + "FILE_FROM_URL(image_url)) AS description FROM " + fqn();
            try {
                List<TypedRow> rows = executeQuery(sql);
                assertThat(rows).hasSize(2);
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                // Runtime errors (no model endpoint) are OK; syntax errors are not
                assertThat(error).doesNotContainIgnoringCase("syntax error");
            }
            executeDdl("DROP TABLE " + fqn());
        }
    }

    @Nested
    @DisplayName("AI_PARSE_DOCUMENT Function")
    class AiParseDocument {

        @Test
        @DisplayName("AI_PARSE_DOCUMENT syntax is accepted by parser")
        void parseDocumentSyntaxAccepted() {
            String sql = "SELECT AI_PARSE_DOCUMENT('gpt-4', "
                    + "FILE_FROM_URL('s3://docs/report.pdf'))";
            try {
                List<TypedRow> rows = executeQuery(sql);
                assertThat(rows).isNotNull();
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
                assertThat(error).doesNotContainIgnoringCase("No match found for function signature");
            }
        }

        @Test
        @DisplayName("AI_PARSE_DOCUMENT with WHERE clause filtering")
        void parseDocumentWithFilter() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, doc_type VARCHAR, doc_url VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'pdf', 's3://docs/a.pdf'), "
                    + "(2, 'docx', 's3://docs/b.docx'), "
                    + "(3, 'pdf', 's3://docs/c.pdf')");

            String sql = "SELECT id, AI_PARSE_DOCUMENT('gpt-4', "
                    + "FILE_FROM_URL(doc_url)) AS content "
                    + "FROM " + fqn() + " WHERE doc_type = 'pdf'";
            try {
                List<TypedRow> rows = executeQuery(sql);
                assertThat(rows).hasSize(2);
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
            }
            executeDdl("DROP TABLE " + fqn());
        }
    }

    @Nested
    @DisplayName("AI_TRANSCRIBE Function")
    class AiTranscribe {

        @Test
        @DisplayName("AI_TRANSCRIBE syntax is accepted by parser")
        void transcribeSyntaxAccepted() {
            String sql = "SELECT AI_TRANSCRIBE('whisper-1', "
                    + "FILE_FROM_URL('https://example.com/audio.mp3'))";
            try {
                List<TypedRow> rows = executeQuery(sql);
                assertThat(rows).isNotNull();
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
                assertThat(error).doesNotContainIgnoringCase("No match found for function signature");
            }
        }

        @Test
        @DisplayName("AI_TRANSCRIBE with table reference")
        void transcribeWithTableData() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, audio_url VARCHAR, language VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'https://example.com/en.mp3', 'en'), "
                    + "(2, 'https://example.com/zh.mp3', 'zh')");

            String sql = "SELECT id, AI_TRANSCRIBE('whisper-1', "
                    + "FILE_FROM_URL(audio_url)) AS transcript "
                    + "FROM " + fqn() + " WHERE language = 'en'";
            try {
                List<TypedRow> rows = executeQuery(sql);
                assertThat(rows).hasSize(1);
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
            }
            executeDdl("DROP TABLE " + fqn());
        }
    }

    @Nested
    @DisplayName("AI_OCR Function")
    class AiOcr {

        @Test
        @DisplayName("AI_OCR syntax is accepted by parser")
        void ocrSyntaxAccepted() {
            String sql = "SELECT AI_OCR('gpt-4-vision', "
                    + "FILE_FROM_URL('https://example.com/receipt.jpg'))";
            try {
                List<TypedRow> rows = executeQuery(sql);
                assertThat(rows).isNotNull();
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
                assertThat(error).doesNotContainIgnoringCase("No match found for function signature");
            }
        }

        @Test
        @DisplayName("AI_OCR result used in subsequent query")
        void ocrResultInSubsequentQuery() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, scan_url VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'https://example.com/scan1.png'), "
                    + "(2, 'https://example.com/scan2.tiff')");

            String sql = "SELECT id, AI_OCR('gpt-4-vision', "
                    + "FILE_FROM_URL(scan_url)) AS extracted_text "
                    + "FROM " + fqn();
            try {
                List<TypedRow> rows = executeQuery(sql);
                assertThat(rows).hasSize(2);
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
            }
            executeDdl("DROP TABLE " + fqn());
        }
    }

    // -----------------------------------------------------------------------
    // Combined Scenarios
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Combined Multimodal Workflows")
    class CombinedWorkflows {

        @Test
        @DisplayName("Multiple AI functions in single SELECT")
        void multipleAiFunctionsInSelect() {
            executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, file_url VARCHAR)");
            executeDml("INSERT INTO " + fqn() + " VALUES "
                    + "(1, 'https://example.com/mixed.pdf')");

            String sql = "SELECT id, "
                    + "AI_DESCRIBE_IMAGE('gpt-4-vision', FILE_FROM_URL(file_url)) AS img_desc, "
                    + "AI_OCR('gpt-4-vision', FILE_FROM_URL(file_url)) AS ocr_text "
                    + "FROM " + fqn();
            try {
                List<TypedRow> rows = executeQuery(sql);
                assertThat(rows).isNotNull();
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
            }
            executeDdl("DROP TABLE " + fqn());
        }

        @Test
        @DisplayName("FILE_FROM_BYTES used with AI_OCR")
        void fileFromBytesWithOcr() {
            String sql = "SELECT AI_OCR('gpt-4-vision', "
                    + "FILE_FROM_BYTES(X'89504E470D0A1A0A', 'image/png'))";
            try {
                List<TypedRow> rows = executeQuery(sql);
                assertThat(rows).isNotNull();
            } catch (AssertionError e) {
                String error = executeExpectingError(sql);
                assertThat(error).doesNotContainIgnoringCase("syntax error");
            }
        }
    }
}
