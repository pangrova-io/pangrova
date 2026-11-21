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
package org.pangrova.coordinator.query.statement.dml;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.pangrova.common.exception.ValidationException;
import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Literal;
import org.pangrova.parser.api.ast.expression.FunctionCall;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link FileColumnMimeValidator} (F-MM-GOV MIME whitelist).
 * Uses real AST + metadata objects (no mocks) per project testing rules.
 */
class FileColumnMimeValidatorTest {

    private static final List<String> COLUMNS = List.of("id", "img");

    private static ConnectorTableMetadata table(Map<String, String> props) {
        return new ConnectorTableMetadata(new SchemaTableName("s", "t"), COLUMNS, props);
    }

    private static List<Expression> fileFromUrlRow(long id, String url) {
        return List.of(
                Literal.ofInteger(id),
                new FunctionCall(Identifier.of("FILE_FROM_URL"),
                        List.<Expression>of(Literal.ofString(url))));
    }

    @Test
    @DisplayName("mimeFromUrl derives MIME from the URL file extension")
    void mimeFromUrlDerivesByExtension() {
        assertThat(FileColumnMimeValidator.mimeFromUrl("s3://b/photo.jpg")).isEqualTo("image/jpeg");
        assertThat(FileColumnMimeValidator.mimeFromUrl("s3://b/DOC.PDF")).isEqualTo("application/pdf");
        assertThat(FileColumnMimeValidator.mimeFromUrl("https://h/p/a.png?v=1#frag")).isEqualTo("image/png");
        assertThat(FileColumnMimeValidator.mimeFromUrl("s3://b/audio.mp3")).isEqualTo("audio/mpeg");
        assertThat(FileColumnMimeValidator.mimeFromUrl("s3://b/noext")).isEqualTo(FileColumnMimeValidator.UNKNOWN_MIME);
        assertThat(FileColumnMimeValidator.mimeFromUrl(null)).isNull();
    }

    @Test
    @DisplayName("rejects a FILE value whose MIME is not in the table whitelist")
    void rejectsNonWhitelistedMime() {
        ConnectorTableMetadata meta = table(Map.of(
                FileColumnMimeValidator.ALLOWED_TYPES_PROPERTY, "image/jpeg,image/png",
                FileColumnMimeValidator.FILE_COLUMN_PROPERTY_PREFIX + "img", "true"));
        assertThatThrownBy(() -> FileColumnMimeValidator.validate(
                meta, COLUMNS, List.of(fileFromUrlRow(1, "s3://b/doc.pdf"))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("mime");
    }

    @Test
    @DisplayName("allows a FILE value whose MIME is in the whitelist")
    void allowsWhitelistedMime() {
        ConnectorTableMetadata meta = table(Map.of(
                FileColumnMimeValidator.ALLOWED_TYPES_PROPERTY, "image/jpeg,image/png",
                FileColumnMimeValidator.FILE_COLUMN_PROPERTY_PREFIX + "img", "true"));
        assertThatCode(() -> FileColumnMimeValidator.validate(
                meta, COLUMNS, List.of(fileFromUrlRow(1, "s3://b/photo.jpg"))))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("no whitelist declared -> validation is a no-op")
    void noWhitelistIsNoOp() {
        ConnectorTableMetadata meta = table(Map.of(
                FileColumnMimeValidator.FILE_COLUMN_PROPERTY_PREFIX + "img", "true"));
        assertThatCode(() -> FileColumnMimeValidator.validate(
                meta, COLUMNS, List.of(fileFromUrlRow(1, "s3://b/doc.pdf"))))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("FILE_FROM_BYTES uses its explicit MIME argument for the whitelist check")
    void fileFromBytesUsesExplicitMime() {
        ConnectorTableMetadata meta = table(Map.of(
                FileColumnMimeValidator.ALLOWED_TYPES_PROPERTY, "application/pdf",
                FileColumnMimeValidator.FILE_COLUMN_PROPERTY_PREFIX + "img", "true"));
        List<Expression> row = List.of(
                Literal.ofInteger(1),
                new FunctionCall(Identifier.of("FILE_FROM_BYTES"),
                        List.<Expression>of(Literal.ofBinaryHex("255044462D"),
                                Literal.ofString("application/pdf"))));
        assertThatCode(() -> FileColumnMimeValidator.validate(meta, COLUMNS, List.of(row)))
                .doesNotThrowAnyException();

        ConnectorTableMetadata imgOnly = table(Map.of(
                FileColumnMimeValidator.ALLOWED_TYPES_PROPERTY, "image/jpeg",
                FileColumnMimeValidator.FILE_COLUMN_PROPERTY_PREFIX + "img", "true"));
        assertThatThrownBy(() -> FileColumnMimeValidator.validate(imgOnly, COLUMNS, List.of(row)))
                .isInstanceOf(ValidationException.class);
    }
}
