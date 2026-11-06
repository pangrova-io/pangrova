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
package org.pangrova.ai.worker.function.file;

import org.pangrova.ai.api.embedding.BlobReference;
import org.pangrova.ai.api.invocation.ContentPart;
import org.pangrova.ai.api.invocation.MultimodalPrompt;
import org.pangrova.types.value.ColumnValue;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Utility class for working with FILE column values (STRUCT representation).
 *
 * <p>FILE is stored as {@code STRUCT<uri VARCHAR, mime_type VARCHAR, size BIGINT, checksum VARCHAR, created_at
 * TIMESTAMP>}.
 * This helper provides methods to:
 * <ul>
 *   <li>Construct FILE struct values from components</li>
 *   <li>Extract fields from FILE struct column values</li>
 *   <li>Convert FILE struct to {@link BlobReference} for AI processing</li>
 *   <li>Build {@link MultimodalPrompt} from text + FILE</li>
 * </ul>
 *
 * <p>Field layout (positional in RowValue):
 * <ol start="0">
 *   <li>uri (String)</li>
 *   <li>mime_type (String)</li>
 *   <li>size (Long)</li>
 *   <li>checksum (String)</li>
 *   <li>created_at (LocalDateTime)</li>
 * </ol>
 */
public final class FileStructHelper {

    private FileStructHelper() {}

    /**
     * Creates a FILE struct ColumnValue from components.
     *
     * @param uri       storage URI
     * @param mimeType  MIME type
     * @param size      file size in bytes
     * @param checksum  SHA-256 checksum
     * @param createdAt creation timestamp
     * @return RowValue representing the FILE struct
     */
    public static ColumnValue.RowValue createFileStruct(
            String uri, String mimeType, long size, String checksum, LocalDateTime createdAt) {
        BlobReference reference = new BlobReference(uri, mimeType, size, checksum);
        if (createdAt == null) {
            throw new IllegalArgumentException("createdAt required");
        }
        return new ColumnValue.RowValue(List.of(
                reference.uri(),
                reference.mimeType(),
                reference.size(),
                reference.checksum(),
                createdAt
        ));
    }

    /**
     * Creates a FILE struct with MIME type validation against the table's whitelist.
     * Throws {@link org.pangrova.ai.worker.governance.MimeTypeValidator.MimeTypeNotAllowedException}
     * if the MIME type is not permitted by the table's {@code pangrova.file.allowed_types} property.
     *
     * @param uri        storage URI
     * @param mimeType   MIME type
     * @param size       file size in bytes
     * @param checksum   SHA-256 checksum
     * @param createdAt  creation timestamp
     * @param validation table-scoped validation context (may be null to skip validation)
     * @return RowValue representing the FILE struct
     */
    public static ColumnValue.RowValue createFileStructValidated(
            String uri, String mimeType, long size, String checksum, LocalDateTime createdAt,
            FileValidationContext validation) {
        if (validation != null && validation.validator() != null
                && validation.tableProperties() != null) {
            validation.validator().validate(mimeType, validation.tableProperties(), validation.tableName());
        }
        return createFileStruct(uri, mimeType, size, checksum, createdAt);
    }

    /**
     * Extracts a {@link BlobReference} from a FILE column value.
     *
     * @param fileValue the ColumnValue (expected to be RowValue representing FILE struct)
     * @return BlobReference, or null if the value is null
     * @throws IllegalArgumentException if the value is not a valid FILE struct
     */
    public static BlobReference extractBlobReference(ColumnValue fileValue) {
        if (fileValue instanceof ColumnValue.NullValue) {
            return null;
        }
        if (!(fileValue instanceof ColumnValue.RowValue rv)) {
            throw new IllegalArgumentException(
                    "Expected FILE (ROW) value, got: " + fileValue.getClass().getSimpleName());
        }
        List<?> fields = rv.fields();
        if (fields.size() < 4) {
            throw new IllegalArgumentException(
                    "Invalid FILE struct: expected at least 4 fields, got " + fields.size());
        }

        String uri = toString(fields.get(0), "uri");
        String mimeType = toString(fields.get(1), "mime_type");
        long size = toLong(fields.get(2), "size");
        String checksum = toString(fields.get(3), "checksum");

        return new BlobReference(uri, mimeType, size, checksum);
    }

    /**
     * Builds a multimodal prompt from a text instruction and a FILE column value.
     *
     * @param textPrompt the text instruction
     * @param fileValue  the FILE column value
     * @return MultimodalPrompt combining text and file reference
     */
    public static MultimodalPrompt buildMultimodalPrompt(String textPrompt, ColumnValue fileValue) {
        BlobReference ref = extractBlobReference(fileValue);
        if (ref == null) {
            return MultimodalPrompt.ofText(textPrompt);
        }
        return MultimodalPrompt.of(textPrompt, new ContentPart.FileRef(ref));
    }

    private static String toString(Object field, String name) {
        if (field == null) {
            throw new IllegalArgumentException("Invalid FILE struct: " + name + " is null");
        }
        if (field instanceof String s) {
            return s;
        }
        throw new IllegalArgumentException(
                "Invalid FILE struct: " + name + " must be VARCHAR");
    }

    private static long toLong(Object field, String name) {
        if (field == null) {
            throw new IllegalArgumentException("Invalid FILE struct: " + name + " is null");
        }
        if (field instanceof Byte || field instanceof Short
                || field instanceof Integer || field instanceof Long) {
            return ((Number) field).longValue();
        }
        try {
            return Long.parseLong(field.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid FILE struct: " + name + " must be BIGINT", e);
        }
    }
}
