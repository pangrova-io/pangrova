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
package org.pangrova.worker.ai.function;

import org.pangrova.spi.ai.embedding.BlobReference;
import org.pangrova.spi.ai.inference.ContentPart;
import org.pangrova.spi.ai.inference.MultimodalPrompt;
import org.pangrova.spi.connector.data.ColumnValue;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    /** MIME types commonly mapped from file extensions. */
    private static final Map<String, String> EXTENSION_TO_MIME = Map.ofEntries(
            Map.entry("jpg", "image/jpeg"),
            Map.entry("jpeg", "image/jpeg"),
            Map.entry("png", "image/png"),
            Map.entry("gif", "image/gif"),
            Map.entry("webp", "image/webp"),
            Map.entry("svg", "image/svg+xml"),
            Map.entry("bmp", "image/bmp"),
            Map.entry("tiff", "image/tiff"),
            Map.entry("tif", "image/tiff"),
            Map.entry("pdf", "application/pdf"),
            Map.entry("doc", "application/msword"),
            Map.entry("docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
            Map.entry("xls", "application/vnd.ms-excel"),
            Map.entry("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            Map.entry("txt", "text/plain"),
            Map.entry("csv", "text/csv"),
            Map.entry("json", "application/json"),
            Map.entry("xml", "application/xml"),
            Map.entry("mp3", "audio/mpeg"),
            Map.entry("wav", "audio/wav"),
            Map.entry("ogg", "audio/ogg"),
            Map.entry("flac", "audio/flac"),
            Map.entry("mp4", "video/mp4"),
            Map.entry("avi", "video/x-msvideo"),
            Map.entry("mov", "video/quicktime"),
            Map.entry("zip", "application/zip"),
            Map.entry("gz", "application/gzip"),
            Map.entry("tar", "application/x-tar")
    );

    /** Allowed URL schemes for FILE storage. */
    private static final Set<String> ALLOWED_SCHEMES = Set.of(
            "s3", "oss", "gs", "gcs", "adls", "abfs", "abfss", "hdfs", "http", "https", "blob");

    private FileStructHelper() {}

    /**
     * Creates a FILE struct ColumnValue from components.
     *
     * @param uri       storage URI
     * @param mimeType  MIME type
     * @param size      file size in bytes (0 if unknown)
     * @param checksum  SHA-256 checksum (empty string if not computed)
     * @param createdAt creation timestamp
     * @return RowValue representing the FILE struct
     */
    public static ColumnValue.RowValue createFileStruct(
            String uri, String mimeType, long size, String checksum, LocalDateTime createdAt) {
        return new ColumnValue.RowValue(List.of(
                uri,
                mimeType,
                size,
                checksum,
                createdAt
        ));
    }

    /**
     * Creates a FILE struct with MIME type validation against the table's whitelist.
     * Throws {@link org.pangrova.worker.ai.governance.MimeTypeValidator.MimeTypeNotAllowedException}
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

        // BlobReference requires non-blank checksum; use placeholder if empty
        if (checksum == null || checksum.isBlank()) {
            checksum = "pending";
        }

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

    /**
     * Infers MIME type from a file URL based on extension.
     *
     * @param url the file URL
     * @return inferred MIME type, or "application/octet-stream" if unknown
     */
    public static String inferMimeType(String url) {
        if (url == null || url.isBlank()) {
            return "application/octet-stream";
        }
        // Strip query params and fragments
        String path = url;
        int queryIdx = path.indexOf('?');
        if (queryIdx > 0) {
            path = path.substring(0, queryIdx);
        }
        int fragIdx = path.indexOf('#');
        if (fragIdx > 0) {
            path = path.substring(0, fragIdx);
        }
        // Extract extension
        int lastDot = path.lastIndexOf('.');
        int lastSlash = path.lastIndexOf('/');
        if (lastDot > lastSlash && lastDot < path.length() - 1) {
            String ext = path.substring(lastDot + 1).toLowerCase();
            String mime = EXTENSION_TO_MIME.get(ext);
            if (mime != null) {
                return mime;
            }
        }
        return "application/octet-stream";
    }

    /**
     * Validates that a URL has a recognized storage scheme (s3://, oss://, gs://, etc.).
     * Blocks potentially dangerous schemes (e.g., file://) as a defense-in-depth measure.
     *
     * @param url the URL to validate
     * @throws IllegalArgumentException if the URL format is invalid or scheme is not allowed
     */
    public static void validateFileUrl(String url) {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("FILE_FROM_URL: URL must not be null or empty");
        }
        int schemeEnd = url.indexOf("://");
        if (schemeEnd < 0) {
            throw new IllegalArgumentException(
                    "FILE_FROM_URL: Invalid URL format (expected scheme://path): " + url);
        }
        String scheme = url.substring(0, schemeEnd).toLowerCase();
        if (!ALLOWED_SCHEMES.contains(scheme)) {
            throw new IllegalArgumentException(
                    "FILE_FROM_URL: Unsupported URL scheme '" + scheme + "'. "
                            + "Allowed schemes: " + ALLOWED_SCHEMES);
        }
    }

    private static String toString(Object field, String name) {
        if (field == null) {
            return "";
        }
        if (field instanceof String s) {
            return s;
        }
        return field.toString();
    }

    private static long toLong(Object field, String name) {
        if (field == null) {
            return 0L;
        }
        if (field instanceof Long l) {
            return l;
        }
        if (field instanceof Integer i) {
            return i.longValue();
        }
        if (field instanceof Number n) {
            return n.longValue();
        }
        try {
            return Long.parseLong(field.toString());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}
