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
package org.pangrova.streaming.cdc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Intercepts CDC records that contain FILE column data, automatically decoding
 * base64-encoded content, writing it to blob storage, and replacing the payload
 * with a FILE struct (Claim-Check pattern).
 *
 * <p><b>Problem:</b> CDC messages from upstream systems may contain inline binary
 * file content (images, documents, audio) as base64-encoded strings. These need
 * to be:</p>
 * <ol>
 *   <li>Decoded from base64</li>
 *   <li>Written to the governed content-addressed FILE store</li>
 *   <li>Replaced in the CDC record with a FILE struct reference</li>
 * </ol>
 *
 * <p><b>CDC field convention:</b> A FILE column in CDC messages is represented as
 * a JSON object with fields:</p>
 * <pre>
 * {
 *   "content_base64": "iVBORw0KGgoAAAA...",
 *   "mime_type": "image/png",
 *   "file_name": "photo.png"  // optional
 * }
 * </pre>
 *
 * <p>After interception, the CDC record is transformed to contain the FILE struct:</p>
 * <pre>
 * {
 *   "uri": "s3://warehouse/blob/photo/uuid.png",
 *   "mime_type": "image/png",
 *   "size": 12345,
 *   "checksum": "sha256hex...",
 *   "created_at": "2024-01-15T10:00:00"
 * }
 * </pre>
 *
 * <p>Thread safety: instances are safe for concurrent use.</p>
 */
public final class StreamFileInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(StreamFileInterceptor.class);

    /** CDC JSON field containing base64-encoded content. */
    static final String FIELD_CONTENT_BASE64 = "content_base64";

    /** CDC JSON field containing MIME type. */
    static final String FIELD_MIME_TYPE = "mime_type";

    /** CDC JSON field containing optional original file name. */
    static final String FIELD_FILE_NAME = "file_name";

    private final Map<String, FileColumnConfig> enabledTables;
    private final BiFunction<FileWriteRequest, String, Map<String, String>> blobWriter;

    /**
     * Creates the interceptor with the given table configs and blob writer.
     *
     * @param enabledTables map of fully-qualified table name to FILE column config
     * @param blobWriter    function that writes blob content and returns FILE struct fields;
     *                      accepts (writeRequest, tableLocation) → struct field map
     */
    public StreamFileInterceptor(
            Map<String, FileColumnConfig> enabledTables,
            BiFunction<FileWriteRequest, String, Map<String, String>> blobWriter) {
        this.enabledTables = Objects.requireNonNull(enabledTables);
        this.blobWriter = Objects.requireNonNull(blobWriter);
    }

    /**
     * Processes a CDC record, detecting and transforming FILE column values.
     *
     * <p>If the record's table has FILE columns configured, any column value
     * containing {@code content_base64} is decoded, written to blob storage,
     * and replaced with a FILE struct reference.</p>
     *
     * @param tableName fully qualified table name
     * @param record    mutable map of column name → value (modified in place)
     * @return true if any FILE columns were intercepted and transformed
     */
    public boolean intercept(String tableName, Map<String, Object> record) {
        if (tableName == null || record == null) {
            return false;
        }
        FileColumnConfig config = enabledTables.get(tableName);
        if (config == null) {
            return false;
        }

        boolean intercepted = false;
        for (String fileColumn : config.fileColumns()) {
            Object value = record.get(fileColumn);
            if (value instanceof Map<?, ?> rawMap
                    && interceptFileColumn(tableName, fileColumn, rawMap, config, record)) {
                intercepted = true;
            }
        }
        return intercepted;
    }

    @SuppressWarnings("unchecked")
    private boolean interceptFileColumn(String tableName, String fileColumn,
                                        Map<?, ?> rawMap, FileColumnConfig config,
                                        Map<String, Object> record) {
        Map<String, Object> filePayload = (Map<String, Object>) rawMap;
        String base64Content = asString(filePayload.get(FIELD_CONTENT_BASE64));
        if (base64Content == null || base64Content.isBlank()) {
            return false;
        }
        try {
            byte[] decoded = Base64.getDecoder().decode(base64Content);
            String mimeType = defaultMimeType(asString(filePayload.get(FIELD_MIME_TYPE)));
            FileWriteRequest writeReq = new FileWriteRequest(fileColumn, decoded, mimeType);
            Map<String, String> structFields = blobWriter.apply(writeReq, config.tableLocation());
            record.put(fileColumn, new LinkedHashMap<>(structFields));
            LOG.debug("event=cdc_file_intercepted table={} column={} size={} mime={}",
                    tableName, fileColumn, decoded.length, mimeType);
            return true;
        } catch (IllegalArgumentException e) {
            LOG.warn("event=cdc_file_decode_failed table={} column={} error={}",
                    tableName, fileColumn, e.getMessage());
            return false;
        }
    }

    private static String defaultMimeType(String mimeType) {
        return mimeType == null || mimeType.isBlank()
                ? "application/octet-stream"
                : mimeType;
    }

    /**
     * Returns the set of enabled table names.
     */
    public Set<String> enabledTableNames() {
        return Collections.unmodifiableSet(enabledTables.keySet());
    }

    private static String asString(Object obj) {
        return obj instanceof String s ? s : null;
    }

    /**
     * Configuration for a table's FILE columns in streaming context.
     *
     * @param fileColumns    set of column names with FILE type
     * @param tableLocation  base storage location for blob writes
     */
    public record FileColumnConfig(
            Set<String> fileColumns,
            String tableLocation) {
        public FileColumnConfig {
            Objects.requireNonNull(fileColumns);
            Objects.requireNonNull(tableLocation);
            fileColumns = Set.copyOf(fileColumns);
        }
    }

    /**
     * Request to write file content to blob storage.
     *
     * @param columnName column name for path organization
     * @param content    decoded binary content
     * @param mimeType   MIME type of the content
     */
    public record FileWriteRequest(
            String columnName,
            byte[] content,
            String mimeType) {
        public FileWriteRequest {
            Objects.requireNonNull(columnName);
            Objects.requireNonNull(content);
            Objects.requireNonNull(mimeType);
        }
    }
}
