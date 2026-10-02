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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.ValidationException;
import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.expression.FunctionCall;
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;

/**
 * Enforces the per-table FILE-column MIME whitelist on INSERT (F-MM-GOV).
 *
 * <p>When a table declares {@code pangrova.file.allowed_types} (a comma-separated
 * whitelist set at {@code CREATE TABLE ... WITH (...)}), every value written to
 * a FILE column (marked by the {@code pangrova.file.column.<name>=true} property
 * emitted by {@code DdlExecutor}) must carry a MIME type contained in that
 * whitelist. Non-whitelisted content is rejected at the coordinator before any
 * connector write, closing the gap where the property was recorded but never
 * checked.</p>
 *
 * <p>MIME resolution mirrors {@code DmlExpressionEvaluator}'s INSERT-function
 * evaluation: {@code FILE_FROM_BYTES(bytes, mime)} carries an explicit MIME;
 * {@code FILE_FROM_URL(uri)} defers content resolution, so its MIME is derived
 * from the URI file extension. Values whose MIME cannot be determined (e.g.
 * SQL NULL or unrecognised expressions) are skipped rather than rejected, so
 * the whitelist never produces false negatives on absent/opaque values.</p>
 */
public final class FileColumnMimeValidator {

    static final String ALLOWED_TYPES_PROPERTY = "pangrova.file.allowed_types";
    static final String FILE_COLUMN_PROPERTY_PREFIX = "pangrova.file.column.";

    /** Fallback MIME for URLs whose extension is not recognised. */
    static final String UNKNOWN_MIME = "application/octet-stream";

    private static final Map<String, String> EXTENSION_TO_MIME = Map.ofEntries(
            Map.entry("jpg", "image/jpeg"), Map.entry("jpeg", "image/jpeg"),
            Map.entry("png", "image/png"), Map.entry("gif", "image/gif"),
            Map.entry("webp", "image/webp"), Map.entry("bmp", "image/bmp"),
            Map.entry("tif", "image/tiff"), Map.entry("tiff", "image/tiff"),
            Map.entry("svg", "image/svg+xml"),
            Map.entry("pdf", "application/pdf"),
            Map.entry("txt", "text/plain"), Map.entry("csv", "text/csv"),
            Map.entry("json", "application/json"), Map.entry("xml", "application/xml"),
            Map.entry("html", "text/html"), Map.entry("md", "text/markdown"),
            Map.entry("doc", "application/msword"),
            Map.entry("docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
            Map.entry("mp3", "audio/mpeg"), Map.entry("wav", "audio/wav"),
            Map.entry("m4a", "audio/mp4"), Map.entry("flac", "audio/flac"),
            Map.entry("ogg", "audio/ogg"),
            Map.entry("mp4", "video/mp4"), Map.entry("mov", "video/quicktime"),
            Map.entry("webm", "video/webm"), Map.entry("mkv", "video/x-matroska"));

    private FileColumnMimeValidator() {}

    /**
     * Validates that every FILE-column value in the INSERT rows carries a
     * whitelisted MIME type. No-op when the table declares no whitelist or has
     * no FILE columns.
     *
     * @param tableMeta the target table metadata (nullable — treated as no-op)
     * @param columns   the resolved INSERT column names, aligned with {@code valueRows} positions
     * @param valueRows the INSERT VALUES rows (each aligned with {@code columns})
     * @throws ValidationException if a FILE value's MIME is not in the whitelist
     */
    public static void validate(ConnectorTableMetadata tableMeta,
                                List<String> columns,
                                List<List<Expression>> valueRows) {
        if (tableMeta == null || columns == null || valueRows == null) {
            return;
        }
        Map<String, String> properties = tableMeta.properties();
        String allowedRaw = properties.get(ALLOWED_TYPES_PROPERTY);
        if (allowedRaw == null || allowedRaw.isBlank()) {
            return; // no whitelist declared → nothing to enforce
        }
        Set<String> allowed = parseAllowed(allowedRaw);
        if (allowed.isEmpty()) {
            return;
        }
        Set<String> fileColumns = fileColumns(properties);
        if (fileColumns.isEmpty()) {
            return;
        }
        for (List<Expression> row : valueRows) {
            int limit = Math.min(columns.size(), row.size());
            for (int i = 0; i < limit; i++) {
                if (!fileColumns.contains(columns.get(i))) {
                    continue;
                }
                String mime = resolveMime(row.get(i));
                if (mime == null) {
                    continue; // undeterminable (NULL / opaque) — do not reject
                }
                if (!allowed.contains(mime.toLowerCase(Locale.ROOT))) {
                    throw new ValidationException(PangrovaErrorCode.VALIDATION_INVALID_ARGUMENT,
                            "FILE column '" + columns.get(i) + "' rejects mime type '" + mime
                                    + "'; table allows only " + allowed);
                }
            }
        }
    }

    private static Set<String> parseAllowed(String raw) {
        return java.util.Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(s -> s.toLowerCase(Locale.ROOT))
                .collect(Collectors.toUnmodifiableSet());
    }

    private static Set<String> fileColumns(Map<String, String> properties) {
        return properties.entrySet().stream()
                .filter(e -> e.getKey().startsWith(FILE_COLUMN_PROPERTY_PREFIX))
                .filter(e -> "true".equalsIgnoreCase(e.getValue()))
                .map(e -> e.getKey().substring(FILE_COLUMN_PROPERTY_PREFIX.length()))
                .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Resolves the MIME type of a FILE-column INSERT expression, or {@code null}
     * when it cannot be determined (not a FILE constructor / opaque value).
     */
    private static String resolveMime(Expression expr) {
        if (!(expr instanceof FunctionCall func)) {
            return null;
        }
        String name = String.join(".", func.name().parts()).toUpperCase(Locale.ROOT);
        return switch (name) {
            case "FILE_FROM_BYTES" -> func.arguments().size() >= 2
                    ? asString(func.arguments().get(1)) : null;
            case "FILE_FROM_URL" -> func.arguments().isEmpty()
                    ? null : mimeFromUrl(asString(func.arguments().get(0)));
            default -> null;
        };
    }

    private static String asString(Expression expr) {
        ColumnValue cv = DmlExpressionEvaluator.extractLiteralColumnValue(expr);
        return cv instanceof ColumnValue.StringValue sv ? sv.value() : null;
    }

    /**
     * Derives a MIME type from a URL's file extension. Returns {@link #UNKNOWN_MIME}
     * for URLs without a recognised extension (which, under a whitelist, are
     * rejected — the safe default for opaque content).
     */
    static String mimeFromUrl(String url) {
        if (url == null || url.isBlank()) {
            return null;
        }
        String path = url;
        int q = path.indexOf('?');
        if (q >= 0) {
            path = path.substring(0, q);
        }
        int h = path.indexOf('#');
        if (h >= 0) {
            path = path.substring(0, h);
        }
        int slash = path.lastIndexOf('/');
        String name = slash >= 0 ? path.substring(slash + 1) : path;
        int dot = name.lastIndexOf('.');
        if (dot < 0 || dot == name.length() - 1) {
            return UNKNOWN_MIME;
        }
        String ext = name.substring(dot + 1).toLowerCase(Locale.ROOT);
        return EXTENSION_TO_MIME.getOrDefault(ext, UNKNOWN_MIME);
    }
}
