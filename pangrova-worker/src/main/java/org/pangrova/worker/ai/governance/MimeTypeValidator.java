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
package org.pangrova.worker.ai.governance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Validates MIME types against table-level whitelists for FILE columns.
 *
 * <p>Tables with the property {@code pangrova.file.allowed_types} restrict which
 * MIME types can be stored in FILE columns. The property value is a comma-separated
 * list of MIME types or patterns (e.g., "image/*,application/pdf").</p>
 *
 * <p>Supports:</p>
 * <ul>
 *   <li>Exact match: {@code image/jpeg}</li>
 *   <li>Wildcard subtype: {@code image/*} (any image type)</li>
 *   <li>Wildcard all: {@code *\/*} (equivalent to no restriction)</li>
 * </ul>
 *
 * <p>Thread-safe: uses ConcurrentHashMap for parsed whitelist caching.</p>
 */
public final class MimeTypeValidator {

    private static final Logger LOG = LoggerFactory.getLogger(MimeTypeValidator.class);

    /** Table property key for MIME type whitelist. */
    public static final String ALLOWED_TYPES_PROPERTY = "pangrova.file.allowed_types";

    /** Singleton wildcard that allows all types. */
    private static final Set<String> ALLOW_ALL = Set.of("*/*");

    /** Cache of parsed whitelists keyed by table qualified name. */
    private final Map<String, Set<String>> whitelistCache = new ConcurrentHashMap<>();

    /**
     * Validates that a MIME type is allowed for a given table's FILE column.
     *
     * @param mimeType        the MIME type to validate (e.g., "image/jpeg")
     * @param tableProperties the table's properties containing allowed_types
     * @param tableName       qualified table name for error messages
     * @throws MimeTypeNotAllowedException if the MIME type is not whitelisted
     */
    public void validate(String mimeType, Map<String, String> tableProperties, String tableName) {
        if (mimeType == null || mimeType.isBlank()) {
            return; // NULL MIME type passes (metadata not yet available)
        }
        if (tableProperties == null) {
            return; // No table properties means no restriction
        }
        String allowedTypesStr = tableProperties.get(ALLOWED_TYPES_PROPERTY);
        if (allowedTypesStr == null || allowedTypesStr.isBlank()) {
            return; // No restriction configured
        }

        Set<String> allowedTypes = parseWhitelist(tableName, allowedTypesStr);
        if (allowedTypes.equals(ALLOW_ALL)) {
            return;
        }

        if (!isAllowed(mimeType, allowedTypes)) {
            throw new MimeTypeNotAllowedException(
                    "MIME type '%s' is not allowed for table '%s'. Allowed types: %s"
                            .formatted(mimeType, tableName, allowedTypesStr));
        }
    }

    /**
     * Checks if a MIME type is allowed without throwing an exception.
     *
     * @param mimeType        the MIME type to check
     * @param tableProperties the table's properties
     * @return true if the MIME type is allowed or no restriction is configured
     */
    public boolean isAllowed(String mimeType, Map<String, String> tableProperties) {
        if (mimeType == null || tableProperties == null) {
            return true;
        }
        String allowedTypesStr = tableProperties.get(ALLOWED_TYPES_PROPERTY);
        if (allowedTypesStr == null || allowedTypesStr.isBlank()) {
            return true;
        }
        Set<String> allowedTypes = parseWhitelist("", allowedTypesStr);
        return isAllowed(mimeType, allowedTypes);
    }

    /**
     * Invalidates the cached whitelist for a specific table (e.g., after ALTER TABLE).
     *
     * @param tableName the qualified table name
     */
    public void invalidateCache(String tableName) {
        whitelistCache.remove(tableName);
    }

    /** Clears all cached whitelists. */
    public void clearCache() {
        whitelistCache.clear();
    }

    private Set<String> parseWhitelist(String tableName, String allowedTypesStr) {
        return whitelistCache.computeIfAbsent(tableName + ":" + allowedTypesStr, k ->
                Stream.of(allowedTypesStr.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .map(String::toLowerCase)
                        .collect(Collectors.toUnmodifiableSet()));
    }

    private boolean isAllowed(String mimeType, Set<String> allowedTypes) {
        String normalizedMime = mimeType.toLowerCase().trim();

        // Direct match
        if (allowedTypes.contains(normalizedMime)) {
            return true;
        }

        // Wildcard match (e.g., "image/*" matches "image/jpeg")
        String primaryType = normalizedMime.split("/")[0];
        String wildcardPattern = primaryType + "/*";
        if (allowedTypes.contains(wildcardPattern)) {
            return true;
        }

        // Universal wildcard
        return allowedTypes.contains("*/*");
    }

    /**
     * Returns the configured allowed MIME types for a table, or empty set if unrestricted.
     *
     * @param tableProperties the table properties
     * @return immutable set of allowed MIME type patterns
     */
    public Set<String> getAllowedTypes(Map<String, String> tableProperties) {
        if (tableProperties == null) {
            return Collections.emptySet();
        }
        String allowedTypesStr = tableProperties.get(ALLOWED_TYPES_PROPERTY);
        if (allowedTypesStr == null || allowedTypesStr.isBlank()) {
            return Collections.emptySet();
        }
        return parseWhitelist("query", allowedTypesStr);
    }

    /**
     * Exception thrown when a MIME type violates the table's whitelist policy.
     */
    public static final class MimeTypeNotAllowedException extends RuntimeException {
        public MimeTypeNotAllowedException(String message) {
            super(message);
        }
    }
}
