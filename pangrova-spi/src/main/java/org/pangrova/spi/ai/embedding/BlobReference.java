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
package org.pangrova.spi.ai.embedding;

import org.pangrova.common.api.annotation.Experimental;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Immutable reference to a binary object in the governed FILE store.
 *
 * <p>Used by {@link EmbeddingBatchRequest.BlobInput} to locate
 * and validate blob content without exposing a connector path or remote URL.
 * The URI is an opaque Pangrova identity; only the Worker blob resolver may
 * translate it to a physical storage location.</p>
 *
 * @param uri      governed URI in
 *                 {@code pangrova-blob://v2/<tenant-scope>/<capability>/<sha256>} form
 * @param mimeType MIME type of the content (e.g., image/jpeg; must not be null or blank)
 * @param size     content length in bytes (must be non-negative)
 * @param checksum SHA-256 hex digest for integrity verification (must not be null or blank)
 */
@Experimental
public record BlobReference(String uri, String mimeType, long size, String checksum) {

    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");

    public BlobReference {
        requireChecksum(checksum);
        requireGovernedUri(uri, checksum);
        requireText(mimeType, "mimeType");
        requireNonNegative(size);
        mimeType = mimeType.trim().toLowerCase(Locale.ROOT);
        checksum = checksum.toLowerCase(Locale.ROOT);
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " required");
        }
    }

    private static void requireNonNegative(long size) {
        if (size < 0) {
            throw new IllegalArgumentException("size must be non-negative");
        }
    }

    private static void requireChecksum(String checksum) {
        requireText(checksum, "checksum");
        if (!SHA256.matcher(checksum.toLowerCase(Locale.ROOT)).matches()) {
            throw new IllegalArgumentException(
                    "checksum must be a 64-character SHA-256 hex digest");
        }
    }

    private static void requireGovernedUri(String value, String checksum) {
        requireText(value, "uri");
        final URI parsed;
        try {
            parsed = new URI(value);
        } catch (URISyntaxException failure) {
            throw new IllegalArgumentException("uri must be a valid governed FILE URI", failure);
        }
        String[] segments = parsed.getPath() == null
                ? new String[0]
                : parsed.getPath().split("/", -1);
        requireCanonicalAuthority(parsed);
        requireCanonicalPath(segments);
        if (checksum != null && !segments[3].equalsIgnoreCase(checksum)) {
            throw new IllegalArgumentException("uri content identity must match checksum");
        }
    }

    private static void requireCanonicalAuthority(URI parsed) {
        boolean canonical = "pangrova-blob".equals(parsed.getScheme())
                && "v2".equals(parsed.getHost())
                && parsed.getPort() == -1
                && parsed.getUserInfo() == null
                && parsed.getQuery() == null
                && parsed.getFragment() == null;
        if (!canonical) {
            throw invalidGovernedUri();
        }
    }

    private static void requireCanonicalPath(String[] segments) {
        boolean canonical = segments.length == 4
                && segments[0].isEmpty()
                && SHA256.matcher(segments[1]).matches()
                && SHA256.matcher(segments[2]).matches()
                && SHA256.matcher(segments[3]).matches();
        if (!canonical) {
            throw invalidGovernedUri();
        }
    }

    private static IllegalArgumentException invalidGovernedUri() {
        return new IllegalArgumentException(
                "uri must use canonical pangrova-blob://v2/"
                        + "<tenant-scope>/<capability>/<sha256> form");
    }
}
