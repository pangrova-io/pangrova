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

import org.pangrova.spi.annotation.Experimental;

/**
 * Immutable reference to an externally stored binary object.
 *
 * <p>Used by {@link EmbeddingProvider#embedBlob(java.util.List)} to locate
 * and validate blob content without loading bytes into JVM heap. Providers
 * stream content directly from the URI.</p>
 *
 * @param uri      storage URI of the blob (e.g., s3://bucket/path; must not be null or blank)
 * @param mimeType MIME type of the content (e.g., image/jpeg; must not be null or blank)
 * @param size     content length in bytes (must be non-negative)
 * @param checksum SHA-256 hex digest for integrity verification (must not be null or blank)
 */
@Experimental
public record BlobReference(String uri, String mimeType, long size, String checksum) {

    public BlobReference {
        if (uri == null || uri.isBlank()) {
            throw new IllegalArgumentException("uri required");
        }
        if (mimeType == null || mimeType.isBlank()) {
            throw new IllegalArgumentException("mimeType required");
        }
        if (size < 0) {
            throw new IllegalArgumentException("size must be non-negative");
        }
        if (checksum == null || checksum.isBlank()) {
            throw new IllegalArgumentException("checksum required");
        }
    }
}
