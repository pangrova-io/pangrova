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
package org.pangrova.ai.api.embedding;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BlobReferenceTest {

    private static final String CHECKSUM = "a".repeat(64);
    private static final String URI = "pangrova-blob://v2/" + "b".repeat(64)
            + "/" + "c".repeat(64) + "/" + CHECKSUM;

    @Test
    void normalizesSha256ToLowerCase() {
        BlobReference reference = new BlobReference(
                URI, " IMAGE/PNG ", 0, "A".repeat(64));

        assertThat(reference.checksum()).isEqualTo("a".repeat(64));
        assertThat(reference.mimeType()).isEqualTo("image/png");
    }

    @Test
    void rejectsNonSha256Checksums() {
        assertThatThrownBy(() -> new BlobReference(
                URI, "image/png", 1, "pending"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SHA-256");
        assertThatThrownBy(() -> new BlobReference(
                URI, "image/png", 1, "z".repeat(64)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SHA-256");
    }

    @Test
    void rejectsNegativeSizeAndMissingMetadata() {
        assertThatThrownBy(() -> new BlobReference(
                URI, "image/png", -1, CHECKSUM))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("size");
        assertThatThrownBy(() -> new BlobReference(
                " ", "image/png", 1, "a".repeat(64)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("uri");
    }

    @Test
    void rejectsRawStorageUrisAndMismatchedContentIdentity() {
        assertThatThrownBy(() -> new BlobReference(
                "s3://bucket/object", "image/png", 1, CHECKSUM))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pangrova-blob");
        assertThatThrownBy(() -> new BlobReference(
                "pangrova-blob://v2/" + "b".repeat(64) + "/" + "c".repeat(64)
                        + "/" + "d".repeat(64),
                "image/png", 1, CHECKSUM))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("match checksum");
    }
}
