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

import org.junit.jupiter.api.Test;
import org.pangrova.ai.api.embedding.BlobReference;
import org.pangrova.connector.spi.data.value.ColumnValue;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileStructHelperTest {

    private static final String URI = "pangrova-blob://v2/" + "b".repeat(64)
            + "/" + "c".repeat(64) + "/" + "a".repeat(64);

    @Test
    void roundTripsValidatedReference() {
        ColumnValue value = FileStructHelper.createFileStruct(
                URI, "application/pdf", 42, "a".repeat(64),
                LocalDateTime.of(2026, 8, 29, 12, 0));

        BlobReference reference = FileStructHelper.extractBlobReference(value);

        assertThat(reference).isEqualTo(new BlobReference(
                URI, "application/pdf", 42, "a".repeat(64)));
    }

    @Test
    void rejectsPlaceholderChecksumAndFractionalSize() {
        assertThatThrownBy(() -> FileStructHelper.extractBlobReference(new ColumnValue.RowValue(
                List.of(URI, "image/png", 1L, "pending"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SHA-256");
        assertThatThrownBy(() -> FileStructHelper.extractBlobReference(new ColumnValue.RowValue(
                List.of(URI, "image/png", 1.5d, "a".repeat(64)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("BIGINT");
    }

    @Test
    void creationRequiresTimestamp() {
        assertThatThrownBy(() -> FileStructHelper.createFileStruct(
                URI, "image/png", 1, "a".repeat(64), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("createdAt");
    }
}
