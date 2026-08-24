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
package org.pangrova.worker.ai.blob;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BlobOrphanCleanerTest {

    private static final String TABLE = "catalog.schema.my_table";
    private static final String BLOB_PREFIX = "s3://bucket/warehouse/my_table/blob/";
    private static final Instant NOW = Instant.parse("2024-06-15T12:00:00Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(NOW, ZoneId.of("UTC"));

    private InMemoryBlobScanner scanner;
    private BlobOrphanCleaner cleaner;

    @BeforeEach
    void setUp() {
        scanner = new InMemoryBlobScanner();
        cleaner = new BlobOrphanCleaner(scanner, FIXED_CLOCK);
    }

    @Test
    void deletesUnreferencedBlobsOlderThanGracePeriod() {
        Instant oldTime = NOW.minus(Duration.ofHours(25));
        scanner.addFile("image", new BlobFileInfo(
                "s3://bucket/warehouse/my_table/blob/image/aaa.jpg", 1024, oldTime));

        BlobCleanupResult result = cleaner.cleanOrphans(
                TABLE, BLOB_PREFIX, List.of("image"), Set.of(), Duration.ofHours(24));

        assertThat(result.filesRemoved()).isEqualTo(1);
        assertThat(result.bytesReclaimed()).isEqualTo(1024L);
        assertThat(scanner.deletedUris()).containsExactly(
                "s3://bucket/warehouse/my_table/blob/image/aaa.jpg");
    }

    @Test
    void skipsReferencedBlobs() {
        Instant oldTime = NOW.minus(Duration.ofHours(25));
        String uri = "s3://bucket/warehouse/my_table/blob/image/bbb.jpg";
        scanner.addFile("image", new BlobFileInfo(uri, 2048, oldTime));

        BlobCleanupResult result = cleaner.cleanOrphans(
                TABLE, BLOB_PREFIX, List.of("image"), Set.of(uri), Duration.ofHours(24));

        assertThat(result.filesRemoved()).isZero();
        assertThat(result.bytesReclaimed()).isZero();
        assertThat(scanner.deletedUris()).isEmpty();
    }

    @Test
    void skipsBlobsWithinGracePeriod() {
        Instant recentTime = NOW.minus(Duration.ofHours(23));
        scanner.addFile("image", new BlobFileInfo(
                "s3://bucket/warehouse/my_table/blob/image/ccc.jpg", 512, recentTime));

        BlobCleanupResult result = cleaner.cleanOrphans(
                TABLE, BLOB_PREFIX, List.of("image"), Set.of(), Duration.ofHours(24));

        assertThat(result.filesRemoved()).isZero();
        assertThat(scanner.deletedUris()).isEmpty();
    }

    @Test
    void handlesMultipleColumns() {
        Instant oldTime = NOW.minus(Duration.ofHours(48));
        scanner.addFile("image", new BlobFileInfo(
                "s3://bucket/warehouse/my_table/blob/image/d1.jpg", 100, oldTime));
        scanner.addFile("pdf", new BlobFileInfo(
                "s3://bucket/warehouse/my_table/blob/pdf/d2.pdf", 200, oldTime));

        BlobCleanupResult result = cleaner.cleanOrphans(
                TABLE, BLOB_PREFIX, List.of("image", "pdf"), Set.of(), Duration.ofHours(24));

        assertThat(result.filesRemoved()).isEqualTo(2);
        assertThat(result.bytesReclaimed()).isEqualTo(300L);
    }

    @Test
    void returnsZeroResultForEmptyBlobColumns() {
        BlobCleanupResult result = cleaner.cleanOrphans(
                TABLE, BLOB_PREFIX, List.of(), Set.of(), Duration.ofHours(24));

        assertThat(result.filesRemoved()).isZero();
        assertThat(result.bytesReclaimed()).isZero();
    }

    @Test
    void continuesOnScanFailure() {
        Instant oldTime = NOW.minus(Duration.ofHours(25));
        scanner.failOnPrefix("s3://bucket/warehouse/my_table/blob/broken/");
        scanner.addFile("good", new BlobFileInfo(
                "s3://bucket/warehouse/my_table/blob/good/e1.jpg", 500, oldTime));

        BlobCleanupResult result = cleaner.cleanOrphans(
                TABLE, BLOB_PREFIX, List.of("broken", "good"), Set.of(), Duration.ofHours(24));

        assertThat(result.filesRemoved()).isEqualTo(1);
        assertThat(result.bytesReclaimed()).isEqualTo(500L);
    }

    @Test
    void continuesOnDeleteFailure() {
        Instant oldTime = NOW.minus(Duration.ofHours(25));
        String failUri = "s3://bucket/warehouse/my_table/blob/image/fail.jpg";
        String okUri = "s3://bucket/warehouse/my_table/blob/image/ok.jpg";
        scanner.addFile("image", new BlobFileInfo(failUri, 100, oldTime));
        scanner.addFile("image", new BlobFileInfo(okUri, 200, oldTime));
        scanner.failOnDelete(failUri);

        BlobCleanupResult result = cleaner.cleanOrphans(
                TABLE, BLOB_PREFIX, List.of("image"), Set.of(), Duration.ofHours(24));

        assertThat(result.filesRemoved()).isEqualTo(1);
        assertThat(result.bytesReclaimed()).isEqualTo(200L);
        assertThat(scanner.deletedUris()).containsExactly(okUri);
    }

    @Test
    void usesDefaultGracePeriodWhenNull() {
        Instant justOver24h = NOW.minus(Duration.ofHours(24).plusSeconds(1));
        scanner.addFile("image", new BlobFileInfo(
                "s3://bucket/warehouse/my_table/blob/image/f1.jpg", 300, justOver24h));

        BlobCleanupResult result = cleaner.cleanOrphans(
                TABLE, BLOB_PREFIX, List.of("image"), Set.of(), null);

        assertThat(result.filesRemoved()).isEqualTo(1);
    }

    @Test
    void asyncReturnsResultViaFuture() {
        Instant oldTime = NOW.minus(Duration.ofHours(25));
        scanner.addFile("image", new BlobFileInfo(
                "s3://bucket/warehouse/my_table/blob/image/g1.jpg", 1000, oldTime));

        BlobCleanupResult result = cleaner.cleanOrphansAsync(
                TABLE, BLOB_PREFIX, List.of("image"), Set.of(), Duration.ofHours(24)).join();

        assertThat(result.filesRemoved()).isEqualTo(1);
        assertThat(result.bytesReclaimed()).isEqualTo(1000L);
    }

    @Test
    void rejectsNullStorageScanner() {
        assertThatThrownBy(() -> new BlobOrphanCleaner(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullTableName() {
        assertThatThrownBy(() -> cleaner.cleanOrphans(
                null, BLOB_PREFIX, List.of("image"), Set.of(), Duration.ofHours(24)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void normalizesPrefixWithoutTrailingSlash() {
        Instant oldTime = NOW.minus(Duration.ofHours(25));
        scanner.addFile("image", new BlobFileInfo(
                "s3://bucket/warehouse/my_table/blob/image/h1.jpg", 100, oldTime));

        BlobCleanupResult result = cleaner.cleanOrphans(
                TABLE, "s3://bucket/warehouse/my_table/blob",
                List.of("image"), Set.of(), Duration.ofHours(24));

        assertThat(result.filesRemoved()).isEqualTo(1);
    }

    // --- In-memory test implementation ---

    private static final class InMemoryBlobScanner implements BlobStorageScanner {

        private final List<FileEntry> files = new ArrayList<>();
        private final List<String> failPrefixes = new ArrayList<>();
        private final List<String> failDeleteUris = new ArrayList<>();
        private final CopyOnWriteArrayList<String> deleted = new CopyOnWriteArrayList<>();

        void addFile(String column, BlobFileInfo info) {
            files.add(new FileEntry(column, info));
        }

        void failOnPrefix(String prefix) {
            failPrefixes.add(prefix);
        }

        void failOnDelete(String uri) {
            failDeleteUris.add(uri);
        }

        List<String> deletedUris() {
            return List.copyOf(deleted);
        }

        @Override
        public List<BlobFileInfo> listBlobFiles(String directoryPrefix) throws IOException {
            for (String failPrefix : failPrefixes) {
                if (directoryPrefix.equals(failPrefix)) {
                    throw new IOException("Simulated scan failure for: " + directoryPrefix);
                }
            }
            List<BlobFileInfo> result = new ArrayList<>();
            for (FileEntry entry : files) {
                if (entry.info.uri().startsWith(directoryPrefix)) {
                    result.add(entry.info);
                }
            }
            return result;
        }

        @Override
        public void delete(String uri) throws IOException {
            if (failDeleteUris.contains(uri)) {
                throw new IOException("Simulated delete failure for: " + uri);
            }
            deleted.add(uri);
        }

        private record FileEntry(String column, BlobFileInfo info) {}
    }
}
