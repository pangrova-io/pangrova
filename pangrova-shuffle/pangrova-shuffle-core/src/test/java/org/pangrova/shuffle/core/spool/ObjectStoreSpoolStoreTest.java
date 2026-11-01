package org.pangrova.shuffle.core.spool;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.shuffle.api.capability.ShuffleDurability;
import org.pangrova.shuffle.api.spool.SpoolReader;
import org.pangrova.shuffle.api.spool.SpoolWriter;
import org.pangrova.shuffle.api.storage.ShuffleObjectStorage;

import static org.assertj.core.api.Assertions.assertThat;

class ObjectStoreSpoolStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void commitMakesSegmentsReadableInMapOrder() throws Exception {
        try (InMemoryObjects objects = new InMemoryObjects();
             ObjectStoreSpoolStore store = new ObjectStoreSpoolStore(objects, tempDir)) {
            writeSegment(store, "q1", 0, 0, 1, 0, "map-1");
            writeSegment(store, "q1", 0, 0, 0, 0, "map-0");
            writeSegment(store, "q1", 0, 0, 2, 0, "map-2");

            List<String> read = readAll(store, "q1", 0, 0, 0, 3);
            assertThat(read).containsExactly("map-0", "map-1", "map-2");
            assertThat(store.durability()).isEqualTo(ShuffleDurability.DURABLE);
        }
    }

    @Test
    void readerYieldsLatestAttemptPerMapOnly() throws Exception {
        try (InMemoryObjects objects = new InMemoryObjects();
             ObjectStoreSpoolStore store = new ObjectStoreSpoolStore(objects, tempDir)) {
            writeSegment(store, "q1", 0, 0, 0, 1, "attempt-1");
            writeSegment(store, "q1", 0, 0, 0, 2, "attempt-2");
            writeSegment(store, "q1", 0, 0, 0, 3, "attempt-3");

            List<String> read = readAll(store, "q1", 0, 0, 0, 1);
            assertThat(read).containsExactly("attempt-3");
        }
    }

    @Test
    void readerHonorsHalfOpenMapRange() throws Exception {
        try (InMemoryObjects objects = new InMemoryObjects();
             ObjectStoreSpoolStore store = new ObjectStoreSpoolStore(objects, tempDir)) {
            for (int mapId = 0; mapId < 5; mapId++) {
                writeSegment(store, "q1", 0, 0, mapId, 0, "map-" + mapId);
            }
            List<String> read = readAll(store, "q1", 0, 0, 1, 3);
            assertThat(read).containsExactly("map-1", "map-2");
        }
    }

    @Test
    void closeWithoutCommitAbortsSegment() throws Exception {
        try (InMemoryObjects objects = new InMemoryObjects();
             ObjectStoreSpoolStore store = new ObjectStoreSpoolStore(objects, tempDir)) {
            SpoolWriter writer = store.open("q1", 0, 0, 0, 0);
            writer.write(ByteBuffer.wrap("uncommitted".getBytes(StandardCharsets.UTF_8)));
            writer.close();
            try (SpoolReader reader = store.read("q1", 0, 0, 0, 1)) {
                assertThat(reader.hasNext()).isFalse();
            }
        }
    }

    @Test
    void deleteRemovesAllSegmentsForQuery() throws Exception {
        try (InMemoryObjects objects = new InMemoryObjects();
             ObjectStoreSpoolStore store = new ObjectStoreSpoolStore(objects, tempDir)) {
            writeSegment(store, "q1", 0, 0, 0, 0, "a");
            writeSegment(store, "q2", 0, 0, 0, 0, "b");
            store.delete("q1");
            assertThat(objects.listPrefix("q1/")).isEmpty();
            assertThat(objects.listPrefix("q2/")).hasSize(1);
        }
    }

    private static void writeSegment(ObjectStoreSpoolStore store, String queryId, int stageId,
                                     int partitionId, int mapId, long attemptId, String content) {
        SpoolWriter writer = store.open(queryId, stageId, partitionId, mapId, attemptId);
        writer.write(ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8)));
        writer.commit();
    }

    private static List<String> readAll(ObjectStoreSpoolStore store, String queryId, int stageId,
                                        int partitionId, int startMap, int endMap) {
        List<String> result = new ArrayList<>();
        try (SpoolReader reader = store.read(queryId, stageId, partitionId, startMap, endMap)) {
            while (reader.hasNext()) {
                result.add(new String(reader.next().array(), StandardCharsets.UTF_8));
            }
        }
        return result;
    }

    /** Minimal in-memory {@link ShuffleObjectStorage} for exercising the generic spool engine. */
    private static final class InMemoryObjects implements ShuffleObjectStorage {

        private final Map<String, byte[]> data = new TreeMap<>();

        @Override
        public void putImmutable(String key, Supplier<InputStream> data, long sizeBytes,
                                 byte[] sha256) {
            try (InputStream in = data.get()) {
                byte[] bytes = in.readAllBytes();
                if (bytes.length != sizeBytes) {
                    throw new IllegalStateException("size mismatch");
                }
                byte[] existing = this.data.putIfAbsent(key, bytes);
                if (existing != null
                        && !MessageDigest.isEqual(existing, bytes)) {
                    throw new IllegalStateException("conflicting content");
                }
            } catch (java.io.IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void verifyStored(String key, long sizeBytes, byte[] sha256) {
            byte[] bytes = data.get(key);
            if (bytes == null || bytes.length != sizeBytes) {
                throw new IllegalStateException("metadata mismatch");
            }
        }

        @Override
        public byte[] get(String key) {
            return data.get(key);
        }

        @Override
        public List<String> listPrefix(String keyPrefix) {
            return data.keySet().stream().filter(key -> key.startsWith(keyPrefix)).toList();
        }

        @Override
        public void delete(List<String> keys) {
            keys.forEach(data::remove);
        }

        @Override
        public String deletePrefixPage(String prefix, String after, int limit) {
            List<String> keys = listPrefix(prefix).stream()
                    .filter(key -> after == null || key.compareTo(after) > 0)
                    .toList();
            List<String> page = new ArrayList<>(keys.subList(0, Math.min(limit, keys.size())));
            delete(page);
            return keys.size() > limit && !page.isEmpty() ? page.get(page.size() - 1) : null;
        }

        @Override
        public void close() {
            // Nothing to release.
        }
    }
}
