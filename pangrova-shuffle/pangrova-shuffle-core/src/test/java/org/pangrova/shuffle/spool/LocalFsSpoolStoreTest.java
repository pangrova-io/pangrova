package org.pangrova.shuffle.spool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import org.pangrova.shuffle.ShuffleDurability;
import org.pangrova.shuffle.spool.SpoolReader;
import org.pangrova.shuffle.spool.SpoolStore;
import org.pangrova.shuffle.spool.SpoolStoreContext;
import org.pangrova.shuffle.spool.SpoolStoreFactory;
import org.pangrova.shuffle.spool.SpoolWriter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link LocalFsSpoolStore} and its writer/reader (ADR-002 D6).
 */
class LocalFsSpoolStoreTest {

    @TempDir
    Path tempDir;

    private LocalFsSpoolStore store;

    @BeforeEach
    void setUp() {
        store = new LocalFsSpoolStore(tempDir);
    }

    @Test
    void roundTrip_writeAndReadBack() {
        byte[] data = "hello-arrow-ipc".getBytes();
        try (SpoolWriter writer = store.open("q1", 0, 0, 0, 0)) {
            writer.write(ByteBuffer.wrap(data));
            writer.commit();
        }

        try (SpoolReader reader = store.read("q1", 0, 0, 0, Integer.MAX_VALUE)) {
            assertTrue(reader.hasNext());
            ByteBuffer buf = reader.next();
            byte[] actual = new byte[buf.remaining()];
            buf.get(actual);
            assertArrayEquals(data, actual);
            assertFalse(reader.hasNext());
        }
    }

    @Test
    void rangeRead_filtersMapIdRange() {
        // Write three mappers
        for (int mapId = 0; mapId < 3; mapId++) {
            byte[] data = ("data-m" + mapId).getBytes();
            try (SpoolWriter writer = store.open("q2", 1, 0, mapId, 0)) {
                writer.write(ByteBuffer.wrap(data));
                writer.commit();
            }
        }

        // Read only [1, 3) — should get mapId 1 and 2
        List<String> results = readAll(store, "q2", 1, 0, 1, 3);
        assertEquals(2, results.size());
        assertEquals("data-m1", results.get(0));
        assertEquals("data-m2", results.get(1));
    }

    @Test
    void attemptDedup_keepsHighestAttemptId() {
        // mapId=0 has two attempts: 0 and 1
        try (SpoolWriter w = store.open("q3", 0, 0, 0, 0)) {
            w.write(ByteBuffer.wrap("attempt-0".getBytes()));
            w.commit();
        }
        try (SpoolWriter w = store.open("q3", 0, 0, 0, 1)) {
            w.write(ByteBuffer.wrap("attempt-1".getBytes()));
            w.commit();
        }

        List<String> results = readAll(store, "q3", 0, 0, 0, Integer.MAX_VALUE);
        assertEquals(1, results.size());
        assertEquals("attempt-1", results.get(0), "Should keep highest attemptId");
    }

    @Test
    void uncommittedSegment_notVisibleAndNoResidue() throws IOException {
        // Open a writer but close without committing
        try (SpoolWriter w = store.open("q4", 0, 0, 0, 0)) {
            w.write(ByteBuffer.wrap("uncommitted".getBytes()));
            // no commit — close triggers abort
        }

        // Reader should see nothing
        try (SpoolReader reader = store.read("q4", 0, 0, 0, Integer.MAX_VALUE)) {
            assertFalse(reader.hasNext());
        }

        // No .tmp residue
        Path partDir = tempDir.resolve("q4").resolve("s0").resolve("p0");
        if (Files.exists(partDir)) {
            try (var files = Files.list(partDir)) {
                assertEquals(0, files.filter(p -> p.toString().endsWith(".tmp")).count(),
                        "No .tmp residue after abort");
            }
        }
    }

    @Test
    void delete_cleansUpQueryData() {
        try (SpoolWriter w = store.open("q5", 0, 0, 0, 0)) {
            w.write(ByteBuffer.wrap("data".getBytes()));
            w.commit();
        }

        Path queryDir = tempDir.resolve("q5");
        assertTrue(Files.exists(queryDir));

        store.delete("q5");
        assertFalse(Files.exists(queryDir));
    }

    @Test
    void durability_isLocalSpill() {
        assertEquals(ShuffleDurability.LOCAL_SPILL, store.durability());
    }

    @Test
    void rejectsPathTraversalInQueryId() {
        // Path traversal attempt via ../
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> store.open("../../../etc/passwd", 0, 0, 0, 0));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> store.read("../secret", 0, 0, 0, Integer.MAX_VALUE));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> store.delete("../../data"));
        // Single and double dot are also rejected
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> store.open(".", 0, 0, 0, 0));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> store.open("..", 0, 0, 0, 0));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> store.delete(".."));
    }

    @Test
    void rejectsPathSeparatorInQueryId() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> store.open("foo/bar", 0, 0, 0, 0));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> store.open("foo\\bar", 0, 0, 0, 0));
    }

    @Test
    void rejectsNullQueryId() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> store.open(null, 0, 0, 0, 0));
    }

    @Test
    void acceptsValidQueryIds() {
        // These should not throw
        byte[] data = "test".getBytes();
        try (SpoolWriter w = store.open("query-123_abc.456", 0, 0, 0, 0)) {
            w.write(ByteBuffer.wrap(data));
            w.commit();
        }
        try (SpoolReader r = store.read("query-123_abc.456", 0, 0, 0, Integer.MAX_VALUE)) {
            assertTrue(r.hasNext());
        }
    }

    @Test
    void serviceLoader_discoversFactoryByName() {
        boolean found = false;
        for (SpoolStoreFactory factory : ServiceLoader.load(SpoolStoreFactory.class)) {
            if ("local-fs".equals(factory.name())) {
                found = true;
                SpoolStore created = factory.create(
                        new SpoolStoreContext("local-fs", java.util.Map.of(
                                "spool.local-fs.base-dir", tempDir.toString())));
                assertNotNull(created);
                assertEquals(ShuffleDurability.LOCAL_SPILL, created.durability());
                break;
            }
        }
        assertTrue(found, "local-fs SpoolStoreFactory should be discoverable via ServiceLoader");
    }

    private List<String> readAll(SpoolStore s, String queryId, int stageId, int partitionId,
                                 int startMapIndex, int endMapIndex) {
        List<String> result = new ArrayList<>();
        try (SpoolReader reader = s.read(queryId, stageId, partitionId, startMapIndex, endMapIndex)) {
            while (reader.hasNext()) {
                ByteBuffer buf = reader.next();
                byte[] bytes = new byte[buf.remaining()];
                buf.get(bytes);
                result.add(new String(bytes));
            }
        }
        return result;
    }
}
