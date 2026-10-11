package org.pangrova.resource.spill;

import org.pangrova.resource.MemoryPressure;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DiskSpillableBufferTest {
    @TempDir
    Path directory;

    @Test
    void actualDiskPayloadAndMemoryAccountingFollowOriginalOrder() throws Exception {
        byte[] first = {1, 2, 3};
        byte[] second = {4, 5};
        byte[] third = {6};
        try (DiskSpillableBuffer buffer = new DiskSpillableBuffer("buffer", 1, directory)) {
            buffer.addBatch(first);
            buffer.addBatch(second);
            buffer.addBatch(third);
            assertThat(buffer.spillToDisk(4)).isEqualTo(5);
            assertThat(buffer.reclaimableBytes()).isEqualTo(1);
            assertThat(buffer.batchCount()).isEqualTo(3);
            assertThat(payloads()).containsExactlyInAnyOrder(first, second);
            buffer.readBack();
            assertThat(files()).isEmpty();
            assertThat(buffer.reclaimableBytes()).isEqualTo(6);
            for (byte[] expected : List.of(first, second, third)) {
                assertThat(buffer.spillToDisk(1)).isEqualTo(expected.length);
                assertThat(Files.readAllBytes(files().get(0))).isEqualTo(expected);
                Files.delete(files().get(0));
            }
            assertThat(buffer.reclaimableBytes()).isZero();
        }
        assertThat(files()).isEmpty();
    }

    @Test
    void lateReadFailureKeepsEveryOriginalSpillFileAndSupportsExactRetry() throws Exception {
        try (DiskSpillableBuffer buffer = new DiskSpillableBuffer("buffer", 1, directory)) {
            buffer.addBatch(new byte[] {1, 2});
            buffer.spillToDisk(1);
            Path first = files().get(0);
            buffer.addBatch(new byte[] {3, 4, 5});
            buffer.spillToDisk(1);
            Path second = files().stream().filter(path -> !path.equals(first)).findFirst().orElseThrow();
            byte[] originalSecond = Files.readAllBytes(second);
            Files.delete(second);
            Files.createDirectory(second);
            assertThatThrownBy(buffer::readBack).isInstanceOf(UncheckedIOException.class);
            assertThat(Files.readAllBytes(first)).isEqualTo(new byte[] {1, 2});
            assertThat(buffer.reclaimableBytes()).isZero();
            assertThat(buffer.batchCount()).isEqualTo(2);
            Files.delete(second);
            Files.write(second, originalSecond);
            buffer.readBack();
            assertThat(buffer.reclaimableBytes()).isEqualTo(5);
            assertThat(files()).isEmpty();
            assertThat(buffer.spillToDisk(1)).isEqualTo(2);
            assertThat(Files.readAllBytes(files().get(0))).isEqualTo(new byte[] {1, 2});
        }
    }

    @Test
    void failedDiskWriteKeepsActualBytesAndCandidateUntilSuccessfulRetry() throws Exception {
        Path spill = directory.resolve("spill");
        Files.write(spill, new byte[] {9});
        SpillManager manager = new SpillManager();
        try (DiskSpillableBuffer buffer = new DiskSpillableBuffer("buffer", 1, spill)) {
            buffer.addBatch(new byte[] {1, 2, 3});
            SpillManager.SpillCandidate candidate = buffer.toSpillCandidate();
            manager.registerCandidate(candidate);
            assertThatThrownBy(() -> manager.handleMemoryPressure(MemoryPressure.HIGH, 1))
                    .isInstanceOf(UncheckedIOException.class);
            assertThat(buffer.reclaimableBytes()).isEqualTo(3);
            assertThat(manager.selectCandidates(10, MemoryPressure.HIGH)).containsExactly(candidate);
            Files.delete(spill);
            assertThat(manager.handleMemoryPressure(MemoryPressure.HIGH, 1)).isEqualTo(3);
            assertThat(buffer.reclaimableBytes()).isZero();
            try (var paths = Files.list(spill)) {
                assertThat(Files.readAllBytes(paths.findFirst().orElseThrow())).isEqualTo(new byte[] {1, 2, 3});
            }
            assertThat(manager.selectCandidates(10, MemoryPressure.HIGH)).isEmpty();
        }
    }

    @Test
    void closeReportsCleanupFailureAndRetriesTheStillOwnedPath() throws Exception {
        DiskSpillableBuffer buffer = new DiskSpillableBuffer("buffer", 1, directory);
        buffer.addBatch(new byte[] {1, 2, 3});
        buffer.spillToDisk(1);
        Path spill = files().get(0);
        Files.delete(spill);
        Files.createDirectory(spill);
        Path blocker = Files.write(spill.resolve("blocker"), new byte[] {9});
        assertThatThrownBy(buffer::close).isInstanceOf(UncheckedIOException.class);
        assertThat(Files.exists(spill)).isTrue();
        assertThat(buffer.reclaimableBytes()).isZero();
        assertThatThrownBy(() -> buffer.addBatch(new byte[] {4})).isInstanceOf(IllegalStateException.class);
        Files.delete(blocker);
        buffer.close();
        assertThat(files()).isEmpty();
        buffer.close();
    }

    @Test
    void partialPressureUsesActualFreedBytesAndRetainsRemainingOwner() throws Exception {
        SpillManager manager = new SpillManager(directory.toString());
        try (DiskSpillableBuffer buffer = new DiskSpillableBuffer("buffer", 1, directory)) {
            buffer.addBatch(new byte[] {1, 2, 3});
            buffer.addBatch(new byte[] {4, 5});
            SpillManager.SpillCandidate candidate = buffer.toSpillCandidate();
            manager.registerCandidate(candidate);
            assertThat(manager.handleMemoryPressure(MemoryPressure.HIGH, 1)).isEqualTo(3);
            assertThat(buffer.reclaimableBytes()).isEqualTo(2);
            assertThat(candidate.reclaimableBytes()).isEqualTo(2);
            assertThat(manager.selectCandidates(10, MemoryPressure.HIGH)).containsExactly(candidate);
            assertThat(manager.handleMemoryPressure(MemoryPressure.HIGH)).isEqualTo(2);
            assertThat(manager.selectCandidates(10, MemoryPressure.HIGH)).isEmpty();
            assertThat(payloads()).containsExactlyInAnyOrder(new byte[] {1, 2, 3}, new byte[] {4, 5});
        }
    }

    @Test
    void zeroAndNegativeTargetsNeverCreateSpillFiles() throws Exception {
        SpillManager manager = new SpillManager(directory.toString());
        try (DiskSpillableBuffer buffer = new DiskSpillableBuffer("buffer", 1, directory)) {
            buffer.addBatch(new byte[] {1, 2, 3});
            manager.registerCandidate(buffer.toSpillCandidate());
            assertThat(buffer.spillToDisk(0)).isZero();
            assertThat(buffer.spillToDisk(-1)).isZero();
            assertThat(manager.handleMemoryPressure(MemoryPressure.HIGH, 0)).isZero();
            assertThat(manager.handleMemoryPressure(MemoryPressure.HIGH, -1)).isZero();
            assertThat(files()).isEmpty();
            assertThat(buffer.reclaimableBytes()).isEqualTo(3);
        }
    }

    @Test
    void distinctOwnersWithSameIdHaveDistinctFilesAndExactPayloads() throws Exception {
        try (DiskSpillableBuffer first = new DiskSpillableBuffer("../../same", 1, directory);
             DiskSpillableBuffer second = new DiskSpillableBuffer("../../same", 1, directory)) {
            first.addBatch(new byte[] {1, 2});
            second.addBatch(new byte[] {3, 4, 5});
            first.spillToDisk(1);
            second.spillToDisk(1);
            assertThat(files()).hasSize(2).allMatch(path -> path.getParent().equals(directory));
            assertThat(payloads()).containsExactlyInAnyOrder(new byte[] {1, 2}, new byte[] {3, 4, 5});
            first.readBack();
            assertThat(files()).hasSize(1);
            assertThat(Files.readAllBytes(files().get(0))).isEqualTo(new byte[] {3, 4, 5});
        }
        assertThat(files()).isEmpty();
    }

    @Test
    void configuredNonDirectoryFailsClosedBeforeAnyReclaim() throws Exception {
        Path invalidDirectory = Files.write(directory.resolve("not-directory"), new byte[] {9});
        SpillManager manager = new SpillManager(invalidDirectory.toString());
        try (DiskSpillableBuffer buffer = new DiskSpillableBuffer("buffer", 1, directory)) {
            buffer.addBatch(new byte[] {1, 2, 3});
            SpillManager.SpillCandidate candidate = buffer.toSpillCandidate();
            manager.registerCandidate(candidate);
            assertThatThrownBy(() -> manager.handleMemoryPressure(MemoryPressure.HIGH))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("not a directory");
            assertThat(buffer.reclaimableBytes()).isEqualTo(3);
            assertThat(manager.selectCandidates(10, MemoryPressure.HIGH)).containsExactly(candidate);
        }
    }

    private List<Path> files() throws Exception {
        try (var paths = Files.list(directory)) {
            return paths.toList();
        }
    }

    private List<byte[]> payloads() throws Exception {
        java.util.ArrayList<byte[]> result = new java.util.ArrayList<>();
        for (Path file : files()) {
            result.add(Files.readAllBytes(file));
        }
        return result;
    }
}
