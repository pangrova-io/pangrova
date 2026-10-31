package org.pangrova.shuffle.spool.localfs;

import org.pangrova.shuffle.spool.SpoolReader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * Reads committed shuffle segments from the local filesystem for a single partition
 * over a half-open map-index range {@code [startMapIndex, endMapIndex)}.
 *
 * <p>When multiple attempts of the same {@code mapId} were committed (speculative execution
 * or task re-run), only the segment with the highest {@code attemptId} is yielded --
 * this is the FTE correctness invariant: re-executed mappers must never be double-read.
 */
final class LocalFsSpoolReader implements SpoolReader {

    private final Iterator<Path> segments;

    /**
     * Scans the partition directory for committed segments matching the map-index range
     * and deduplicates by keeping the highest attemptId per mapId.
     *
     * @param partitionDir  the directory containing {@code m<mapId>-a<attemptId>.arrow} files
     * @param startMapIndex start of the map-index range (inclusive)
     * @param endMapIndex   end of the map-index range (exclusive)
     */
    LocalFsSpoolReader(Path partitionDir, int startMapIndex, int endMapIndex) {
        if (!Files.isDirectory(partitionDir)) {
            this.segments = List.<Path>of().iterator();
            return;
        }

        // Scan and parse committed segment files, keeping highest attemptId per mapId
        TreeMap<Integer, Path> deduped = new TreeMap<>();
        TreeMap<Integer, Long> highestAttempt = new TreeMap<>();
        try (Stream<Path> files = Files.list(partitionDir)) {
            files.filter(p -> p.getFileName().toString().endsWith(".arrow"))
                    .forEach(p -> {
                        String name = p.getFileName().toString();
                        // Expected format: m<mapId>-a<attemptId>.arrow
                        int[] parsed = parseSegmentName(name);
                        if (parsed == null) {
                            return;
                        }
                        int mapId = parsed[0];
                        long attemptId = parsed[1];
                        if (mapId >= startMapIndex && mapId < endMapIndex) {
                            Long existing = highestAttempt.get(mapId);
                            if (existing == null || attemptId > existing) {
                                highestAttempt.put(mapId, attemptId);
                                deduped.put(mapId, p);
                            }
                        }
                    });
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to list spool segments in " + partitionDir, e);
        }

        // Yield in ascending mapId order
        this.segments = deduped.values().iterator();
    }

    @Override
    public boolean hasNext() {
        return segments.hasNext();
    }

    @Override
    public ByteBuffer next() {
        if (!segments.hasNext()) {
            throw new NoSuchElementException("No more spool segments");
        }
        Path path = segments.next();
        try {
            byte[] bytes = Files.readAllBytes(path);
            return ByteBuffer.wrap(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read spool segment " + path, e);
        }
    }

    @Override
    public void close() {
        // No resources held open; all reads are one-shot
    }

    /**
     * Parses a segment filename of the form {@code m<mapId>-a<attemptId>.arrow}.
     *
     * @return {@code [mapId, attemptId]} or {@code null} if the name does not match
     */
    static int[] parseSegmentName(String name) {
        // m<int>-a<long>.arrow
        if (!name.startsWith("m") || !name.endsWith(".arrow")) {
            return null;
        }
        String core = name.substring(1, name.length() - ".arrow".length());
        int dash = core.indexOf("-a");
        if (dash < 0) {
            return null;
        }
        try {
            int mapId = Integer.parseInt(core.substring(0, dash));
            long attemptId = Long.parseLong(core.substring(dash + 2));
            return new int[]{mapId, (int) attemptId};
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
