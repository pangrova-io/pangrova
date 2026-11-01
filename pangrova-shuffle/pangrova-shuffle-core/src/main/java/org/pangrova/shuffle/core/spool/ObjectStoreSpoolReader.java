package org.pangrova.shuffle.core.spool;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.pangrova.shuffle.api.spool.SpoolReader;
import org.pangrova.shuffle.api.storage.ShuffleObjectStorage;

/**
 * object-storage spool segment reader: enumerates the committed segments of one partition, dedupes
 * speculative/re-run attempts (latest attempt per mapId wins), and yields each segment in
 * ascending mapId order over the requested half-open range.
 */
final class ObjectStoreSpoolReader implements SpoolReader {

    private static final Pattern SEGMENT_NAME =
            Pattern.compile("^m(\\d+)-a(\\d+)\\.arrow$");

    private final ShuffleObjectStorage objects;
    private final Queue<String> segmentKeys;

    ObjectStoreSpoolReader(ShuffleObjectStorage objects, String prefix,
                  int startMapIndex, int endMapIndex) {
        this.objects = objects;
        Map<Integer, Map.Entry<Long, String>> latestByMap = new TreeMap<>();
        for (String key : objects.listPrefix(prefix)) {
            String fileName = key.substring(key.lastIndexOf('/') + 1);
            Matcher matcher = SEGMENT_NAME.matcher(fileName);
            if (!matcher.matches()) {
                continue;
            }
            int mapId = Integer.parseInt(matcher.group(1));
            long attemptId = Long.parseLong(matcher.group(2));
            if (mapId < startMapIndex || mapId >= endMapIndex) {
                continue;
            }
            latestByMap.merge(mapId, Map.entry(attemptId, key),
                    (existing, candidate) -> candidate.getKey() >= existing.getKey()
                            ? candidate : existing);
        }
        this.segmentKeys = new ArrayDeque<>();
        latestByMap.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator.naturalOrder()))
                .map(entry -> entry.getValue().getValue())
                .forEach(segmentKeys::add);
    }

    @Override
    public boolean hasNext() {
        return !segmentKeys.isEmpty();
    }

    @Override
    public ByteBuffer next() {
        if (segmentKeys.isEmpty()) {
            throw new NoSuchElementException("no further spool segment available");
        }
        return ByteBuffer.wrap(objects.get(segmentKeys.remove()));
    }

    @Override
    public void close() {
        // No long-lived resources to release.
    }
}
