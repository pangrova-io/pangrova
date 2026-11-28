package org.pangrova.shuffle.server;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

import org.pangrova.common.concurrent.NotThreadSafe;
import org.pangrova.protocol.shuffle.codec.ShufflePartitionManifestChecksum;
import org.pangrova.shuffle.checksum.ShuffleChecksums;
import org.pangrova.shuffle.server.enums.FailureKind;
import org.pangrova.shuffle.server.exception.ShuffleServiceException;

import static org.pangrova.shuffle.server.ShuffleServiceModel.BatchIdentity;
import static org.pangrova.shuffle.server.ShuffleServiceModel.FinalizedPartition;
import static org.pangrova.shuffle.server.ShuffleServiceModel.PartitionPublication;
import static org.pangrova.shuffle.server.ShuffleServiceModel.ReadBatch;
import static org.pangrova.shuffle.server.ShuffleServiceModel.StoredBatch;

/** Fixed-width, seekable index for a finalized partition. Its root lives in the publication receipt. */
final class ShufflePartitionManifest {
    static final int INLINE_BATCHES = 256;
    static final int BATCHES_PER_PAGE = 4_096;
    private static final int BATCHES_PER_READ = 256;
    private static final int DECODED_ENTRY_BYTES = 512;
    static final int FANOUT = 64;
    private static final int DIGEST_BYTES = 32;
    private static final int ENTRY_BYTES = 64;
    private static final int NODE_ID_BYTES = 2 * Long.BYTES;

    private ShufflePartitionManifest() {}

    static String objectKey(String shuffleId, byte[] digest) {
        return nodePrefix(shuffleId) + HexFormat.of().formatHex(digest) + ".bin";
    }

    static String nodePrefix(String shuffleId) {
        return shuffleId + "/manifest/nodes/";
    }

    static int rootSize(long batches) {
        if (batches <= INLINE_BATCHES) {
            return Math.toIntExact(batches) * ENTRY_BYTES;
        }
        long children = ceilingDivide(batches, BATCHES_PER_PAGE);
        while (children > FANOUT) {
            children = ceilingDivide(children, FANOUT);
        }
        return Math.toIntExact(children) * DIGEST_BYTES;
    }

    private static long ceilingDivide(long count, int size) {
        return (count - 1) / size + 1;
    }

    @FunctionalInterface
    interface NodeWriter {
        void put(byte[] digest, byte[] bytes);
    }

    @FunctionalInterface
    interface NodeReader {
        byte[] get(String key, int length);
    }

    /** Keeps one unfinished leaf and one unfinished directory per level. */
    @NotThreadSafe
    static final class Writer {
        private final NodeWriter objects;
        private final List<ByteBuffer> directories = new ArrayList<>();
        private final ShufflePartitionManifestChecksum.Accumulator checksum =
                ShufflePartitionManifestChecksum.accumulator();
        private final long rangeTargetBytes;
        private ShuffleReadRanges.Writer ranges;
        private ByteBuffer page;
        private long batches;
        private long bytes;
        private long rows;

        Writer(NodeWriter objects) {
            this(objects, ShuffleReadRanges.TARGET_BYTES);
        }

        Writer(NodeWriter objects, long rangeTargetBytes) {
            this.objects = objects;
            if (rangeTargetBytes <= 0) throw new IllegalArgumentException("shuffle range target must be positive");
            this.rangeTargetBytes = rangeTargetBytes;
        }

        void add(StoredBatch batch) {
            var identity = batch.identity();
            if (!identity.objectKey().equals(batch.objectKey())) {
                throw new IllegalStateException("committed shuffle batch is not canonical");
            }
            byte[] digest = batch.checksum();
            checksum.add(batches, batch.sizeBytes(), batch.rowCount(), digest);
            addRangeEntry(batch, digest);
            bytes = Math.addExact(bytes, batch.sizeBytes());
            rows = Math.addExact(rows, batch.rowCount());
            batches = Math.incrementExact(batches);
            if (page == null) {
                page = ByteBuffer.allocate(INLINE_BATCHES * ENTRY_BYTES);
            } else if (!page.hasRemaining()) {
                advancePage();
            }
            page.putInt(identity.producerPartitionId()).putInt(identity.producerAttemptId())
                    .putLong(identity.batchSequence()).putLong(batch.sizeBytes()).putLong(batch.rowCount())
                    .put(digest);
        }

        PartitionPublication finish(int partitionId) {
            var partition = new FinalizedPartition(
                    partitionId, bytes, rows, batches - 1, checksum.finish(),
                    ranges == null ? List.of() : ranges.finish(batches));
            return new PartitionPublication(partition, finishRoot());
        }

        private void addRangeEntry(StoredBatch batch, byte[] digest) {
            if (ranges == null && page != null && (bytes >= rangeTargetBytes
                    || (!page.hasRemaining() && page.capacity() == BATCHES_PER_PAGE * ENTRY_BYTES))) {
                // All preceding entries are still in the first leaf. Small outputs never hash a second manifest.
                ranges = new ShuffleReadRanges.Writer(rangeTargetBytes);
                ByteBuffer prefix = page.asReadOnlyBuffer().flip();
                byte[] prefixDigest = new byte[DIGEST_BYTES];
                for (long sequence = 0; prefix.hasRemaining(); sequence++) {
                    prefix.position(prefix.position() + 2 * Integer.BYTES + Long.BYTES);
                    long batchBytes = prefix.getLong();
                    long batchRows = prefix.getLong();
                    prefix.get(prefixDigest);
                    ranges.add(sequence, batchBytes, batchRows, prefixDigest);
                }
            }
            if (ranges != null) ranges.add(batches, batch.sizeBytes(), batch.rowCount(), digest);
        }

        private byte[] finishRoot() {
            if (page == null) {
                return new byte[0];
            }
            if (batches <= INLINE_BATCHES) {
                return Arrays.copyOf(page.array(), page.position());
            }
            append(0, store(page));
            for (int level = 0; level + 1 < directories.size(); level++) {
                append(level + 1, store(directories.get(level)));
            }
            ByteBuffer root = directories.get(directories.size() - 1);
            return Arrays.copyOf(root.array(), root.position());
        }

        private void advancePage() {
            if (page.capacity() == INLINE_BATCHES * ENTRY_BYTES) {
                page.flip();
                page = ByteBuffer.allocate(BATCHES_PER_PAGE * ENTRY_BYTES).put(page);
                return;
            }
            append(0, store(page));
        }

        private void append(int level, byte[] digest) {
            if (level == directories.size()) {
                directories.add(ByteBuffer.allocate(FANOUT * DIGEST_BYTES));
            }
            ByteBuffer directory = directories.get(level);
            if (!directory.hasRemaining()) {
                append(level + 1, store(directory));
            }
            directory.put(digest);
        }

        private byte[] store(ByteBuffer node) {
            // A fresh ID makes each physical PUT independently observable during cleanup.
            UUID nodeId = UUID.randomUUID();
            byte[] encoded = ByteBuffer.allocate(NODE_ID_BYTES + node.position())
                    .putLong(nodeId.getMostSignificantBits()).putLong(nodeId.getLeastSignificantBits())
                    .put(node.array(), 0, node.position()).array();
            byte[] digest = ShuffleChecksums.sha256(encoded);
            objects.put(digest, encoded);
            node.clear();
            return digest;
        }
    }

    /** Seeks directly by output sequence. Sequential reads reuse only their current directory path. */
    @NotThreadSafe
    static final class Reader {
        private final String shuffleId;
        private final int partitionId;
        private final long batches;
        private byte[] root;
        private final byte[][] cachedDigests;
        private final byte[][] cachedNodes;

        Reader(String shuffleId, int partitionId, long committedLastSequence, byte[] root) {
            this.shuffleId = shuffleId;
            this.partitionId = partitionId;
            this.batches = Math.addExact(committedLastSequence, 1);
            this.root = root.clone();
            if (batches < 0 || root.length != rootSize(batches)) {
                throw corrupt("root length does not match the publication receipt");
            }
            int levels = 0;
            if (batches > INLINE_BATCHES) {
                levels++;
                for (long pages = ceilingDivide(batches, BATCHES_PER_PAGE);
                        pages > FANOUT; pages = ceilingDivide(pages, FANOUT)) {
                    levels++;
                }
            }
            this.cachedDigests = new byte[levels][];
            this.cachedNodes = new byte[levels][];
        }

        List<ReadBatch> page(long start, NodeReader objects, Runnable requireAuthority) {
            return page(start, batches, objects, requireAuthority);
        }

        List<ReadBatch> page(long start, long endExclusive, NodeReader objects, Runnable requireAuthority) {
            if (start < 0 || start > endExclusive || endExclusive > batches) {
                throw new IllegalArgumentException("shuffle manifest sequence is outside the partition");
            }
            if (start == endExclusive) {
                return List.of();
            }
            byte[] encoded = findLeaf(start, objects, requireAuthority);
            int payloadOffset = batches > INLINE_BATCHES ? NODE_ID_BYTES : 0;
            ByteBuffer leaf = ByteBuffer.wrap(encoded);
            int firstEntry = (int) (start % BATCHES_PER_PAGE);
            leaf.position(payloadOffset + firstEntry * ENTRY_BYTES);
            int entries = (int) Math.min(endExclusive - start,
                    Math.min(BATCHES_PER_READ, leaf.remaining() / ENTRY_BYTES));
            List<ReadBatch> result = new ArrayList<>(entries);
            for (int index = 0; index < entries; index++) {
                result.add(readEntry(leaf, start + result.size()));
            }
            return result;
        }

        int memoryReservation(long start, long endExclusive) {
            if (start == endExclusive) {
                return root.length;
            }
            int reservation = root.length
                    + 2 * Math.toIntExact(Math.min(BATCHES_PER_READ, endExclusive - start)) * DECODED_ENTRY_BYTES;
            if (batches > INLINE_BATCHES) {
                reservation += 2 * (NODE_ID_BYTES + ENTRY_BYTES * Math.toIntExact(Math.min(BATCHES_PER_PAGE, batches)));
                reservation += 2 * (cachedNodes.length - 1) * (NODE_ID_BYTES + FANOUT * DIGEST_BYTES);
            }
            return reservation;
        }

        void discard() {
            root = null;
            Arrays.fill(cachedDigests, null);
            Arrays.fill(cachedNodes, null);
        }

        private byte[] findLeaf(long sequence, NodeReader objects, Runnable requireAuthority) {
            if (batches <= INLINE_BATCHES) {
                return root;
            }
            long available = ceilingDivide(batches, BATCHES_PER_PAGE);
            long childSpan = 1;
            while ((available - 1) / childSpan >= FANOUT) {
                childSpan *= FANOUT;
            }
            long ordinal = sequence / BATCHES_PER_PAGE;
            byte[] directory = root;
            int depth = 0;
            while (childSpan > 1) {
                int child = Math.toIntExact(ordinal / childSpan);
                available = Math.min(childSpan, available - child * childSpan);
                ordinal %= childSpan;
                childSpan /= FANOUT;
                int length = Math.toIntExact((available - 1) / childSpan + 1) * DIGEST_BYTES;
                directory = load(directory, child, length, depth++, objects, requireAuthority);
            }
            long pageStart = sequence - sequence % BATCHES_PER_PAGE;
            int length = Math.toIntExact(Math.min(BATCHES_PER_PAGE, batches - pageStart)) * ENTRY_BYTES;
            return load(directory, Math.toIntExact(ordinal), length, depth, objects, requireAuthority);
        }

        private byte[] load(byte[] directory, int child, int length, int depth,
                NodeReader objects, Runnable requireAuthority) {
            int index = (depth == 0 ? 0 : NODE_ID_BYTES) + child * DIGEST_BYTES;
            int encodedLength = Math.addExact(NODE_ID_BYTES, length);
            byte[] digest = Arrays.copyOfRange(directory, index, index + DIGEST_BYTES);
            byte[] cached = cachedNodes[depth];
            if (cached == null || cached.length != encodedLength
                    || !MessageDigest.isEqual(digest, cachedDigests[depth])) {
                requireAuthority.run();
                byte[] data = objects.get(objectKey(shuffleId, digest), encodedLength);
                if (data.length != encodedLength || !MessageDigest.isEqual(ShuffleChecksums.sha256(data), digest)) {
                    throw corrupt("object page does not match its published digest");
                }
                requireAuthority.run();
                cachedDigests[depth] = digest;
                cachedNodes[depth] = data;
            }
            return cachedNodes[depth];
        }

        private ReadBatch readEntry(ByteBuffer leaf, long sequence) {
            int producer = leaf.getInt();
            int attempt = leaf.getInt();
            long batchSequence = leaf.getLong();
            long bytes = leaf.getLong();
            long rows = leaf.getLong();
            byte[] checksum = new byte[DIGEST_BYTES];
            leaf.get(checksum);
            if (producer < 0 || attempt < 0 || batchSequence < 0 || bytes <= 0 || rows <= 0) {
                throw corrupt("object page contains an invalid batch");
            }
            String key = new BatchIdentity(shuffleId, producer, attempt, partitionId, batchSequence).objectKey();
            return new ReadBatch(sequence, key, bytes, rows, checksum);
        }
    }

    private static ShuffleServiceException corrupt(String detail) {
        return new ShuffleServiceException(FailureKind.CORRUPT, "shuffle manifest " + detail);
    }
}
