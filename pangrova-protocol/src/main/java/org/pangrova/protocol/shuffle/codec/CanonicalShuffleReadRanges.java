package org.pangrova.protocol.shuffle.codec;

import org.pangrova.protocol.v2.ShufflePartitionReadRange;

/** Validates the complete wire evidence before an absolute batch interval is used. */
public final class CanonicalShuffleReadRanges {
    private CanonicalShuffleReadRanges() {}

    /** Returns whether the range has explicit bounds and valid manifest evidence. */
    public static boolean valid(ShufflePartitionReadRange range) {
        return range.hasStartSequence() && range.getStartSequence() >= 0
                && range.hasEndSequenceExclusive() && range.getEndSequenceExclusive() > range.getStartSequence()
                && range.hasTotalBytes() && range.hasTotalRows()
                && range.getEndSequenceExclusive() - range.getStartSequence()
                    <= Math.min(range.getTotalBytes(), range.getTotalRows())
                && range.getManifestSha256().size() == 32 && range.getUnknownFields().asMap().isEmpty();
    }
}
