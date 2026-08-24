package org.pangrova.streaming.maintenance.policy;

import java.time.Duration;

/**
 * Policy defining when and how compaction should be triggered for a table.
 *
 * @param minFilesToCompact minimum number of small files before compaction triggers
 * @param maxEqualityDeleteFiles threshold of equality delete files triggering urgent compaction
 * @param compactionInterval cooldown between consecutive compaction runs
 * @param targetFileSizeBytes desired output file size after compaction
 */
public record CompactionPolicy(
    int minFilesToCompact,
    int maxEqualityDeleteFiles,
    Duration compactionInterval,
    long targetFileSizeBytes
) {
    public static CompactionPolicy defaults() {
        return new CompactionPolicy(
            5,
            10,
            Duration.ofMinutes(60),
            128 * 1024 * 1024L
        );
    }
}
