package org.pangrova.execution.split;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.SchemaEvolution;

/**
 * Deduplicates bulky table-level {@link SchemaEvolution} messages while keeping
 * every split identity inline.
 *
 * <p>Wide-table splits repeat the same column mapping set once per split, which
 * dominates request bytes at high file counts. The request carries each distinct
 * descriptor once and splits reference it by index. A per-file inline evolution
 * (for example Paimon LSM files with different schemas) is never rewritten, and
 * splits without an evolution stay identity-mapped.</p>
 */
public final class SchemaDescriptorDeduplicator {

    private SchemaDescriptorDeduplicator() {}

    /** Deduplicated splits plus the descriptor table they reference. */
    public record Result(List<DataSplit> splits, List<SchemaEvolution> descriptors) {
        public Result {
            splits = List.copyOf(splits);
            descriptors = List.copyOf(descriptors);
        }
    }

    /** Rewrites splits so each distinct table-level schema appears once. */
    public static Result deduplicate(List<DataSplit> splits) {
        if (splits == null || splits.isEmpty()) {
            return new Result(List.of(), List.of());
        }
        Map<SchemaEvolution, Integer> indexes = new LinkedHashMap<>();
        List<SchemaEvolution> descriptors = new ArrayList<>();
        List<DataSplit> rewritten = new ArrayList<>(splits.size());
        for (DataSplit split : splits) {
            FileSplitDetail detail = DataSplitProtoCodec.fileDetail(split);
            SchemaEvolution evolution = deduplicableEvolution(detail);
            if (evolution == null) {
                rewritten.add(split);
                continue;
            }
            Integer index = indexes.get(evolution);
            if (index == null) {
                index = descriptors.size();
                descriptors.add(evolution);
                indexes.put(evolution, index);
            }
            FileSplitDetail referenced = detail.toBuilder()
                    .clearSchemaEvolution()
                    .setSchemaDescriptorRef(index)
                    .build();
            rewritten.add(DataSplitProtoCodec.withFileDetail(split, referenced));
        }
        return new Result(rewritten, descriptors);
    }

    private static SchemaEvolution deduplicableEvolution(FileSplitDetail detail) {
        if (!detail.hasSchemaEvolution()) {
            return null;
        }
        SchemaEvolution evolution = detail.getSchemaEvolution();
        if (evolution.getColumnMappingsCount() == 0
                && evolution.getTypePromotionsCount() == 0) {
            return null;
        }
        // A per-file evolution is the authoritative override for that file.
        if (detail.hasPhysicalFileScan()
                && detail.getPhysicalFileScan().hasSchemaEvolution()) {
            return null;
        }
        return evolution;
    }
}
