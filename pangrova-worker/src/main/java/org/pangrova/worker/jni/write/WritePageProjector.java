package org.pangrova.worker.jni.write;

import org.pangrova.protocol.batch.ColumnarBatch;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

/** Resolves write ordinals once per batch, without materializing projected rows. */
final class WritePageProjector {
    private static final String RIGHT_SUFFIX = "_right";

    private WritePageProjector() {}

    static ColumnarBatch projectToWriteSchema(ColumnarBatch batch, List<String> descriptorColumns) {
        List<String> source = batch.columns();
        List<String> target = resolveWriteColumns(descriptorColumns);
        int[] ordinals = new int[target.size()];
        int[] fallbacks = new int[target.size()];
        Arrays.fill(fallbacks, -1);
        boolean positional = source.size() == target.size()
                && source.stream().anyMatch(name -> name.startsWith("proj_"));
        for (int i = 0; i < target.size(); i++) {
            // MERGE update expressions can repeat source names. Last occurrence
            // wins, matching the previous row-map semantics, including nulls.
            ordinals[i] = positional ? i : source.lastIndexOf(target.get(i));
            if (!positional && !source.equals(target)) {
                fallbacks[i] = source.lastIndexOf(target.get(i) + RIGHT_SUFFIX);
            }
        }
        return batch.project(target, ordinals, fallbacks);
    }

    private static List<String> resolveWriteColumns(List<String> descriptorColumns) {
        LinkedHashSet<String> nonRightColumns = new LinkedHashSet<>();
        LinkedHashSet<String> rightBaseNames = new LinkedHashSet<>();
        for (String col : descriptorColumns) {
            if (col.endsWith(RIGHT_SUFFIX)) {
                rightBaseNames.add(col.substring(0, col.length() - RIGHT_SUFFIX.length()));
            } else {
                nonRightColumns.add(col);
            }
        }
        rightBaseNames.addAll(nonRightColumns);
        return List.copyOf(rightBaseNames);
    }
}
