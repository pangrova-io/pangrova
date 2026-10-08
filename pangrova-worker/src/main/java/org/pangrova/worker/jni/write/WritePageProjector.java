package org.pangrova.worker.jni.write;

import org.pangrova.protocol.batch.ColumnarBatch;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

/** Resolves write ordinals once per batch, without materializing projected rows. */
final class WritePageProjector {
    private static final String RIGHT_SUFFIX = "_right";

    private WritePageProjector() {}

    static ColumnarBatch projectToWriteSchema(ColumnarBatch batch, List<String> descriptorColumns,
            boolean requirePhysicalColumns) {
        List<String> source = batch.columns();
        boolean positional = hasCanonicalPhysicalColumns(source);
        if (requirePhysicalColumns && !positional) {
            throw new IllegalArgumentException("Native write batch lost its canonical physical column identities");
        }
        List<String> target = descriptorColumns;
        if (positional && source.size() != target.size()) {
            throw new IllegalArgumentException("Canonical write batch width differs from verified output columns");
        }
        int[] ordinals = new int[target.size()];
        int[] fallbacks = new int[target.size()];
        Arrays.fill(fallbacks, -1);
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

    /** Recognizes only the protocol's exact aliases and rejects partial or reordered physical layouts. */
    static boolean hasCanonicalPhysicalColumns(List<String> columns) {
        if (columns.stream().noneMatch(WritePageProjector::isPhysicalAlias)) {
            return false;
        }
        for (int ordinal = 0; ordinal < columns.size(); ordinal++) {
            if (!SubstraitRelationLayout.physicalAlias(ordinal).equals(columns.get(ordinal))) {
                throw new IllegalArgumentException("Native write requires complete ordered canonical physical columns");
            }
        }
        return true;
    }

    private static boolean isPhysicalAlias(String name) {
        int separator = name.lastIndexOf('_');
        if (separator < 0) return false;
        String suffix = name.substring(separator + 1);
        if (suffix.isEmpty() || suffix.length() > 10) return false;
        for (int digit = 0; digit < suffix.length(); digit++) {
            if (suffix.charAt(digit) < '0' || suffix.charAt(digit) > '9') return false;
        }
        long ordinal = Long.parseLong(suffix);
        return ordinal <= Integer.MAX_VALUE && SubstraitRelationLayout.physicalAlias((int) ordinal).equals(name);
    }

    /** Resolves only the logical MERGE output naming convention; physical layouts never use this path. */
    static List<String> resolveWriteColumns(List<String> descriptorColumns) {
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
