package org.pangrova.worker.jni.write;

import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.data.page.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Projects decoded native pages onto the write schema expected by connector
 * page sinks. Extracted from {@link NativeWriteExecutor} to keep the JNI
 * adapter focused on session and sink lifecycle.
 *
 * <p>Handles three layouts: exact match (no-op), synthetic native column names
 * (positional rename), and MERGE/RIGHT-JOIN output with {@code _right}
 * suffixed duplicates (coalesce toward the target value).</p>
 */
final class WritePageProjector {

    private static final Logger LOG = LoggerFactory.getLogger(WritePageProjector.class);

    private static final String RIGHT_SUFFIX = "_right";

    private WritePageProjector() {}

    /**
     * Projects a decoded page to match the write schema column names.
     *
     * <p>The native MERGE plan output contains both source and target columns
     * (e.g., id, name, amount, id_right, name_right, amount_right, name, amount).
     * The write schema expects only the target column names (id, name, amount).
     *
     * <p>TypedRow uses a LinkedHashMap internally, so duplicate column names
     * (e.g., 'name' appearing at positions 1 and 6) are resolved by keeping
     * the LAST value — which is the update expression for MERGE UPDATE.
     * This method uses name-based access (not positional) to leverage this.</p>
     */
    static Page projectToWriteSchema(Page decodedPage, List<String> descriptorColumns) {
        List<String> sourceColumns = decodedPage.columns();
        LOG.debug("event=project_to_write_schema source_columns={} descriptor_columns={}",
                sourceColumns, descriptorColumns);

        List<String> writeColumns = resolveWriteColumns(descriptorColumns);
        LOG.debug("event=project_to_write_schema write_columns={}", writeColumns);

        if (sourceColumns.equals(writeColumns)) {
            return decodedPage;
        }
        boolean hasSyntheticSource = sourceColumns.stream().anyMatch(n -> n.startsWith("proj_"));
        if (hasSyntheticSource && sourceColumns.size() == writeColumns.size()) {
            return renamePositionally(decodedPage, sourceColumns, writeColumns);
        }
        return projectWithCoalesce(decodedPage, sourceColumns, writeColumns);
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
        LinkedHashSet<String> targetColumns = new LinkedHashSet<>(rightBaseNames);
        targetColumns.addAll(nonRightColumns);
        if (targetColumns.isEmpty()) {
            targetColumns.addAll(descriptorColumns);
        }
        return new ArrayList<>(targetColumns);
    }

    private static Page renamePositionally(Page decodedPage,
                                           List<String> sourceColumns,
                                           List<String> writeColumns) {
        List<TypedRow> renamedRows = new ArrayList<>();
        for (TypedRow row : decodedPage.rows()) {
            TypedRow.Builder b = TypedRow.builder();
            for (int i = 0; i < writeColumns.size(); i++) {
                b.put(writeColumns.get(i), row.get(sourceColumns.get(i)));
            }
            renamedRows.add(b.build());
        }
        return new Page(writeColumns, renamedRows);
    }

    private static Page projectWithCoalesce(Page decodedPage,
                                            List<String> sourceColumns,
                                            List<String> writeColumns) {
        Set<String> hasRightColumn = new HashSet<>();
        for (String col : sourceColumns) {
            if (col.endsWith(RIGHT_SUFFIX)) {
                hasRightColumn.add(col.substring(0, col.length() - RIGHT_SUFFIX.length()));
            }
        }
        List<TypedRow> projectedRows = new ArrayList<>(decodedPage.getPositionCount());
        for (TypedRow row : decodedPage.rows()) {
            projectedRows.add(projectRow(row, writeColumns, hasRightColumn));
        }
        return new Page(writeColumns, projectedRows);
    }

    private static TypedRow projectRow(TypedRow row,
                                       List<String> writeColumns,
                                       Set<String> hasRightColumn) {
        TypedRow.Builder builder = TypedRow.builder();
        for (String col : writeColumns) {
            builder.put(col, resolveColumnValue(row, col, hasRightColumn));
        }
        return builder.build();
    }

    private static ColumnValue resolveColumnValue(TypedRow row,
                                                  String col,
                                                  Set<String> hasRightColumn) {
        if (!row.containsColumn(col)) {
            if (hasRightColumn.contains(col) && row.containsColumn(col + RIGHT_SUFFIX)) {
                return row.get(col + RIGHT_SUFFIX);
            }
            return new ColumnValue.NullValue();
        }
        ColumnValue value = row.get(col);
        // COALESCE: for RIGHT JOIN output, source columns are null for
        // unmatched target rows. Fall back to the _right (target) value.
        if (value instanceof ColumnValue.NullValue
                && hasRightColumn.contains(col)
                && row.containsColumn(col + RIGHT_SUFFIX)) {
            ColumnValue rightVal = row.get(col + RIGHT_SUFFIX);
            if (!(rightVal instanceof ColumnValue.NullValue)) {
                return rightVal;
            }
        }
        return value;
    }
}
