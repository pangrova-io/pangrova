package org.pangrova.coordinator.query.statement.dml;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.ValidationException;
import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.Literal;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;

/**
 * Fail-closed guard for {@code FILE MANAGED} columns.
 *
 * <p>{@code FILE MANAGED} declares that the engine owns a copy of the bytes. The
 * managed-copy write path is not implemented, so accepting a non-null value would
 * silently store an external reference under a declaration that promises managed
 * ownership. Until the copy path exists, every non-null write is rejected and the
 * caller is pointed at the supported alternatives.</p>
 */
public final class ManagedFileWriteGuard {

    static final String FILE_STORAGE_PROPERTY_PREFIX = "pangrova.file.storage.";
    static final String MANAGED = "MANAGED";

    private ManagedFileWriteGuard() {}

    /**
     * Rejects non-null writes to {@code FILE MANAGED} columns.
     *
     * @param tableMeta the target table metadata (nullable — treated as no-op)
     * @param columns   the resolved INSERT column names, aligned with {@code valueRows}
     * @param valueRows the INSERT VALUES rows
     * @throws ValidationException when a managed FILE column receives a non-null value
     */
    public static void requireSupported(ConnectorTableMetadata tableMeta,
                                        List<String> columns,
                                        List<List<Expression>> valueRows) {
        if (tableMeta == null || columns == null || valueRows == null) {
            return;
        }
        Map<String, String> properties = tableMeta.properties();
        if (properties == null || properties.isEmpty()) {
            return;
        }
        Set<String> managedColumns = properties.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(FILE_STORAGE_PROPERTY_PREFIX))
                .filter(entry -> MANAGED.equalsIgnoreCase(entry.getValue()))
                .map(entry -> entry.getKey().substring(FILE_STORAGE_PROPERTY_PREFIX.length()))
                .collect(Collectors.toUnmodifiableSet());
        if (managedColumns.isEmpty()) {
            return;
        }
        for (List<Expression> row : valueRows) {
            int limit = Math.min(columns.size(), row.size());
            for (int i = 0; i < limit; i++) {
                if (!managedColumns.contains(columns.get(i)) || isNull(row.get(i))) {
                    continue;
                }
                throw new ValidationException(PangrovaErrorCode.FILE_STORAGE_NOT_CONFIGURED,
                        "FILE MANAGED column '" + columns.get(i)
                                + "' cannot accept writes yet: no managed file space copies"
                                + " bytes. Upload through the files API and declare FILE"
                                + " EXTERNAL, or insert NULL");
            }
        }
    }

    private static boolean isNull(Expression expression) {
        return expression instanceof Literal literal && literal.isNull();
    }
}
