package org.pangrova.metadata.resolution;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import java.util.List;

/** Enforces the same-catalog metadata rule for table rename targets. */
public final class QualifiedTableNames {
    private QualifiedTableNames() {}

    /** Resolves a rename target within the source catalog. */
    public static QualifiedTableName resolveRenameTarget(List<String> parts, QualifiedTableName source) {
        QualifiedTableName target = QualifiedTableName.resolve(parts, source.catalog(), source.schema());
        if (!target.catalog().equals(source.catalog())) {
            throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "ALTER TABLE RENAME cannot move a table between catalogs");
        }
        return target;
    }
}
