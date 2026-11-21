package org.pangrova.planner.metadata;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.api.annotation.Internal;

/** Validates compiled table identity before metadata, policy, rewrite or serialization access. */
@Internal
public final class PlanTable {
    private PlanTable() {}

    /** Returns the table attached to the compiled relation, never a fresh catalog lookup. */
    public static RelOptTable require(RelNode relation) {
        if (relation == null) {
            throw invalid("Compiled relation is absent");
        }
        RelOptTable table;
        if (relation instanceof TableScan scan) {
            table = scan.getTable();
        } else if (relation instanceof TableModify modify) {
            table = modify.getTable();
        } else {
            throw invalid("Compiled relation does not carry a table: " + relation.getRelTypeName());
        }
        if (table == null) {
            throw invalid("Compiled table identity is absent: " + relation.getRelTypeName());
        }
        return table;
    }

    private static PangrovaException invalid(String message) {
        return new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message);
    }
}
