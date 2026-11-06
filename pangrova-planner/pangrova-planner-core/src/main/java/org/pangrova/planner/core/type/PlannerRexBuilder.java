package org.pangrova.planner.core.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/** Builds engine-typed comparisons before Calcite can apply Java UUID ordering. */
public final class PlannerRexBuilder extends RexBuilder {
    public PlannerRexBuilder(RelDataTypeFactory types) { super(types); }

    @Override
    public RexNode makeCall(SqlParserPos position, RelDataType type, SqlOperator operator, List<RexNode> args) {
        return super.makeCall(position, type, UuidOrderingSqlFunction.bind(operator, args), args);
    }

    @Override
    public RexNode makeCall(SqlParserPos position, SqlOperator operator, List<? extends RexNode> args) {
        RelDataType type = deriveReturnType(operator, args);
        return super.makeCall(position, type, UuidOrderingSqlFunction.bind(operator, args), List.copyOf(args));
    }
}
