package org.pangrova.planner.type;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

/** Canonical planner type factory: invalid decimal declarations must never be clamped. */
public final class PlannerTypeFactory extends JavaTypeFactoryImpl {

    public PlannerTypeFactory() {
        super(PlannerTypeSystem.INSTANCE);
    }

    @Override
    public RelDataType createSqlType(SqlTypeName typeName, int precision, int scale) {
        if (typeName == SqlTypeName.DECIMAL) {
            PlannerTypeSystem.INSTANCE.validateDecimal(precision, scale);
        }
        return super.createSqlType(typeName, precision, scale);
    }
}
