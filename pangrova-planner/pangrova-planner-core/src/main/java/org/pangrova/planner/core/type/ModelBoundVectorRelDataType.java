package org.pangrova.planner.core.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.ArraySqlType;

/**
 * Planner marker for a first-class VECTOR whose dimension is selected from
 * the exact model deployment before dispatch.
 *
 * <p>Substrait and the Native engine use a nullable list of required FP32 as
 * the physical column representation. This distinct Calcite type prevents an
 * AI embedding from being treated as an arbitrary SQL ARRAY while the exact
 * dimension remains a mandatory {@code ModelBinding} contract.</p>
 */
public final class ModelBoundVectorRelDataType extends ArraySqlType {

    public ModelBoundVectorRelDataType(RelDataType elementType, boolean nullable) {
        super(elementType, nullable);
    }

    @Override
    protected void generateTypeString(StringBuilder builder, boolean withDetail) {
        builder.append("VECTOR(EXACT_MODEL)");
        if (!isNullable()) {
            builder.append(" NOT NULL");
        }
    }
}
