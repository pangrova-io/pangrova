package org.pangrova.planner.core.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.v2.LogicalType;

import java.util.Objects;

/** SQL identity of a declared storage VECTOR; its physical projection remains the canonical LIST. */
public final class StorageVectorRelDataType extends ArraySqlType {
    private final LogicalType logicalType;

    StorageVectorRelDataType(RelDataType elementType, LogicalType logicalType) {
        super(elementType, logicalType.getSubstraitType().getList().getNullability()
                == io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE);
        VectorLogicalType.requireStorageDimension(logicalType);
        this.logicalType = logicalType;
        // ArraySqlType computes a digest during its own construction. Recompute
        // only after the complete canonical VECTOR identity is available.
        computeDigest();
    }

    static StorageVectorRelDataType create(LogicalType logicalType, RelDataTypeFactory factory) {
        VectorLogicalType.requireStorageDimension(logicalType);
        var component = logicalType.getSubstraitType().getList().getType();
        var nullable = component.hasFp32() ? component.getFp32().getNullability()
                : component.getFp64().getNullability();
        RelDataType element = factory.createTypeWithNullability(factory.createSqlType(
                component.hasFp32() ? SqlTypeName.REAL : SqlTypeName.DOUBLE),
                nullable == io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE);
        return new StorageVectorRelDataType(element, logicalType);
    }

    /** Returns the same immutable declaration used by canonical schema compilation. */
    public LogicalType logicalType() {
        return logicalType;
    }

    @Override
    protected void generateTypeString(StringBuilder builder, boolean withDetail) {
        if (logicalType == null) {
            // Superclass construction precedes this class's final-field assignment.
            super.generateTypeString(builder, withDetail);
            return;
        }
        builder.append(logicalType.getCanonicalSql());
        if (withDetail) {
            var component = getComponentType();
            if (component == null) throw new IllegalStateException("VECTOR element type is missing");
            builder.append(" ELEMENT ").append(component.getFullTypeString());
        }
    }

    @Override
    public boolean deepEquals(Object other) {
        var component = getComponentType();
        if (component == null) throw new IllegalStateException("VECTOR element type is missing");
        return other instanceof StorageVectorRelDataType vector
                && logicalType.equals(vector.logicalType)
                && component.equals(vector.getComponentType());
    }

    @Override
    public int deepHashCode() {
        return Objects.hash(logicalType, getComponentType());
    }
}
