package org.pangrova.planner.core.type;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeMappingRule;

import java.util.List;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.v2.LogicalType;

/** Canonical planner type factory: invalid decimal declarations must never be clamped. */
public final class PlannerTypeFactory extends JavaTypeFactoryImpl {

    public PlannerTypeFactory() {
        super(PlannerTypeSystem.INSTANCE);
    }

    /** Projects the sole canonical VECTOR declaration into a distinct SQL collection type. */
    public StorageVectorRelDataType createStorageVector(LogicalType logicalType) {
        return (StorageVectorRelDataType) canonize(StorageVectorRelDataType.create(logicalType, this));
    }

    @Override
    public RelDataType createTypeWithNullability(RelDataType type, boolean nullable) {
        if (type instanceof StorageVectorRelDataType vector) {
            var logical = vector.logicalType();
            var component = vector.getComponentType();
            if (component == null) throw new IllegalStateException("VECTOR element type is missing");
            return createStorageVector(VectorLogicalType.create(VectorLogicalType.requireStorageDimension(logical),
                    logical.getSubstraitType().getList().getType().hasFp32()
                            ? VectorLogicalType.FLOAT32 : VectorLogicalType.FLOAT64,
                    nullable, component.isNullable()));
        }
        if (type instanceof ModelBoundVectorRelDataType) {
            return canonize(new ModelBoundVectorRelDataType(copyType(type.getComponentType()), nullable));
        }
        return super.createTypeWithNullability(type, nullable);
    }

    @Override
    public RelDataType enforceTypeWithNullability(RelDataType type, boolean nullable) {
        if (type instanceof StorageVectorRelDataType || type instanceof ModelBoundVectorRelDataType) {
            return createTypeWithNullability(type, nullable);
        }
        return super.enforceTypeWithNullability(type, nullable);
    }

    /** A common VECTOR keeps its dimension only when every non-null branch declares that dimension. */
    @Override
    public RelDataType leastRestrictive(List<RelDataType> types, SqlTypeMappingRule mappingRule) {
        var branches = types.stream().filter(type -> type.getSqlTypeName() != SqlTypeName.NULL).toList();
        if (branches.isEmpty() || branches.stream().anyMatch(type -> !(type instanceof StorageVectorRelDataType))) {
            return super.leastRestrictive(types, mappingRule);
        }
        var first = (StorageVectorRelDataType) branches.get(0);
        int dimension = VectorLogicalType.requireStorageDimension(first.logicalType());
        if (branches.stream().anyMatch(type -> VectorLogicalType.requireStorageDimension(
                ((StorageVectorRelDataType) type).logicalType()) != dimension)) {
            // A union of different fixed dimensions is an ordinary variable-width SQL ARRAY.
            return super.leastRestrictive(types, mappingRule);
        }
        var element = super.leastRestrictive(
                branches.stream().map(RelDataType::getComponentType).toList(), mappingRule);
        if (element == null) return null;
        return createStorageVector(VectorLogicalType.create(dimension,
                element.getSqlTypeName() == SqlTypeName.DOUBLE ? VectorLogicalType.FLOAT64 : VectorLogicalType.FLOAT32,
                types.stream().anyMatch(type -> type.isNullable() || type.getSqlTypeName() == SqlTypeName.NULL),
                element.isNullable()));
    }

    @Override
    public RelDataType createSqlType(SqlTypeName typeName) {
        // Calcite interns types across factories by their digest. An unparameterized
        // FLOAT could otherwise reuse a default factory's double-precision type.
        return typeName == SqlTypeName.FLOAT
                ? super.createSqlType(typeName, getTypeSystem().getDefaultPrecision(typeName))
                : super.createSqlType(typeName);
    }

    @Override
    public RelDataType createSqlType(SqlTypeName typeName, int precision, int scale) {
        if (typeName == SqlTypeName.DECIMAL) {
            PlannerTypeSystem.INSTANCE.validateDecimal(precision, scale);
        }
        return super.createSqlType(typeName, precision, scale);
    }
}
