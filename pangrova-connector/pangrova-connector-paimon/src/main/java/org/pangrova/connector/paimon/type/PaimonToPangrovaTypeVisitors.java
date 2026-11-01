package org.pangrova.connector.paimon.type;

import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.type.mapping.PangrovaTypeMapping;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataTypeVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.types.VariantType;

/**
 * Visitor-based Paimon DataType → Pangrova type conversions (read direction of
 * the type mapping). Using the visitor pattern avoids instanceof chains and
 * ensures compile-time coverage of new Paimon types.
 */
final class PaimonToPangrovaTypeVisitors {

    private PaimonToPangrovaTypeVisitors() {}

    /**
     * Visitor-based Paimon DataType → PangrovaType conversion.
     */
    static final class PangrovaTypeVisitor implements DataTypeVisitor<PangrovaType> {

        @Override
        public PangrovaType visit(BooleanType booleanType) {
            return PangrovaType.BOOLEAN;
        }
        @Override
        public PangrovaType visit(TinyIntType tinyIntType) {
            return PangrovaType.TINYINT;
        }
        @Override
        public PangrovaType visit(SmallIntType smallIntType) {
            return PangrovaType.SMALLINT;
        }
        @Override
        public PangrovaType visit(IntType intType) {
            return PangrovaType.INTEGER;
        }
        @Override
        public PangrovaType visit(BigIntType bigIntType) {
            return PangrovaType.BIGINT;
        }
        @Override
        public PangrovaType visit(FloatType floatType) {
            return PangrovaType.FLOAT;
        }
        @Override
        public PangrovaType visit(DoubleType doubleType) {
            return PangrovaType.DOUBLE;
        }
        @Override
        public PangrovaType visit(DecimalType decimalType) {
            return PangrovaType.DECIMAL;
        }
        @Override
        public PangrovaType visit(CharType charType) {
            return PangrovaType.VARCHAR;
        }
        @Override
        public PangrovaType visit(VarCharType varCharType) {
            return PangrovaType.VARCHAR;
        }
        @Override
        public PangrovaType visit(BinaryType binaryType) {
            return PangrovaType.VARBINARY;
        }
        @Override
        public PangrovaType visit(VarBinaryType varBinaryType) {
            return PangrovaType.VARBINARY;
        }
        @Override
        public PangrovaType visit(DateType dateType) {
            return PangrovaType.DATE;
        }
        @Override
        public PangrovaType visit(TimeType timeType) {
            return PangrovaType.TIME;
        }
        @Override
        public PangrovaType visit(TimestampType timestampType) {
            return PangrovaType.TIMESTAMP;
        }
        @Override
        public PangrovaType visit(LocalZonedTimestampType localZonedTimestampType) {
            return PangrovaType.TIMESTAMP_TZ;
        }
        @Override
        public PangrovaType visit(ArrayType arrayType) {
            return PangrovaType.ARRAY;
        }
        @Override
        public PangrovaType visit(MapType mapType) {
            return PangrovaType.MAP;
        }
        @Override
        public PangrovaType visit(MultisetType multisetType) {
            return PangrovaType.MAP;
        }
        @Override
        public PangrovaType visit(RowType rowType) {
            return PangrovaType.STRUCT;
        }
        @Override
        public PangrovaType visit(VariantType variantType) {
            return PangrovaType.VARIANT;
        }
        @Override
        public PangrovaType visit(org.apache.paimon.types.VectorType vectorType) {
            return vectorType.getElementType() instanceof FloatType
                    || vectorType.getElementType() instanceof DoubleType
                    ? PangrovaType.VECTOR : PangrovaType.ARRAY;
        }
        @Override
        public PangrovaType visit(org.apache.paimon.types.BlobType blobType) {
            return PangrovaType.VARBINARY;
        }
    }

    /**
     * Visitor-based Paimon DataType → parameterized Type conversion.
     * Preserves full parameter information for DECIMAL, ARRAY, MAP, and ROW.
     */
    static final class ParameterizedTypeVisitor
            implements DataTypeVisitor<org.pangrova.spi.connector.data.type.Type> {

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(BooleanType t) {
            return PangrovaTypeMapping.toType(PangrovaType.BOOLEAN);
        }

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(TinyIntType t) {
            return PangrovaTypeMapping.toType(PangrovaType.TINYINT);
        }

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(SmallIntType t) {
            return PangrovaTypeMapping.toType(PangrovaType.SMALLINT);
        }

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(IntType t) {
            return PangrovaTypeMapping.toType(PangrovaType.INTEGER);
        }

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(BigIntType t) {
            return PangrovaTypeMapping.toType(PangrovaType.BIGINT);
        }

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(FloatType t) {
            return PangrovaTypeMapping.toType(PangrovaType.FLOAT);
        }

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(DoubleType t) {
            return PangrovaTypeMapping.toType(PangrovaType.DOUBLE);
        }

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(DecimalType dt) {
            return new org.pangrova.spi.connector.data.type.primitive.DecimalType(
                    dt.getPrecision(), dt.getScale());
        }

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(CharType t) {
            return new org.pangrova.spi.connector.data.type.primitive.CharType(t.getLength());
        }

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(VarCharType t) {
            int len = t.getLength();
            if (len == VarCharType.MAX_LENGTH) {
                return org.pangrova.spi.connector.data.type.primitive.VarcharType.UNBOUNDED;
            }
            return new org.pangrova.spi.connector.data.type.primitive.VarcharType(len);
        }

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(BinaryType t) {
            return PangrovaTypeMapping.toType(PangrovaType.VARBINARY);
        }

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(VarBinaryType t) {
            return PangrovaTypeMapping.toType(PangrovaType.VARBINARY);
        }

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(DateType t) {
            return PangrovaTypeMapping.toType(PangrovaType.DATE);
        }

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(TimeType t) {
            return PangrovaTypeMapping.toType(PangrovaType.TIME);
        }

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(TimestampType t) {
            return PangrovaTypeMapping.toType(PangrovaType.TIMESTAMP);
        }

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(LocalZonedTimestampType t) {
            return PangrovaTypeMapping.toType(PangrovaType.TIMESTAMP_TZ);
        }

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(ArrayType at) {
            return new org.pangrova.spi.connector.data.type.ArrayType(
                    at.getElementType().accept(this));
        }

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(MapType mt) {
            return new org.pangrova.spi.connector.data.type.MapType(
                    mt.getKeyType().accept(this),
                    mt.getValueType().accept(this));
        }

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(MultisetType mt) {
            return new org.pangrova.spi.connector.data.type.MapType(
                    mt.getElementType().accept(this),
                    PangrovaTypeMapping.toType(PangrovaType.INTEGER));
        }

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(RowType rt) {
            var fields = rt.getFields().stream()
                    .map(f -> new org.pangrova.spi.connector.data.type.RowField(
                            f.name(), f.type().accept(this)))
                    .toList();
            return new org.pangrova.spi.connector.data.type.RowType(fields);
        }

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(VariantType variantType) {
            return PangrovaTypeMapping.toType(PangrovaType.VARIANT);
        }

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(org.apache.paimon.types.VectorType vectorType) {
            if (vectorType.getElementType() instanceof FloatType
                    || vectorType.getElementType() instanceof DoubleType) {
                return new org.pangrova.spi.connector.data.type.VectorType(vectorType.getLength(),
                        vectorType.getElementType() instanceof DoubleType
                                ? org.pangrova.spi.connector.data.type.VectorType.ElementType.DOUBLE
                                : org.pangrova.spi.connector.data.type.VectorType.ElementType.FLOAT);
            }
            return new org.pangrova.spi.connector.data.type.ArrayType(
                    vectorType.getElementType().accept(this));
        }

        @Override
        public org.pangrova.spi.connector.data.type.Type visit(org.apache.paimon.types.BlobType blobType) {
            return PangrovaTypeMapping.toType(PangrovaType.VARBINARY);
        }
    }
}
