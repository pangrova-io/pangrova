package org.pangrova.metadata.core;

import org.pangrova.metadata.api.schema.ColumnMetadata;
import org.pangrova.metadata.api.schema.ColumnType;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.type.Type;
import org.pangrova.spi.connector.data.type.primitive.DecimalType;
import org.pangrova.spi.connector.data.type.primitive.VarcharType;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;

import java.util.Locale;
import java.util.OptionalInt;

/** Preserves scalar declarations crossing from Metadata's model to the connector creation contract. */
final class MetadataColumnDeclarations {
    private MetadataColumnDeclarations() { }

    static ConnectorTableMetadata.Column connectorColumn(ColumnMetadata column) {
        var declaration = column.type();
        String name = declaration.baseType().trim().toUpperCase(Locale.ROOT);
        var type = switch (name) {
            case "INT" -> PangrovaType.INTEGER;
            case "REAL" -> PangrovaType.FLOAT;
            case "STRING" -> PangrovaType.VARCHAR;
            case "NUMERIC" -> PangrovaType.DECIMAL;
            default -> PangrovaType.valueOf(name);
        };
        if (type == PangrovaType.ARRAY || type == PangrovaType.MAP || type == PangrovaType.STRUCT
                || type == PangrovaType.VECTOR || type == PangrovaType.UNKNOWN) {
            throw new IllegalArgumentException("Metadata creation requires a complete scalar declaration: " + name);
        }
        Type parameters = parameters(type, declaration);
        return new ConnectorTableMetadata.Column(column.name(), type, OptionalInt.empty(), parameters);
    }

    private static Type parameters(PangrovaType type, ColumnType declaration) {
        if (type == PangrovaType.DECIMAL) {
            if (declaration.length() != 0) throw new IllegalArgumentException("DECIMAL cannot declare a length");
            return new DecimalType(declaration.precision(), declaration.scale());
        }
        if (declaration.precision() != 0 || declaration.scale() != 0) {
            throw new IllegalArgumentException("Only DECIMAL accepts precision and scale in Metadata creation");
        }
        if (type == PangrovaType.VARCHAR) {
            return declaration.length() == 0 ? VarcharType.UNBOUNDED : new VarcharType(declaration.length());
        }
        if (declaration.length() != 0) {
            throw new IllegalArgumentException("Only VARCHAR accepts a length in Metadata creation");
        }
        return null;
    }
}
