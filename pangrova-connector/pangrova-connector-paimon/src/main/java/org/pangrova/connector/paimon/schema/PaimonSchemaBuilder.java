package org.pangrova.connector.paimon.schema;

import java.util.Map;

import org.apache.paimon.schema.Schema;
import org.pangrova.connector.paimon.type.PaimonTypeMapper;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;

/**
 * Extracts Paimon schema construction from PaimonMetadata.
 * Stateless helper — all methods are static.
 */
public final class PaimonSchemaBuilder {

    private PaimonSchemaBuilder() {}

    /**
     * Builds a Paimon {@link Schema} from connector table metadata,
     * applying primary-key, partition, bucket, and format properties.
     */
    public static Schema buildPaimonSchema(ConnectorTableMetadata metadata) {
        Schema.Builder builder = Schema.newBuilder();
        for (ConnectorTableMetadata.Column col : metadata.typedColumns()) {
            builder.column(col.name(), toPaimonTypeWithParameterized(col));
            if (col.type() == PangrovaType.VARIANT) {
                builder.option(PaimonTypeMapper.VARIANT_COLUMN_KEY_PREFIX + col.name(), "true");
            }
        }
        applyTableProperties(builder, metadata.properties());
        return builder.build();
    }

    /**
     * Converts a column to Paimon DataType, using the parameterized type when available
     * (e.g. DECIMAL precision/scale) and falling back to the enum-based mapper.
     */
    private static org.apache.paimon.types.DataType toPaimonTypeWithParameterized(
            ConnectorTableMetadata.Column col) {
        if (col.parameterizedType() instanceof org.pangrova.spi.connector.data.type.primitive.DecimalType dt) {
            return new org.apache.paimon.types.DecimalType(dt.precision(), dt.scale());
        }
        return PaimonTypeMapper.toPaimonType(col.type());
    }

    private static void applyTableProperties(Schema.Builder builder, Map<String, String> properties) {
        applyKeyProperty(builder, properties, "primary-key");
        applyPartitionProperty(builder, properties);
        applyDefaultBucket(builder, properties);
        applyCrossPartitionDefaults(builder, properties);
        applySortCompactionColumns(builder, properties);
        // Default to Parquet format when not explicitly specified.
        // Paimon defaults to ORC, but the native engine (Velox) has limited
        // ORC support for complex queries. Parquet is the standard format.
        if (!properties.containsKey("file.format")) {
            builder.option("file.format", "parquet");
        }
        properties.forEach((key, value) -> {
            if (!"primary-key".equals(key) && !"partition-by".equals(key)
                    && !"sorted-by".equals(key) && !"sort-method".equals(key)) {
                builder.option(key, value);
            }
        });
    }

    private static void applyKeyProperty(Schema.Builder builder, Map<String, String> properties, String key) {
        String value = properties.get(key);
        if (value != null) {
            builder.primaryKey(value.split(","));
        }
    }

    private static void applyPartitionProperty(Schema.Builder builder, Map<String, String> properties) {
        String partitionBy = properties.get("partition-by");
        if (partitionBy != null) {
            builder.partitionKeys(partitionBy.split(","));
        }
    }

    private static void applyDefaultBucket(Schema.Builder builder, Map<String, String> properties) {
        // PK tables without explicit bucket config default to dynamic bucket mode,
        // which requires TableWrite.write(row, bucket). Set fixed bucket=1 as fallback
        // since our write path uses the simpler TableWrite.write(row) API.
        if (properties.containsKey("primary-key") && !properties.containsKey("bucket")) {
            builder.option("bucket", "1");
        }
    }

    private static void applyCrossPartitionDefaults(Schema.Builder builder, Map<String, String> properties) {
        if ("true".equalsIgnoreCase(properties.get("cross-partition-upsert"))
                && !properties.containsKey("changelog-producer")) {
            builder.option("changelog-producer", "lookup");
        }
    }

    /**
     * Converts the Pangrova {@code sorted-by} property to Paimon's
     * {@code sort-compaction.columns} table option. Paimon uses this
     * property to sort data files during compaction for better data skipping.
     */
    private static void applySortCompactionColumns(Schema.Builder builder, Map<String, String> properties) {
        String sortedBy = properties.get("sorted-by");
        if (sortedBy == null || sortedBy.isBlank()) {
            return;
        }
        // sorted-by format: "col1:asc,col2:desc" — extract column names only
        StringBuilder columns = new StringBuilder();
        for (String entry : sortedBy.split(",")) {
            String trimmed = entry.trim();
            String columnName = trimmed.contains(":")
                    ? trimmed.substring(0, trimmed.indexOf(':')).trim()
                    : trimmed;
            if (!columns.isEmpty()) {
                columns.append(",");
            }
            columns.append(columnName);
        }
        builder.option("sort-compaction.columns", columns.toString());

        // If z-order is specified, use zorder sort strategy
        String sortMethod = properties.get("sort-method");
        if ("zorder".equalsIgnoreCase(sortMethod)) {
            builder.option("sort-compaction.sort-strategy", "zorder");
        }
    }
}
