package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.type.IcebergTypeMapper;
import org.pangrova.types.enums.PangrovaType;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Schema DDL operations for Iceberg tables: add/drop/rename/change column, partition spec.
 * Extracted from IcebergMetadata to keep class size under 500 lines.
 */
final class IcebergDdlOperations {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergDdlOperations.class);

    private IcebergDdlOperations() {}

    /** Drops the table, purging its underlying data. */
    static void dropTable(Catalog catalog, IcebergTableHandle handle) {
        TableIdentifier id = TableIdentifier.of(handle.schemaName(), handle.tableName());
        catalog.dropTable(id, true);
        LOG.info("Dropped Iceberg table {}.{}", handle.schemaName(), handle.tableName());
    }

    /** Renames the table into the target schema/table names. */
    static void renameTable(Catalog catalog, IcebergTableHandle handle,
                            String newSchemaName, String newTableName) {
        TableIdentifier from = TableIdentifier.of(handle.schemaName(), handle.tableName());
        TableIdentifier to = TableIdentifier.of(newSchemaName, newTableName);
        catalog.renameTable(from, to);
    }

    static void addColumn(Catalog catalog, IcebergTableHandle handle,
                          String columnName, PangrovaType columnType) {
        addColumn(catalog, handle, columnName, columnType, null);
    }

    static void addColumn(Catalog catalog, IcebergTableHandle handle,
                          String columnName, PangrovaType columnType, Object defaultValue) {
        Table icebergTable = loadTable(catalog, handle);
        Type icebergType;
        if (columnType == PangrovaType.STRUCT) {
            // STRUCT columns added via ALTER TABLE are typically FILE type expansions.
            // Build the FILE struct schema: STRUCT(uri VARCHAR, mime_type VARCHAR,
            // size BIGINT, checksum VARCHAR, created_at TIMESTAMP)
            int baseId = icebergTable.schema().highestFieldId() + 100;
            icebergType = Types.StructType.of(
                    Types.NestedField.optional(baseId, "uri", Types.StringType.get()),
                    Types.NestedField.optional(baseId + 1, "mime_type", Types.StringType.get()),
                    Types.NestedField.optional(baseId + 2, "size", Types.LongType.get()),
                    Types.NestedField.optional(baseId + 3, "checksum", Types.StringType.get()),
                    Types.NestedField.optional(baseId + 4, "created_at", Types.TimestampType.withoutZone()));
        } else {
            icebergType = IcebergTypeMapper.toIcebergType(columnType);
        }
        UpdateSchema updateSchema;
        if (defaultValue != null) {
            Literal<?> icebergDefault = toIcebergLiteral(defaultValue, icebergType);
            updateSchema = icebergTable.updateSchema()
                    .addColumn(columnName, icebergType, icebergDefault);
            LOG.info("event=add_column_with_default table={}.{} column={} type={} default={}",
                    handle.schemaName(), handle.tableName(), columnName, columnType, defaultValue);
        } else {
            updateSchema = icebergTable.updateSchema()
                    .addColumn(columnName, icebergType);
        }
        updateSchema.commit();
        LOG.info("Added column '{}' ({}) to {}.{}", columnName, columnType,
                handle.schemaName(), handle.tableName());
    }

    /**
     * Converts a raw Java default value (from the coordinator expression evaluator)
     * to an Iceberg {@link Literal} for use with {@link UpdateSchema#addColumn}.
     *
     * <p>Supported conversions: Long→long, Integer→int, Double→double, Float→double,
     * String→string, Boolean→boolean, BigDecimal→decimal, UUID→uuid.</p>
     */
    static Literal<?> toIcebergLiteral(Object value) {
        if (value instanceof Number n) {
            return numericToLiteral(n);
        } else if (value instanceof String s) {
            return Literal.of(s);
        } else if (value instanceof Boolean b) {
            return Literal.of(b);
        } else if (value instanceof UUID uuid) {
            return Literal.of(uuid);
        }
        throw new IllegalArgumentException(
                "Unsupported default value type: " + value.getClass().getName());
    }

    /**
     * Same as {@link #toIcebergLiteral(Object)} but additionally casts the resulting
     * literal to the target Iceberg primitive type. Required because SQL literals like
     * {@code DEFAULT 10} are parsed as INTEGER, while the target column may be BIGINT;
     * Iceberg requires the literal's logical type to match the column type exactly.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static Literal<?> toIcebergLiteral(Object value, Type targetType) {
        Literal raw = toIcebergLiteral(value);
        if (targetType instanceof Type.PrimitiveType primitive) {
            Literal casted = raw.to(primitive);
            if (casted == null) {
                throw new IllegalArgumentException(
                        "Default value " + value + " cannot be cast to " + targetType);
            }
            return casted;
        }
        throw new IllegalArgumentException(
                "Default values for non-primitive column type " + targetType + " are unsupported");
    }

    private static Literal<?> numericToLiteral(Number value) {
        if (value instanceof Long l) {
            return Literal.of(l);
        } else if (value instanceof Integer i) {
            return Literal.of(i);
        } else if (value instanceof Double d) {
            return Literal.of(d);
        } else if (value instanceof Float f) {
            return Literal.of((double) f);
        } else if (value instanceof BigDecimal bd) {
            return Literal.of(bd);
        }
        throw new IllegalArgumentException(
                "Unsupported numeric default value type: " + value.getClass().getName());
    }

    static void dropColumn(Catalog catalog, IcebergTableHandle handle, String columnName) {
        Table icebergTable = loadTable(catalog, handle);
        icebergTable.updateSchema()
                .deleteColumn(columnName)
                .commit();
        LOG.info("Dropped column '{}' from {}.{}", columnName,
                handle.schemaName(), handle.tableName());
    }

    static void renameColumn(Catalog catalog, IcebergTableHandle handle,
                             String oldName, String newName) {
        Table icebergTable = loadTable(catalog, handle);
        icebergTable.updateSchema()
                .renameColumn(oldName, newName)
                .commit();
        LOG.info("Renamed column '{}' to '{}' in {}.{}", oldName, newName,
                handle.schemaName(), handle.tableName());
    }

    static void changeColumnType(Catalog catalog, IcebergTableHandle handle,
                                 String columnName, String newType) {
        Table icebergTable = loadTable(catalog, handle);
        Type icebergType = IcebergTypeMapper.toIcebergType(PangrovaType.fromSqlTypeName(newType));
        Types.NestedField existingField = icebergTable.schema().findField(columnName);
        if (existingField != null
                && existingField.type().typeId() == Type.TypeID.UNKNOWN) {
            icebergTable.updateSchema()
                    .deleteColumn(columnName)
                    .addColumn(columnName, icebergType)
                    .commit();
        } else {
            icebergTable.updateSchema()
                    .updateColumn(columnName, icebergType.asPrimitiveType())
                    .commit();
        }
        LOG.info("Changed column '{}' type to '{}' in {}.{}", columnName, newType,
                handle.schemaName(), handle.tableName());
    }

    static void createSchema(Catalog catalog, String schemaName) {
        if (catalog instanceof SupportsNamespaces nsCatalog) {
            Namespace ns = Namespace.of(schemaName);
            nsCatalog.createNamespace(ns);
            LOG.info("Created Iceberg namespace '{}'", schemaName);
        }
    }

    static void setPartitionSpec(Catalog catalog, IcebergTableHandle handle,
                                 List<Map.Entry<String, List<String>>> transforms) {
        Table icebergTable = loadTable(catalog, handle);
        org.apache.iceberg.UpdatePartitionSpec updateSpec = icebergTable.updateSpec();
        removeExistingFields(icebergTable, updateSpec);
        applyTransforms(transforms, updateSpec);
        updateSpec.commit();
        LOG.info("event=partition_spec_updated table={}.{} transforms={}",
                handle.schemaName(), handle.tableName(), transforms.size());
    }

    private static void removeExistingFields(Table table,
                                             org.apache.iceberg.UpdatePartitionSpec updateSpec) {
        for (org.apache.iceberg.PartitionField field : table.spec().fields()) {
            updateSpec.removeField(field.name());
        }
    }

    private static void applyTransforms(List<Map.Entry<String, List<String>>> transforms,
                                        org.apache.iceberg.UpdatePartitionSpec updateSpec) {
        for (var transform : transforms) {
            String funcName = transform.getKey();
            List<String> args = transform.getValue();
            if (funcName == null || funcName.isEmpty()) {
                // Identity transform — partition by raw column value
                updateSpec.addField(args.get(0));
            } else {
                applyNamedTransform(updateSpec, funcName, args);
            }
        }
    }

    private static void applyNamedTransform(org.apache.iceberg.UpdatePartitionSpec updateSpec,
                                            String funcName, List<String> args) {
        String colName = args.get(0);
        switch (funcName.toLowerCase()) {
            case "year" -> updateSpec.addField(Expressions.year(colName));
            case "month" -> updateSpec.addField(Expressions.month(colName));
            case "day" -> updateSpec.addField(Expressions.day(colName));
            case "hour" -> updateSpec.addField(Expressions.hour(colName));
            case "bucket" -> applyBucketTransform(updateSpec, args);
            case "truncate" -> {
                int width = args.size() > 1 ? parseIntArg(args.get(args.size() - 1), 10) : 10;
                updateSpec.addField(Expressions.truncate(colName, width));
            }
            default -> throw new UnsupportedOperationException(
                    "Unsupported partition transform: " + funcName);
        }
    }

    private static void applyBucketTransform(org.apache.iceberg.UpdatePartitionSpec updateSpec,
                                             List<String> args) {
        int numBuckets = args.size() > 1 ? parseIntArg(args.get(args.size() - 1), 16) : 16;
        if (args.size() <= 2) {
            updateSpec.addField(Expressions.bucket(args.get(0), numBuckets));
            return;
        }
        for (int i = 0; i < args.size() - 1; i++) {
            updateSpec.addField(Expressions.bucket(args.get(i), numBuckets));
        }
    }

    private static int parseIntArg(String arg, int defaultValue) {
        try {
            return Integer.parseInt(arg.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static Table loadTable(Catalog catalog, IcebergTableHandle handle) {
        TableIdentifier id = TableIdentifier.of(handle.schemaName(), handle.tableName());
        return catalog.loadTable(id);
    }
}
