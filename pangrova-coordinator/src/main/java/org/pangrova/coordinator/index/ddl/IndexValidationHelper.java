package org.pangrova.coordinator.index.ddl;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.index.IndexTypeCompatibility;
import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.index.NativeIndexCapability;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.index.IndexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Validates preconditions for index DDL operations: table existence,
 * column compatibility, multi-column constraints, and native capability overlap.
 */
final class IndexValidationHelper {

    private static final Logger LOG = LoggerFactory.getLogger(IndexValidationHelper.class);

    private IndexValidationHelper() {}

    static void validateTableExists(
            QualifiedTableName ref, CatalogRegistry catalogRegistry, ConnectorSession session) {
        CatalogHandle handle = catalogRegistry.getCatalog(ref.catalog());
        if (handle == null) {
            throw new PangrovaException(PangrovaErrorCode.METADATA_TABLE_NOT_FOUND,
                    "Catalog not found: " + ref.catalog());
        }
        ConnectorMetadata metadata = handle.getMetadata(session);
        ConnectorTableHandle tableHandle = metadata.getTableHandle(
                session, new SchemaTableName(ref.schema(), ref.table()));
        if (tableHandle == null) {
            throw new PangrovaException(PangrovaErrorCode.METADATA_TABLE_NOT_FOUND,
                    "Table not found: " + ref);
        }
    }

    static List<String> validateColumns(QualifiedTableName ref, List<String> columns,
                                IndexType indexType, CatalogRegistry catalogRegistry, ConnectorSession session) {
        CatalogHandle handle = catalogRegistry.getCatalog(ref.catalog());
        ConnectorMetadata metadata = handle.getMetadata(session);
        ConnectorTableHandle tableHandle = metadata.getTableHandle(
                session, new SchemaTableName(ref.schema(), ref.table()));
        ConnectorTableMetadata tableMeta = metadata.getTableMetadata(session, tableHandle);

        List<String> resolvedColumns = new ArrayList<>(columns.size());
        for (String col : columns) {
            ConnectorTableMetadata.Column matched = tableMeta.typedColumns().stream()
                    .filter(c -> c.name().equalsIgnoreCase(col))
                    .findFirst()
                    .orElseThrow(() -> new PangrovaException(
                            PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR,
                            "Column '" + col + "' not found in table " + ref));

            if (!IndexTypeCompatibility.isCompatible(indexType, matched.type().name())) {
                throw new PangrovaException(PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR,
                        "Column '" + col + "' type " + matched.type().name()
                                + " is not compatible with index type " + indexType);
            }
            resolvedColumns.add(matched.name());
        }
        return List.copyOf(resolvedColumns);
    }

    static void validateMultiColumnConstraints(IndexType indexType, List<String> columns) {
        if (IndexTypeCompatibility.SINGLE_COLUMN_ONLY.contains(indexType) && columns.size() > 1) {
            throw new PangrovaException(PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR,
                    "Index type " + indexType + " supports single column only, got " + columns.size());
        }
        if (indexType == IndexType.R_TREE && (columns.size() < 2 || columns.size() > 3)) {
            throw new PangrovaException(PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR,
                    "R_TREE index requires 2 or 3 columns, got " + columns.size());
        }
    }

    static void checkNativeCapabilityOverlap(
            String catalogName, List<String> columns, IndexType indexType,
            boolean force, ConnectorAccessorResolver accessorResolver) {
        List<NativeIndexCapability> capabilities =
                accessorResolver.resolveNativeCapabilities(catalogName);
        for (NativeIndexCapability cap : capabilities) {
            boolean overlaps = columns.stream().allMatch(cap::coversColumn)
                    && nativeTypeOverlaps(cap.indexType(), indexType);
            if (overlaps) {
                handleNativeOverlap(catalogName, columns, indexType, cap, force);
            }
        }
    }

    private static void handleNativeOverlap(String catalogName, List<String> columns,
                                            IndexType indexType, NativeIndexCapability cap,
                                            boolean force) {
        if (force) {
            LOG.warn("action=native_capability_overlap_forced catalog={} columns={} "
                            + "nativeType={} indexType={} description={}",
                    catalogName, columns, cap.indexType(), indexType, cap.description());
        } else {
            throw new PangrovaException(
                    PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR,
                    "Index type " + indexType + " on columns " + columns
                            + " overlaps with native " + cap.indexType()
                            + " capability (" + cap.description()
                            + "). Use FORCE to override.");
        }
    }

    private static boolean nativeTypeOverlaps(
            org.pangrova.spi.connector.index.NativeIndexType nativeType, IndexType indexType) {
        return switch (nativeType) {
            case MIN_MAX -> indexType == IndexType.BTREE || indexType == IndexType.SPARSE;
            case BLOOM_FILTER -> indexType == IndexType.BLOOM_FILTER;
            case BITMAP -> indexType == IndexType.BITMAP;
            case BSI -> indexType == IndexType.BTREE;
        };
    }
}
