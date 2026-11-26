package org.pangrova.coordinator.indexing.ddl;

import org.pangrova.coordinator.indexing.connector.ConnectorAccessorResolver;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.indexing.IndexTypeCompatibility;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.index.NativeIndexCapability;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.indexing.api.enums.IndexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Validates preconditions for index DDL operations: table existence,
 * column compatibility, multi-column constraints, and native capability overlap.
 */
final class IndexDdlValidation {

    private static final Logger LOG = LoggerFactory.getLogger(IndexDdlValidation.class);

    private IndexDdlValidation() {}

    static ConnectorTableMetadata resolveTable(
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
        return metadata.getTableMetadata(session, tableHandle);
    }

    static List<String> validateColumns(QualifiedTableName ref, List<String> columns,
                                        IndexType indexType, ConnectorTableMetadata tableMeta) {
        return validateColumns(ref, columns, indexType, tableMeta, false);
    }

    static List<String> validateExistingColumns(QualifiedTableName ref, List<String> columns,
                                                IndexType indexType, ConnectorTableMetadata tableMeta) {
        return validateColumns(ref, columns, indexType, tableMeta, true);
    }

    private static List<String> validateColumns(QualifiedTableName ref, List<String> columns,
                                                IndexType indexType, ConnectorTableMetadata tableMeta,
                                                boolean alreadyResolved) {
        List<String> resolvedColumns = new ArrayList<>(columns.size());
        for (String col : columns) {
            ConnectorTableMetadata.Column matched;
            try {
                matched = alreadyResolved
                        ? org.pangrova.indexing.api.descriptor.IndexSchemaFingerprint.resolveExactColumn(
                                col, tableMeta.typedColumns())
                        : org.pangrova.indexing.api.descriptor.IndexSchemaFingerprint.resolveColumn(
                                col, tableMeta.typedColumns());
            } catch (IllegalArgumentException failure) {
                throw new PangrovaException(PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR,
                        failure.getMessage() + " in table " + ref, failure);
            }
            if (resolvedColumns.contains(matched.name())) {
                throw new PangrovaException(PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR,
                        "Duplicate indexed column '" + matched.name() + "' in table " + ref);
            }

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
            org.pangrova.connector.api.enums.NativeIndexType nativeType, IndexType indexType) {
        return switch (nativeType) {
            case MIN_MAX -> indexType == IndexType.BTREE || indexType == IndexType.SPARSE;
            case BLOOM_FILTER -> indexType == IndexType.BLOOM_FILTER;
            case BITMAP -> indexType == IndexType.BITMAP;
            case BSI -> indexType == IndexType.BTREE;
        };
    }
}
