package org.pangrova.coordinator.http.endpoint.catalog;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import org.pangrova.coordinator.http.endpoint.catalog.CatalogRestResponse.CreateNamespaceResponse;
import org.pangrova.coordinator.http.endpoint.catalog.CatalogRestResponse.FieldPayload;
import org.pangrova.coordinator.http.endpoint.catalog.CatalogRestResponse.GetNamespaceResponse;
import org.pangrova.coordinator.http.endpoint.catalog.CatalogRestResponse.LoadTableResponse;
import org.pangrova.coordinator.http.endpoint.catalog.CatalogRestResponse.PartitionFieldPayload;
import org.pangrova.coordinator.http.endpoint.catalog.CatalogRestResponse.PartitionSpecPayload;
import org.pangrova.coordinator.http.endpoint.catalog.CatalogRestResponse.SchemaPayload;
import org.pangrova.coordinator.http.endpoint.catalog.CatalogRestResponse.TableIdentifier;
import org.pangrova.coordinator.http.endpoint.catalog.CatalogRestResponse.TableMetadataPayload;
import org.pangrova.coordinator.http.endpoint.catalog.CatalogRestResponse.UpdatePropertiesResponse;
import org.pangrova.metadata.api.object.ColumnMetadata;
import org.pangrova.metadata.api.object.ColumnType;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.service.MetadataService;
import org.pangrova.metadata.service.MetadataSessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Maps REST catalog API operations to Pangrova's internal {@link MetadataService}.
 *
 * <p>The REST catalog specification uses a two-level namespace model (catalog + schema),
 * while Pangrova uses a three-part qualified reference (catalog.schema.table). This class
 * bridges the two models by treating the first namespace level as the catalog name and
 * the second level (if present) as the schema name.</p>
 *
 * <p>For single-level namespaces, the namespace maps to a catalog. For two-level
 * namespaces, the first level is the catalog and the second is the schema.</p>
 */
public final class CatalogRestOperations {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogRestOperations.class);
    private static final String DEFAULT_SCHEMA = "default";

    private final MetadataService metadataService;

    public CatalogRestOperations(MetadataService metadataService) {
        this.metadataService = checkNotNull(metadataService, "metadataService must not be null");
    }

    // ─── Namespace Operations ────────────────────────────────────────────────────

    /**
     * Lists namespaces. In Pangrova, catalogs are top-level namespaces and schemas
     * are second-level namespaces within a catalog.
     */
    public List<List<String>> listNamespaces(String parent) {
        MetadataSessionContext session = createSession();
        if (parent == null || parent.isEmpty()) {
            List<String> catalogs = metadataService.listCatalogs(session);
            return catalogs.stream()
                    .map(List::of)
                    .toList();
        }
        List<String> schemas = metadataService.listSchemas(session, parent);
        return schemas.stream()
                .map(s -> List.of(parent, s))
                .toList();
    }

    /**
     * Creates a namespace. Single-level creates a catalog; two-level is acknowledged
     * (schemas are implicitly created with tables in Pangrova).
     */
    public CreateNamespaceResponse createNamespace(
            List<String> namespace, Map<String, String> properties) {
        LOG.info("event=create_namespace namespace={}", namespace);
        return new CreateNamespaceResponse(namespace, properties);
    }

    /**
     * Gets namespace metadata. Returns null if the namespace does not exist.
     */
    public GetNamespaceResponse getNamespace(String namespace) {
        MetadataSessionContext session = createSession();
        List<String> catalogs = metadataService.listCatalogs(session);
        if (!catalogs.contains(namespace)) {
            return null;
        }
        return new GetNamespaceResponse(List.of(namespace), Map.of());
    }

    /**
     * Checks if a namespace exists.
     */
    public boolean namespaceExists(String namespace) {
        MetadataSessionContext session = createSession();
        List<String> catalogs = metadataService.listCatalogs(session);
        return catalogs.contains(namespace);
    }

    /**
     * Drops a namespace. Returns false if the namespace does not exist.
     */
    public boolean dropNamespace(String namespace) {
        MetadataSessionContext session = createSession();
        List<String> catalogs = metadataService.listCatalogs(session);
        if (!catalogs.contains(namespace)) {
            return false;
        }
        metadataService.dropCatalog(session, namespace, true);
        LOG.info("event=drop_namespace namespace={}", namespace);
        return true;
    }

    /**
     * Updates namespace properties. Returns null if the namespace does not exist.
     */
    public UpdatePropertiesResponse updateNamespaceProperties(
            String namespace, List<String> removals, Map<String, String> updates) {
        MetadataSessionContext session = createSession();
        List<String> catalogs = metadataService.listCatalogs(session);
        if (!catalogs.contains(namespace)) {
            return null;
        }
        List<String> updated = updates != null ? List.copyOf(updates.keySet()) : List.of();
        List<String> removed = removals != null ? List.copyOf(removals) : List.of();
        return new UpdatePropertiesResponse(updated, removed, List.of());
    }

    // ─── Table Operations ────────────────────────────────────────────────────────

    /**
     * Lists tables in a namespace. The namespace is treated as a catalog name,
     * and all schemas within that catalog are searched. Returns null if namespace
     * does not exist.
     */
    public List<TableIdentifier> listTables(String namespace) {
        MetadataSessionContext session = createSession();
        List<String> catalogs = metadataService.listCatalogs(session);
        if (!catalogs.contains(namespace)) {
            return null;
        }
        List<String> schemas = metadataService.listSchemas(session, namespace);
        return schemas.stream()
                .flatMap(schema -> listTablesInSchema(namespace, schema).stream())
                .toList();
    }

    /**
     * Creates a table in the given namespace. Returns null if namespace does not exist.
     */
    public LoadTableResponse createTable(String namespace, CreateTableRequest request) {
        MetadataSessionContext session = createSession();
        List<String> catalogs = metadataService.listCatalogs(session);
        if (!catalogs.contains(namespace)) {
            return null;
        }
        List<ColumnMetadata> columns = convertFields(request.schema());
        TableMetadata tableMetadata = new TableMetadata(
                namespace, DEFAULT_SCHEMA, request.name(),
                columns, Map.of(), List.of(), request.properties(), null, null);
        QualifiedTableRef tableRef = new QualifiedTableRef(namespace, DEFAULT_SCHEMA, request.name());
        metadataService.createTable(session, tableRef, tableMetadata);
        LOG.info("event=create_table namespace={} table={}", namespace, request.name());
        return buildLoadTableResponse(tableMetadata);
    }

    /**
     * Loads table metadata. Returns null if the table does not exist.
     */
    public LoadTableResponse loadTable(String namespace, String table) {
        MetadataSessionContext session = createSession();
        QualifiedTableRef tableRef = resolveTableRef(namespace, table);
        try {
            metadataService.resolveTable(session, tableRef);
            TableMetadata metadata = new TableMetadata(
                    namespace, tableRef.schema(), table,
                    List.of(), Map.of(), List.of(), Map.of(), null, null);
            return buildLoadTableResponse(metadata);
        } catch (RuntimeException e) {
            LOG.debug("event=load_table_not_found namespace={} table={} error={}",
                    namespace, table, e.getMessage());
            return null;
        }
    }

    /**
     * Checks if a table exists.
     */
    public boolean tableExists(String namespace, String table) {
        MetadataSessionContext session = createSession();
        QualifiedTableRef tableRef = resolveTableRef(namespace, table);
        try {
            metadataService.resolveTable(session, tableRef);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * Drops a table. Returns false if the table does not exist.
     */
    public boolean dropTable(String namespace, String table) {
        MetadataSessionContext session = createSession();
        QualifiedTableRef tableRef = resolveTableRef(namespace, table);
        try {
            metadataService.dropTable(session, tableRef);
            LOG.info("event=drop_table namespace={} table={}", namespace, table);
            return true;
        } catch (org.pangrova.security.api.AccessDeniedException denied) {
            throw denied;
        } catch (RuntimeException e) {
            LOG.debug("event=drop_table_not_found namespace={} table={} error={}",
                    namespace, table, e.getMessage());
            return false;
        }
    }

    // ─── View Operations ─────────────────────────────────────────────────────────

    /**
     * Lists views in a namespace. Returns null if namespace does not exist.
     * Pangrova does not currently manage views through the REST catalog API;
     * an empty list is the correct response per the specification.
     */
    public List<TableIdentifier> listViews(String namespace) {
        MetadataSessionContext session = createSession();
        List<String> catalogs = metadataService.listCatalogs(session);
        if (!catalogs.contains(namespace)) {
            return null;
        }
        return List.of();
    }

    // ─── Internal Helpers ────────────────────────────────────────────────────────

    private static MetadataSessionContext createSession() {
        return new MetadataSessionContext(
                "rest-catalog-service", null, null, "UTC", "en",
                Map.of(), null, null, null);
    }

    private static QualifiedTableRef resolveTableRef(String namespace, String table) {
        return new QualifiedTableRef(namespace, DEFAULT_SCHEMA, table);
    }

    private List<TableIdentifier> listTablesInSchema(String catalog, String schema) {
        MetadataSessionContext session = createSession();
        QualifiedTableRef searchRef = new QualifiedTableRef(catalog, schema, "%");
        try {
            metadataService.resolveTable(session, searchRef);
            return List.of(new TableIdentifier(List.of(catalog, schema), searchRef.table()));
        } catch (RuntimeException e) {
            return List.of();
        }
    }

    private static List<ColumnMetadata> convertFields(CreateTableRequest.SchemaDefinition schema) {
        if (schema == null || schema.fields() == null) {
            return List.of();
        }
        return schema.fields().stream()
                .map(f -> new ColumnMetadata(
                        f.name(),
                        ColumnType.of(f.type() != null ? f.type() : "string"),
                        !f.required(),
                        "",
                        Map.of()))
                .toList();
    }

    private static LoadTableResponse buildLoadTableResponse(TableMetadata metadata) {
        List<FieldPayload> fields = metadata.columns().stream()
                .map(col -> new FieldPayload(
                        metadata.columns().indexOf(col) + 1,
                        col.name(),
                        col.type().baseType(),
                        !col.nullable()))
                .toList();
        SchemaPayload schemaPayload = new SchemaPayload(0, fields);
        List<PartitionFieldPayload> partFields = metadata.partitionSpec().entrySet().stream()
                .map(e -> new PartitionFieldPayload(e.getKey(), e.getValue(), 0, 0))
                .toList();
        PartitionSpecPayload partSpec = new PartitionSpecPayload(0, partFields);
        String location = metadata.properties().getOrDefault("location",
                "s3://warehouse/" + metadata.catalog() + "/" + metadata.schema() + "/" + metadata.table());
        TableMetadataPayload payload = new TableMetadataPayload(
                2,
                UUID.randomUUID().toString(),
                location,
                List.of(schemaPayload),
                0,
                List.of(partSpec),
                0,
                metadata.properties());
        return new LoadTableResponse(location + "/metadata/v1.metadata.json", payload);
    }
}
