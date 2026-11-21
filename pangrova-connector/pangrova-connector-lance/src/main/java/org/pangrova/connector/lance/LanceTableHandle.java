package org.pangrova.connector.lance;

import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import java.util.List;
import java.util.Map;

/** Exact immutable catalog view used by all splits in one SQL query. */
public record LanceTableHandle(String catalog, String tenantId, SchemaTableName table,
        String tableId, String datasetUri, List<LanceColumn> columns, Map<String, String> properties,
        byte[] schemaFence, long version, long revision, long rowCount) implements ConnectorTableHandle {

    /** Format capabilities this adapter can prove; Lance has no row-level delete path here. */
    public static final org.pangrova.connector.api.metadata.table.TableCapabilities CAPABILITIES =
            new org.pangrova.connector.api.metadata.table.TableCapabilities(
                    true, false, false, true, true, false,
                    java.util.Set.of(), java.util.Set.of(),
                    org.pangrova.connector.api.enums.WriterFenceKind
                            .FORMAT_IDENTITY_AND_BASE_VERSION);

    public LanceTableHandle {
        columns = List.copyOf(columns);
        properties = Map.copyOf(properties);
        schemaFence = schemaFence.clone();
        if (columns.isEmpty() || schemaFence.length != 32 || version < 0 || revision <= 0 || rowCount < 0) {
            throw new IllegalArgumentException("Invalid durable Lance table state");
        }
        requireText(catalog);
        requireText(tenantId);
        requireText(tableId);
        requireText(datasetUri);
        java.util.Objects.requireNonNull(table, "table");
        requireText(table.schema());
        requireText(table.table());
        if (columns.stream().map(LanceColumn::name).distinct().count() != columns.size()) {
            throw new IllegalArgumentException("Duplicate Lance columns");
        }
    }

    @Override public List<LanceColumn> columns() { return List.copyOf(columns); }
    @Override public Map<String, String> properties() { return Map.copyOf(properties); }
    @Override public byte[] schemaFence() { return schemaFence.clone(); }

    /** Selects an exact version; metadata and split access resolve its persisted publication evidence. */
    @Override public ConnectorTableHandle withTimeTravelSnapshot(long snapshotId) {
        if (snapshotId <= 0) throw new IllegalArgumentException("Lance snapshot must be positive");
        return new LanceTableHandle(catalog, tenantId, table, tableId, datasetUri, columns, properties,
                schemaFence, snapshotId, revision, rowCount);
    }

    private static void requireText(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Lance table identity must not be blank");
        }
    }

    @Override
    public org.pangrova.connector.api.metadata.table.TableCapabilities capabilities() {
        return CAPABILITIES;
    }

    @Override
    public java.util.Optional<org.pangrova.connector.api.metadata.table.TableIdentity> tableIdentity() {
        return java.util.Optional.of(new org.pangrova.connector.api.metadata.table.TableIdentity(
                "LANCE", tableId, datasetUri, hex(schemaFence)));
    }

    static String hex(byte[] bytes) {
        return java.util.HexFormat.of().formatHex(bytes);
    }
}
