package org.pangrova.connector.paimon;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.OptionalLong;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.connector.paimon.handle.PaimonSystemTableHandle;
import org.pangrova.spi.connector.base.ConnectorException;
import org.pangrova.spi.connector.metadata.table.ConnectorReadBinding;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.ConnectorTableReadMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;

/** Pins system-table source objects after query authorization, without producing metadata rows. */
final class PaimonSystemReadBindings {
    private PaimonSystemReadBindings() {}

    static ConnectorReadBinding bind(Catalog catalog, PaimonSystemTableHandle selector) {
        PaimonSystemTableHandle pinned = selector;
        if (!selector.bound()) {
            FileStoreTable table = load(catalog, selector);
            var manager = table.snapshotManager();
            List<PaimonSystemTableHandle.MetadataObject> objects = new ArrayList<>();
            try {
                if (selector.kind() == PaimonSystemTableHandle.Kind.SNAPSHOTS) {
                    Long selectedLatest = manager.latestSnapshotId();
                    if (selectedLatest != null) {
                        try (var ids = manager.snapshotIdStream()) {
                            for (long id : ids.filter(value -> value <= selectedLatest).sorted().toList()) {
                                var path = manager.snapshotPath(id);
                                long length = manager.fileIO().getFileStatus(path).getLen();
                                objects.add(new PaimonSystemTableHandle.MetadataObject(
                                        id, null, -1, path.toString(), length,
                                        checksum(table, path, length)));
                            }
                        }
                    }
                } else if (selector.kind() == PaimonSystemTableHandle.Kind.TAGS) {
                    for (var listedPath : table.tagManager().tagPaths(ignored -> true).stream()
                            .sorted(java.util.Comparator.comparing(Object::toString)).toList()) {
                        String name = listedPath.getName();
                        if (!name.startsWith("tag-") || name.length() == 4) {
                            throw new IllegalStateException("Invalid Paimon tag metadata object name");
                        }
                        var path = table.tagManager().tagPath(name.substring(4));
                        long length = manager.fileIO().getFileStatus(path).getLen();
                        objects.add(new PaimonSystemTableHandle.MetadataObject(
                                0, name.substring(4), -1, path.toString(), length,
                                checksum(table, path, length)));
                    }
                } else if (selector.kind() == PaimonSystemTableHandle.Kind.SCHEMAS) {
                    long selectedLatest = table.schemaManager().latest()
                            .orElseThrow(() -> new IllegalStateException("Paimon table has no schema"))
                            .id();
                    for (var schema : table.schemaManager().listAll()) {
                        if (schema.id() > selectedLatest) {
                            continue;
                        }
                        var path = table.schemaManager().toSchemaPath(schema.id());
                        long length = manager.fileIO().getFileStatus(path).getLen();
                        objects.add(new PaimonSystemTableHandle.MetadataObject(
                                0, null, schema.id(), path.toString(), length,
                                checksum(table, path, length)));
                    }
                } else {
                    var schema = table.schemaManager().latest()
                            .orElseThrow(() -> new IllegalStateException("Paimon table has no schema"));
                    var path = table.schemaManager().toSchemaPath(schema.id());
                    long length = manager.fileIO().getFileStatus(path).getLen();
                    objects.add(new PaimonSystemTableHandle.MetadataObject(
                            0, null, schema.id(), path.toString(), length,
                            checksum(table, path, length)));
                }
            } catch (IOException failure) {
                throw new ConnectorException(PangrovaErrorCode.CONNECTOR_READ_ERROR,
                    "Failed to pin Paimon system metadata objects", failure);
            }
            pinned = new PaimonSystemTableHandle(selector.baseTable(), selector.kind(),
                    objects, identity(table, objects));
        }
        var columns = pinned.typedColumns();
        var declaration = new ConnectorTableReadMetadata(new ConnectorTableMetadata(
                new SchemaTableName(pinned.baseTable().schemaName(), pinned.tableName()),
                columns.stream().map(ConnectorTableMetadata.Column::name).toList(), columns,
                java.util.Map.of()), ConnectorTableReadMetadata.DeclarationEvidence.EXACT);
        var binding = new ConnectorReadBinding(pinned, declaration,
                new TableStatistics(pinned.objects().size(), -1, -1, java.util.Map.of()),
                ConnectorReadBinding.SnapshotState.METADATA, OptionalLong.empty(),
                pinned.sourceIdentity());
        if (selector.bound()) {
            validate(catalog, binding);
        }
        return binding;
    }

    static void validate(Catalog catalog, ConnectorReadBinding binding) {
        var pinned = (PaimonSystemTableHandle) binding.handle();
        if (!pinned.bound()) {
            throw new IllegalStateException("Paimon system table read is not bound");
        }
        FileStoreTable table = load(catalog, pinned);
        try {
            for (var object : pinned.objects()) {
                var expectedPath = switch (pinned.kind()) {
                    case SNAPSHOTS -> table.snapshotManager().snapshotPath(object.snapshotId());
                    case TAGS -> table.tagManager().tagPath(object.tagName());
                    case OPTIONS, SCHEMAS -> table.schemaManager().toSchemaPath(object.schemaId());
                };
                if (!expectedPath.toString().equals(object.uri())
                        || table.snapshotManager().fileIO().getFileStatus(expectedPath).getLen()
                        != object.length()
                        || !checksum(table, expectedPath, object.length()).equals(object.sha256())) {
                    throw new IllegalStateException("Pinned Paimon metadata object changed");
                }
            }
        } catch (IOException failure) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_SNAPSHOT_NOT_FOUND,
                    "Pinned Paimon metadata object is unavailable", failure);
        }
        if (!binding.sourceIdentity().equals(identity(table, pinned.objects()))) {
            throw new IllegalStateException("Paimon system table source identity changed");
        }
    }

    private static FileStoreTable load(Catalog catalog, PaimonSystemTableHandle handle) {
        var base = handle.baseTable();
        try {
            var table = catalog.getTable(Identifier.create(base.schemaName(), base.tableName()));
            if (!(table instanceof FileStoreTable fileStore)
                    || !fileStore.location().toString().equals(base.tableLocation())) {
                throw new IllegalStateException("Selected Paimon physical table location changed");
            }
            return fileStore;
        } catch (Catalog.TableNotExistException missing) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND,
                    "Selected Paimon base table is unavailable", missing);
        }
    }

    private static String identity(FileStoreTable table,
            List<PaimonSystemTableHandle.MetadataObject> objects) {
        var evidence = new StringBuilder(table.uuid()).append('\0').append(table.location());
        for (var object : objects) {
            evidence.append('\0').append(object.snapshotId()).append('\0')
                    .append(object.schemaId()).append('\0')
                    .append(object.tagName()).append('\0')
                    .append(object.uri()).append('\0').append(object.length())
                    .append('\0').append(object.sha256());
        }
        return DigestUtils.sha256Hex(evidence.toString());
    }

    static String checksum(FileStoreTable table, org.apache.paimon.fs.Path path,
            long expectedLength) throws IOException {
        if (expectedLength <= 0 || expectedLength > 64L * 1024 * 1024) {
            throw new IOException("Paimon snapshot metadata object exceeds Native reader limit");
        }
        try (var input = table.fileIO().newInputStream(path)) {
            var digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[64 * 1024];
            long bytes = 0;
            for (int count; (count = input.read(buffer)) != -1;) {
                if (count == 0) {
                    throw new IOException("Paimon snapshot metadata reader made no progress");
                }
                bytes = Math.addExact(bytes, count);
                if (bytes > expectedLength) {
                    throw new IOException("Paimon snapshot metadata object grew during pinning");
                }
                digest.update(buffer, 0, count);
            }
            if (bytes != expectedLength) {
                throw new IOException("Paimon snapshot metadata object shrank during pinning");
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
