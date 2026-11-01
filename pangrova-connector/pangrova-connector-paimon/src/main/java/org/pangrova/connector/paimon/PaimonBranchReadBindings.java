package org.pangrova.connector.paimon;

import org.pangrova.connector.paimon.handle.PaimonTableIdentity;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.OptionalLong;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.FileSystemBranchManager;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.connector.paimon.handle.PaimonBranchTableHandle;
import org.pangrova.spi.connector.base.ConnectorException;
import org.pangrova.spi.connector.metadata.table.ConnectorReadBinding;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.ConnectorTableReadMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;

/** Pins local branch directory identities; row values are read by Worker Native. */
final class PaimonBranchReadBindings {
    private PaimonBranchReadBindings() {}

    static ConnectorReadBinding bind(Catalog catalog, PaimonBranchTableHandle selector) {
        PaimonBranchTableHandle pinned = selector;
        if (!selector.bound()) {
            var table = load(catalog, selector);
            requireLocalBranchManager(table);
            List<PaimonBranchTableHandle.BranchDirectory> directories = new ArrayList<>();
            for (String name : table.branchManager().branches().stream().sorted().toList()) {
                directories.add(directory(table, name));
            }
            pinned = new PaimonBranchTableHandle(selector.baseTable(), directories,
                    identity(table, directories));
        }
        var columns = pinned.typedColumns();
        var declaration = new ConnectorTableReadMetadata(new ConnectorTableMetadata(
                new SchemaTableName(pinned.baseTable().schemaName(), pinned.tableName()),
                columns.stream().map(ConnectorTableMetadata.Column::name).toList(),
                columns, java.util.Map.of()), ConnectorTableReadMetadata.DeclarationEvidence.EXACT);
        var binding = new ConnectorReadBinding(pinned, declaration,
                new TableStatistics(pinned.directories().size(), -1, -1, java.util.Map.of()),
                ConnectorReadBinding.SnapshotState.METADATA, OptionalLong.empty(),
                pinned.sourceIdentity(), java.util.Optional.empty());
        if (selector.bound()) {
            validate(catalog, binding);
        }
        return binding;
    }

    static void validate(Catalog catalog, ConnectorReadBinding binding) {
        var pinned = (PaimonBranchTableHandle) binding.handle();
        if (!pinned.bound()) {
            throw new IllegalStateException("Paimon branches read is not bound");
        }
        var table = load(catalog, pinned);
        requireLocalBranchManager(table);
        for (var expected : pinned.directories()) {
            if (!directory(table, expected.name()).equals(expected)) {
                throw new IllegalStateException("Pinned Paimon branch directory changed");
            }
        }
        if (!identity(table, pinned.directories()).equals(binding.sourceIdentity())) {
            throw new IllegalStateException("Paimon branches source identity changed");
        }
    }

    private static void requireLocalBranchManager(FileStoreTable table) {
        if (!(table.branchManager() instanceof FileSystemBranchManager)) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                    "Paimon $branches requires a filesystem branch manager");
        }
    }

    private static PaimonBranchTableHandle.BranchDirectory directory(
            FileStoreTable table, String name) {
        try {
            var path = new Path(BranchManager.branchPath(table.location(), name));
            URI paimonUri = path.toUri();
            if ((paimonUri.getScheme() != null && !paimonUri.getScheme().equals("file"))
                    || paimonUri.getAuthority() != null) {
                throw new ConnectorException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                        "Paimon $branches directory stat is supported only for local file URIs");
            }
            var local = java.nio.file.Path.of(paimonUri.getPath());
            if (!local.isAbsolute()) {
                throw new ConnectorException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                        "Paimon $branches requires an absolute local directory path");
            }
            String uri = local.toUri().toString();
            if (uri.endsWith("/")) {
                uri = uri.substring(0, uri.length() - 1);
            }
            var status = table.fileIO().getFileStatus(path);
            if (!status.isDir() || !Files.isDirectory(local, LinkOption.NOFOLLOW_LINKS)) {
                throw new IllegalStateException("Paimon branch source is not a real directory");
            }
            var attributes = Files.readAttributes(local, "unix:dev,ino", LinkOption.NOFOLLOW_LINKS);
            long device = ((Number) attributes.get("dev")).longValue();
            long inode = ((Number) attributes.get("ino")).longValue();
            long modificationTime = status.getModificationTime();
            int offsetSeconds = ZoneId.systemDefault().getRules()
                    .getOffset(Instant.ofEpochMilli(modificationTime)).getTotalSeconds();
            return new PaimonBranchTableHandle.BranchDirectory(
                    name, uri, modificationTime, device, inode, offsetSeconds);
        } catch (IOException failure) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_SNAPSHOT_NOT_FOUND,
                    "Pinned Paimon branch directory is unavailable", failure);
        }
    }

    private static FileStoreTable load(Catalog catalog, PaimonBranchTableHandle handle) {
        var base = handle.baseTable();
        try {
            var table = PaimonTableIdentity.loadFresh(catalog, Identifier.create(base.schemaName(), base.tableName()));
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
            List<PaimonBranchTableHandle.BranchDirectory> directories) {
        var evidence = new StringBuilder(table.uuid()).append('\0').append(table.location());
        for (var directory : directories.stream()
                .sorted(Comparator.comparing(PaimonBranchTableHandle.BranchDirectory::name)).toList()) {
            evidence.append('\0').append(directory.name()).append('\0')
                    .append(directory.uri()).append('\0')
                    .append(directory.modificationTimeMillis()).append('\0')
                    .append(directory.device()).append('\0').append(directory.inode())
                    .append('\0').append(directory.zoneOffsetSeconds());
        }
        return DigestUtils.sha256Hex(evidence.toString());
    }
}
