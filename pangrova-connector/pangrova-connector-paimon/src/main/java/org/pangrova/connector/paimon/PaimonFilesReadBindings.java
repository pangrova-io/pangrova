package org.pangrova.connector.paimon;

import org.pangrova.connector.paimon.handle.PaimonTableIdentity;

import com.google.protobuf.ByteString;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.connector.paimon.handle.PaimonFilesTableHandle;
import org.pangrova.protocol.v2.PaimonFilesReadContext;
import org.pangrova.protocol.v2.PinnedMetadataObject;
import org.pangrova.connector.spi.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.spi.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.spi.metadata.table.ConnectorTableReadMetadata;
import org.pangrova.connector.spi.metadata.table.SchemaTableName;
import org.pangrova.connector.spi.metadata.table.TableStatistics;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.OptionalLong;
import java.util.TreeSet;

/** SDK metadata planning only; all twenty result values are read and projected by Native. */
final class PaimonFilesReadBindings {
    private PaimonFilesReadBindings() {}

    static ConnectorReadBinding bind(Catalog catalog, PaimonFilesTableHandle selector) {
        if (selector.bound()) {
            var binding = declaration(selector);
            validate(catalog, binding);
            return binding;
        }
        FileStoreTable table = load(catalog, selector);
        try {
            var snapshot = selector.baseTable().snapshotId().map(table::snapshot)
                    .orElseGet(() -> table.latestSnapshot().orElse(null));
            if (snapshot == null) {
                return declaration(new PaimonFilesTableHandle(selector.baseTable(), List.of(),
                        identity(table, List.of(), PinnedMetadataObject.getDefaultInstance()), table.uuid(), 0,
                        PinnedMetadataObject.getDefaultInstance()));
            }
            var sources = new ArrayList<ManifestFileMeta>();
            if (snapshot.baseManifestList() != null) {
                sources.addAll(table.manifestListReader().read(snapshot.baseManifestList()));
            }
            sources.addAll(table.manifestListReader().read(snapshot.deltaManifestList()));
            var active = FileEntry.mergeEntries(entries(table, sources));
            return declaration(freeze(table, selector, snapshot.id(), sources, active));
        } catch (IOException failure) {
            throw new UncheckedIOException("Cannot pin Paimon file metadata", failure);
        }
    }

    private static PaimonFilesTableHandle freeze(FileStoreTable table, PaimonFilesTableHandle selector,
            long snapshotId, List<ManifestFileMeta> sources,
            java.util.Collection<PaimonManifestSourceEntry> active) throws IOException {
        var snapshot = pin(table, table.snapshotManager().snapshotPath(snapshotId));
        var schemaIds = new TreeSet<Long>();
        schemaIds.add(table.schema().id());
        Map<String, List<PaimonManifestSourceEntry>> groups = new LinkedHashMap<>();
        for (var source : active) {
            if (source.kind() != FileKind.ADD) continue;
            schemaIds.add(source.entry().file().schemaId());
            groups.computeIfAbsent(source.manifestName(), ignored -> new ArrayList<>()).add(source);
        }
        var schemas = schemaIds.stream().map(id -> PaimonFilesReadContext.Schema.newBuilder()
                .setSchemaId(id).setSchemaJson(schemaDeclaration(table.schemaManager().schema(id))).build()).toList();
        var manifests = new ArrayList<PaimonFilesTableHandle.Manifest>();
        for (var source : sources) {
            var selected = groups.remove(source.fileName());
            if (selected == null) continue;
            var context = PaimonFilesReadContext.newBuilder().setSnapshot(snapshot)
                    .setReadSchemaId(table.schema().id()).addAllSchemas(schemas);
            for (var file : selected) {
                String uri = file.entry().file().externalPath().orElseGet(() ->
                        table.store().pathFactory().bucketPath(file.partition(), file.bucket())
                                + "/" + file.fileName());
                context.addFiles(PaimonFilesReadContext.FileSelection.newBuilder()
                        .setIdentifierSha256(ByteString.copyFrom(PaimonFileIdentifier.digest(file)))
                        .setFileUri(uri));
            }
            var object = pin(table, table.store().pathFactory().toManifestFilePath(source.fileName()));
            manifests.add(new PaimonFilesTableHandle.Manifest(object.getUri(), object.getLength(),
                    HexFormat.of().formatHex(object.getSha256().toByteArray()), snapshotId, context.build()));
        }
        return new PaimonFilesTableHandle(selector.baseTable(), manifests, identity(table, manifests, snapshot),
                table.uuid(), snapshotId, snapshot);
    }

    private static String schemaDeclaration(TableSchema schema) {
        // Physical declarations are control metadata; catalog/table options are not task payload.
        var declaration = new LinkedHashMap<String, Object>();
        declaration.put("id", schema.id());
        declaration.put("fields", schema.fields().stream()
                .map(field -> new DataField(field.id(), field.name(), field.type())).toList());
        declaration.put("partitionKeys", schema.partitionKeys());
        declaration.put("primaryKeys", schema.primaryKeys());
        return JsonSerdeUtil.toJson(declaration);
    }

    private static Iterable<PaimonManifestSourceEntry> entries(FileStoreTable table, List<ManifestFileMeta> sources) {
        return () -> new Iterator<>() {
            private final Iterator<ManifestFileMeta> manifests = sources.iterator();
            private Iterator<ManifestEntry> current = List.<ManifestEntry>of().iterator();
            private String name;
            @Override public boolean hasNext() {
                while (!current.hasNext() && manifests.hasNext()) {
                    name = manifests.next().fileName();
                    if (name.contains("/") || name.contains("\\") || name.equals("..")) {
                        throw new IllegalArgumentException("Invalid Paimon manifest object name");
                    }
                    current = table.manifestFileReader().read(name).iterator();
                }
                return current.hasNext();
            }
            @Override public PaimonManifestSourceEntry next() {
                if (!hasNext()) throw new NoSuchElementException();
                return new PaimonManifestSourceEntry(current.next(), name);
            }
        };
    }

    static void validate(Catalog catalog, ConnectorReadBinding binding) {
        var handle = (PaimonFilesTableHandle) binding.handle();
        var table = load(catalog, handle);
        if (!table.uuid().equals(handle.tableUuid())) {
            throw new IllegalStateException("Paimon file source incarnation changed");
        }
        try {
            if (handle.snapshotId() > 0 && !pin(table, table.snapshotManager().snapshotPath(handle.snapshotId()))
                    .equals(handle.snapshot())) {
                throw new IllegalStateException("Pinned Paimon snapshot changed");
            }
            for (var manifest : handle.manifests()) {
                if (!pin(table, new Path(manifest.uri())).getSha256()
                        .equals(ByteString.copyFrom(HexFormat.of().parseHex(manifest.sha256())))) {
                    throw new IllegalStateException("Pinned Paimon manifest changed");
                }
                var snapshot = manifest.context().getSnapshot();
                if (!pin(table, table.snapshotManager().snapshotPath(manifest.snapshotId())).equals(snapshot)) {
                    throw new IllegalStateException("Pinned Paimon snapshot changed");
                }
            }
        } catch (IOException failure) {
            throw new UncheckedIOException("Pinned Paimon file source is unavailable", failure);
        }
        if (!identity(table, handle.manifests(), handle.snapshot()).equals(binding.sourceIdentity())) {
            throw new IllegalStateException("Paimon file source incarnation changed");
        }
    }

    private static PinnedMetadataObject pin(FileStoreTable table, Path path) throws IOException {
        long length = table.fileIO().getFileStatus(path).getLen();
        return PinnedMetadataObject.newBuilder().setUri(path.toString()).setLength(length)
                .setSha256(ByteString.copyFrom(HexFormat.of().parseHex(
                        PaimonSystemReadBindings.checksum(table, path, length)))).build();
    }

    private static FileStoreTable load(Catalog catalog, PaimonFilesTableHandle handle) {
        try {
            var identifier = Identifier.create(handle.baseTable().schemaName(), handle.baseTable().tableName());
            catalog.invalidateTable(identifier);
            var table = PaimonTableIdentity.loadFresh(catalog, identifier);
            if (!(table instanceof FileStoreTable source)
                    || !source.location().toString().equals(handle.baseTable().tableLocation())) {
                throw new IllegalStateException("Paimon file source location changed");
            }
            return source;
        } catch (Catalog.TableNotExistException missing) {
            throw new IllegalStateException("Paimon file source table is unavailable", missing);
        }
    }

    private static String identity(FileStoreTable table, List<PaimonFilesTableHandle.Manifest> manifests,
            PinnedMetadataObject snapshot) {
        var evidence = new StringBuilder("pangrova-paimon-files-v1\0").append(table.uuid()).append('\0')
                .append(table.location()).append('\0').append(HexFormat.of().formatHex(snapshot.toByteArray()));
        for (var source : manifests) {
            evidence.append('\0').append(source.uri()).append('\0').append(source.sha256()).append('\0')
                    .append(HexFormat.of().formatHex(source.context().toByteArray()));
        }
        return DigestUtils.sha256Hex(evidence.toString());
    }

    private static ConnectorReadBinding declaration(PaimonFilesTableHandle handle) {
        var columns = handle.typedColumns();
        var schema = new ConnectorTableReadMetadata(new ConnectorTableMetadata(
                new SchemaTableName(handle.baseTable().schemaName(), handle.tableName()),
                columns.stream().map(ConnectorTableMetadata.Column::name).toList(), columns, Map.of()),
                ConnectorTableReadMetadata.DeclarationEvidence.EXACT);
        long count = handle.manifests().stream().mapToLong(source -> source.context().getFilesCount()).sum();
        return new ConnectorReadBinding(handle, schema, new TableStatistics(count, -1, -1, Map.of()),
                handle.snapshotId() == 0 ? ConnectorReadBinding.SnapshotState.EMPTY
                        : ConnectorReadBinding.SnapshotState.SNAPSHOT,
                handle.snapshotId() == 0 ? OptionalLong.empty() : OptionalLong.of(handle.snapshotId()),
                handle.sourceIdentity(), java.util.Optional.empty());
    }
}
