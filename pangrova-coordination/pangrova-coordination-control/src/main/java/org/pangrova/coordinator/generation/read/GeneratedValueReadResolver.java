package org.pangrova.coordinator.generation.read;
import org.pangrova.planner.substrait.NativeDatasetSchemaCompiler;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.persistence.generation.api.generated.GeneratedValueBuildMetadataStore;
import org.pangrova.persistence.generation.api.job.GenerationJobStore;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.generation.codec.column.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.generation.GeneratedValuesOverlayPlanBinder;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.commit.WriteDeclarationSnapshot;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Binds protected query reads to generated values of their exact SDK declaration and source snapshot. */
public final class GeneratedValueReadResolver {
    private final CatalogRegistry catalogs;
    private final GenerationJobStore builds;
    private final GeneratedValueGenerationBindingResolver artifacts;

    /** Uses the same immutable publication store and task-local generic artifact reader as other capabilities. */
    public GeneratedValueReadResolver(CatalogRegistry catalogs, GenerationJobStore builds,
                                       GeneratedValueBuildMetadataStore metadata) {
        this.catalogs = Objects.requireNonNull(catalogs, "catalogs");
        this.builds = Objects.requireNonNull(builds, "builds");
        this.artifacts = new GeneratedValueGenerationBindingResolver(metadata, builds);
    }

    /** Resolves unsigned read-local templates; Native consumes objects only after exact task partitioning. */
    public BoundExecutionPlan bindReadOverlays(byte[] plan, QuerySession actor,
                                               List<String> stageSplits, Map<String, Long> snapshotOverrides) {
        Objects.requireNonNull(snapshotOverrides, "snapshotOverrides");
        var splits = org.pangrova.generation.codec.source.GenerationSourceSplitCodec.decode(stageSplits);
        var bySource = splits.stream().collect(Collectors.groupingBy(DataSplit::getSourceId));
        var overlays = new ArrayList<GeneratedValuesOverlayPlanBinder.ReadOverlay>();
        var reads = GeneratedValuesOverlayPlanBinder.reads(plan);
        for (int ordinal = 0; ordinal < reads.size(); ordinal++) {
            var read = reads.get(ordinal);
            if (!read.hasNamedTable() || read.getNamedTable().getNamesCount() != 3) continue;
            var table = read.getNamedTable().getNamesList();
            var sourceSplits = bySource.get(String.join(".", table));
            if (sourceSplits == null || sourceSplits.isEmpty()) continue;
            bindRead(actor, read, ordinal, sourceSplits, snapshotOverrides, overlays);
        }
        return GeneratedValuesOverlayPlanBinder.bind(plan, overlays);
    }

    private void bindRead(QuerySession actor, io.substrait.proto.ReadRel read, int ordinal,
                          List<DataSplit> splits, Map<String, Long> snapshotOverrides,
                          List<GeneratedValuesOverlayPlanBinder.ReadOverlay> overlays) {
        var names = read.getNamedTable().getNamesList();
        var table = new QualifiedTableName(names.get(0), names.get(1), names.get(2));
        String snapshot = singleSnapshot(splits);
        var session = SessionContextConverter.toConnectorSession(actor, table.catalog(), table.schema());
        var metadata = catalogs.getCatalog(table.catalog()).getMetadata(session);
        var handle = metadata.getTableHandle(session, new SchemaTableName(table.schema(), table.table()));
        if (handle == null) throw new IllegalStateException("Generated query read table disappeared");
        var declared = declarationMetadata(metadata, handle, session, snapshot,
                snapshotOverrides.get(table.toString()));
        var definitions = GeneratedColumnMetadataCodec.decode(declared.properties());
        requireSchema(read, declared);
        if (definitions.getDefinitionsCount() == 0) return;
        parseSnapshot(snapshot);
        var declaration = WriteDeclarationSnapshot.capture(table, declared, definitions.toByteArray(), true);
        List<String> serialized = splits.stream()
                .map(split -> Base64.getEncoder().encodeToString(split.toByteArray())).toList();
        for (var definition : definitions.getDefinitionsList()) {
            if (!read.hasFilter() && !isOutput(read, definition.getTargetColumn())) continue;
            String node = actor.queryId() + ":generated-read:" + ordinal + ":" + definition.getTargetColumn();
            var template = artifacts.resolveTemplate(actor.tenantId(), node, table.toString(),
                    definition.getTargetColumn(), snapshot, serialized);
            template.ifPresent(binding -> {
                requireDeclaration(actor.tenantId(), binding, declaration);
                overlays.add(new GeneratedValuesOverlayPlanBinder.ReadOverlay(
                        ordinal, names, definition.getTargetColumn(), binding));
            });
        }
    }

    private static org.pangrova.connector.api.metadata.table.ConnectorTableMetadata declarationMetadata(
            org.pangrova.connector.api.metadata.ConnectorMetadata metadata,
            org.pangrova.connector.api.metadata.table.ConnectorTableHandle handle,
            org.pangrova.connector.api.session.ConnectorSession session, String snapshot, Long historicalVersion) {
        if (historicalVersion == null) return metadata.getTableMetadata(session, handle);
        if (parseSnapshot(snapshot) != historicalVersion) {
            throw new IllegalStateException("Historical generated read split differs from its declared snapshot");
        }
        var pinned = handle.withTimeTravelSnapshot(historicalVersion);
        if (pinned == handle) {
            throw new IllegalStateException("Generated query requires an exact historical SDK handle");
        }
        return metadata.getTableReadMetadata(session, pinned).requireExactDeclaration();
    }

    private void requireDeclaration(String tenant, OperatorBinding binding, WriteDeclarationSnapshot declaration) {
        var artifact = binding.getArtifacts(0);
        var manifest = builds.findPublishedGeneration(tenant, artifact.getArtifactId(), artifact.getGeneration(),
                artifact.getSourceSnapshot()).orElseThrow();
        var spec = builds.findDispatchSpec(tenant, manifest.jobId()).orElseThrow();
        var expected = spec.authorizationScope().definitionSnapshot().orElseThrow(() ->
                new IllegalStateException("Generated query publication has no declared generated definition"));
        if (!expected.declaration().equals(declaration)) {
            throw new IllegalStateException("Generated query definition differs from the exact published declaration");
        }
    }

    private static void requireSchema(io.substrait.proto.ReadRel read,
                                      org.pangrova.connector.api.metadata.table.ConnectorTableMetadata table) {
        var actual = NativeDatasetSchemaCompiler.compile(table.typedColumns()).schema();
        if (!read.hasBaseSchema() || !(read.getBaseSchema().equals(actual)
                || hasExactRetrievalIdentity(read.getBaseSchema(), actual))) {
            throw new IllegalStateException("Generated query compiled scan differs from its declared SDK schema");
        }
    }

    private static boolean hasExactRetrievalIdentity(io.substrait.proto.NamedStruct read,
            io.substrait.proto.NamedStruct declared) {
        var fields = org.pangrova.protocol.retrieval.RetrievalCandidateIdentity.PHYSICAL_FIELDS;
        int columns = declared.getStruct().getTypesCount();
        int names = declared.getNamesCount();
        if (read.getStruct().getTypesCount() != columns + fields.size()
                || read.getNamesCount() != names + fields.size()
                || !read.getNamesList().subList(0, names).equals(declared.getNamesList())
                || !read.getNamesList().subList(names, read.getNamesCount()).equals(fields)
                || !read.getStruct().toBuilder().clearTypes().build()
                        .equals(declared.getStruct().toBuilder().clearTypes().build())) {
            return false;
        }
        for (int index = 0; index < columns; index++) {
            if (!read.getStruct().getTypes(index).equals(declared.getStruct().getTypes(index))) return false;
        }
        var required = io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED;
        for (int index = 0; index < fields.size(); index++) {
            var type = read.getStruct().getTypes(columns + index);
            if (index == fields.size() - 1
                    ? !type.equals(io.substrait.proto.Type.newBuilder()
                            .setI64(io.substrait.proto.Type.I64.newBuilder().setNullability(required)).build())
                    : !type.equals(io.substrait.proto.Type.newBuilder()
                            .setString(io.substrait.proto.Type.String.newBuilder().setNullability(required)).build())) {
                return false;
            }
        }
        return true;
    }

    private static boolean isOutput(io.substrait.proto.ReadRel read, String target) {
        int field = read.getBaseSchema().getNamesList().indexOf(target);
        return field >= 0 && (!read.getCommon().hasEmit()
                || read.getCommon().getEmit().getOutputMappingList().contains(field));
    }

    private static long parseSnapshot(String value) {
        try {
            long version = Long.parseLong(value);
            if (version <= 0 || !Long.toString(version).equals(value)) throw new NumberFormatException();
            return version;
        } catch (NumberFormatException invalid) {
            throw new IllegalStateException("Generated query requires a canonical positive snapshot", invalid);
        }
    }

    private static String singleSnapshot(List<DataSplit> splits) {
        var snapshots = splits.stream().map(DataSplit::getSourceSnapshot).distinct().toList();
        if (snapshots.size() != 1) {
            throw new IllegalStateException("Generated query read spans distinct source snapshots");
        }
        return snapshots.get(0);
    }
}
