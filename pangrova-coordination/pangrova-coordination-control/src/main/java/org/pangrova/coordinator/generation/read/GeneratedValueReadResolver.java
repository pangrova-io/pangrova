package org.pangrova.coordinator.generation.read;
import io.substrait.proto.ReadRel;
import org.pangrova.planner.substrait.NativeDatasetSchemaCompiler;
import org.pangrova.coordinator.generation.read.GeneratedValueGenerationBindingResolver.ResolvedRead;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.persistence.generation.api.generated.GeneratedValueBuildMetadataStore;
import org.pangrova.persistence.generation.api.job.GenerationJobStore;
import org.pangrova.planner.analysis.SplitSourceDescriptor;
import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.generation.codec.column.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.generation.GeneratedValuesOverlayPlanBinder;
import org.pangrova.protocol.generation.GeneratedValuesOverlayPlanBinder.ReadSite;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.commit.WriteDeclarationSnapshot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Binds protected query reads to generated values of their exact SDK declaration and source snapshot. */
public final class GeneratedValueReadResolver {
    private final GeneratedValueGenerationBindingResolver artifacts;

    /** Uses the same immutable publication store and task-local generic artifact reader as other capabilities. */
    public GeneratedValueReadResolver(GenerationJobStore builds,
                                       GeneratedValueBuildMetadataStore metadata) {
        this.artifacts = new GeneratedValueGenerationBindingResolver(metadata, builds);
    }

    /** Resolves unsigned read-local templates; Native consumes objects only after exact task partitioning. */
    public BoundExecutionPlan bindReadOverlays(byte[] plan, QuerySession actor,
            List<DataSplit> stageSplits, List<SplitSourceDescriptor> sources) {
        if (stageSplits.isEmpty()) {
            return new BoundExecutionPlan(plan, List.of());
        }
        stageSplits.forEach(org.pangrova.generation.codec.source.GenerationSourceSplitValidator::validateSplit);
        var bySource = stageSplits.stream().collect(Collectors.groupingBy(DataSplit::getSourceId));
        var bindings = selectedBindings(sources);
        return GeneratedValuesOverlayPlanBinder.resolve(plan,
                reads -> selectOverlays(actor, reads, bySource, bindings));
    }

    private List<GeneratedValuesOverlayPlanBinder.ReadOverlay> selectOverlays(QuerySession actor,
            List<ReadSite> reads, Map<String, List<DataSplit>> bySource,
            Map<QualifiedTableName, ConnectorReadBinding> bindings) {
        var overlays = new ArrayList<GeneratedValuesOverlayPlanBinder.ReadOverlay>();
        Map<PublicationRead, Optional<ResolvedRead>> publications = new HashMap<>();
        for (int ordinal = 0; ordinal < reads.size(); ordinal++) {
            var site = reads.get(ordinal);
            var read = site.read();
            if (!read.hasNamedTable() || read.getNamedTable().getNamesCount() != 3) continue;
            var table = QualifiedTableName.resolve(read.getNamedTable().getNamesList());
            var sourceSplits = bySource.get(table.toString());
            if (sourceSplits == null || sourceSplits.isEmpty()) continue;
            var binding = bindings.get(table);
            if (binding == null) {
                throw new IllegalStateException("Compiled read has no admitted connector binding for " + table);
            }
            bindRead(actor, site, ordinal, table, sourceSplits, binding, publications, overlays);
        }
        return overlays;
    }

    private static Map<QualifiedTableName, ConnectorReadBinding> selectedBindings(
            List<SplitSourceDescriptor> sources) {
        var bindings = new HashMap<QualifiedTableName, ConnectorReadBinding>();
        for (var source : sources) {
            source.readBinding().ifPresent(binding -> {
                var table = new QualifiedTableName(source.catalogName(), source.schemaName(), source.tableName());
                var previous = bindings.putIfAbsent(table, binding);
                if (previous != null && !previous.equals(binding)) {
                    throw new IllegalStateException("Conflicting admitted read bindings for " + table);
                }
            });
        }
        return bindings;
    }

    private void bindRead(QuerySession actor, ReadSite site, int ordinal,
                          QualifiedTableName table, List<DataSplit> splits, ConnectorReadBinding binding,
                          Map<PublicationRead, Optional<ResolvedRead>> publications,
                          List<GeneratedValuesOverlayPlanBinder.ReadOverlay> overlays) {
        String snapshot = singleSnapshot(splits);
        if (binding.snapshotId().isPresent()
                && !Long.toString(binding.snapshotId().orElseThrow()).equals(snapshot)) {
            throw new IllegalStateException("Generated query split differs from the admitted snapshot");
        }
        var declared = binding.declaration().requireExactDeclaration();
        var definitions = GeneratedColumnMetadataCodec.decode(declared.properties());
        requireSchema(site.read(), declared);
        if (definitions.getDefinitionsCount() == 0) return;
        parseSnapshot(snapshot);
        var declaration = WriteDeclarationSnapshot.capture(table, declared, definitions.toByteArray(), true);
        for (var definition : definitions.getDefinitionsList()) {
            if (!site.usesColumn(definition.getTargetColumn())) continue;
            String node = actor.queryId() + ":generated-read:" + ordinal + ":" + definition.getTargetColumn();
            var publication = new PublicationRead(table, definition.getTargetColumn(), snapshot);
            var template = publications.computeIfAbsent(publication, key -> artifacts.resolveTemplate(
                    actor.tenantId(), key.table().toString(), key.column(), key.snapshot(), splits));
            template.ifPresent(resolved -> {
                var definitionSnapshot = resolved.definitionSnapshot().orElseThrow(() ->
                        new IllegalStateException("Generated query publication has no declared generated definition"));
                if (!definitionSnapshot.declaration().equals(declaration)) {
                    throw new IllegalStateException(
                            "Generated query definition differs from the exact published declaration");
                }
                overlays.add(new GeneratedValuesOverlayPlanBinder.ReadOverlay(
                        ordinal, site.read().getNamedTable().getNamesList(),
                        definition.getTargetColumn(), resolved.bind(node)));
            });
        }
    }

    private record PublicationRead(QualifiedTableName table, String column, String snapshot) {}

    private static void requireSchema(ReadRel read,
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
