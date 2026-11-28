package org.pangrova.coordinator.generation.read;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.ReadRel;
import org.pangrova.planner.substrait.NativeDatasetSchemaCompiler;
import org.pangrova.coordinator.generation.read.GeneratedValueGenerationBindingResolver.ResolvedRead;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.persistence.generation.api.generatedvalue.GeneratedValueBuildMetadataStore;
import org.pangrova.persistence.generation.api.job.GenerationJobStore;
import org.pangrova.planner.analysis.SplitSourceDescriptor;
import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.generation.codec.column.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.generation.GeneratedValuesOverlayPlanBinder;
import org.pangrova.protocol.generation.GeneratedValuesOverlayPlanBinder.ReadSite;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
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
    public BoundExecutionPlan bindReadOverlays(Plan plan, QuerySession actor,
            List<DataSplit> stageSplits, List<SplitSourceDescriptor> sources) {
        if (stageSplits.isEmpty()) {
            return new BoundExecutionPlan(plan.toByteArray(), List.of());
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
        Map<QualifiedTableName, ReadSource> admittedSources = new HashMap<>();
        Map<PublicationRead, Optional<ResolvedRead>> publications = new HashMap<>();
        for (int ordinal = 0; ordinal < reads.size(); ordinal++) {
            var site = reads.get(ordinal);
            var read = site.read();
            if (!read.hasNamedTable() || read.getNamedTable().getNamesCount() != 3) continue;
            var table = QualifiedTableName.resolve(read.getNamedTable().getNamesList());
            var sourceSplits = bySource.get(table.toString());
            if (sourceSplits == null || sourceSplits.isEmpty()) continue;
            var source = admittedSources.computeIfAbsent(table,
                    key -> readSource(key, sourceSplits, bindings.get(key)));
            requireSchema(site.read(), source.schema);
            bindRead(actor, site, ordinal, source, publications, overlays);
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

    private static ReadSource readSource(QualifiedTableName table, List<DataSplit> splits,
            ConnectorReadBinding binding) {
        if (binding == null) {
            throw new IllegalStateException("Compiled read has no admitted connector binding for " + table);
        }
        String snapshot = singleSnapshot(splits);
        if (binding.snapshotId().isPresent()
                && !Long.toString(binding.snapshotId().orElseThrow()).equals(snapshot)) {
            throw new IllegalStateException("Generated query split differs from the admitted snapshot");
        }
        var declaration = binding.declaration().requireExactDeclaration();
        var definitions = GeneratedColumnMetadataCodec.decode(declaration.properties());
        return new ReadSource(table, splits, snapshot, declaration, definitions);
    }

    private void bindRead(QuerySession actor, ReadSite site, int ordinal, ReadSource source,
                          Map<PublicationRead, Optional<ResolvedRead>> publications,
                          List<GeneratedValuesOverlayPlanBinder.ReadOverlay> overlays) {
        if (source.definitions.getDefinitionsCount() == 0) return;
        source.captureGeneratedDeclaration();
        for (var definition : source.definitions.getDefinitionsList()) {
            if (!site.usesColumn(definition.getTargetColumn())) continue;
            String node = actor.queryId() + ":generated-read:" + ordinal + ":" + definition.getTargetColumn();
            var publication = new PublicationRead(source.table, definition.getTargetColumn(), source.snapshot);
            var template = publications.computeIfAbsent(publication, key -> {
                var resolved = artifacts.resolveTemplate(actor.tenantId(), key.table().toString(),
                        key.column(), key.snapshot(), source.splits);
                resolved.ifPresent(source::requirePublishedDeclaration);
                return resolved;
            });
            template.ifPresent(resolved -> overlays.add(new GeneratedValuesOverlayPlanBinder.ReadOverlay(
                    ordinal, site.read().getNamedTable().getNamesList(),
                    definition.getTargetColumn(), resolved.bind(node))));
        }
    }

    private record PublicationRead(QualifiedTableName table, String column, String snapshot) {}

    private static final class ReadSource {
        private final QualifiedTableName table;
        private final List<DataSplit> splits;
        private final String snapshot;
        private final ConnectorTableMetadata declaration;
        private final GeneratedColumnSet definitions;
        private final NamedStruct schema;
        private WriteDeclarationSnapshot digest;

        private ReadSource(QualifiedTableName table, List<DataSplit> splits, String snapshot,
                ConnectorTableMetadata declaration, GeneratedColumnSet definitions) {
            this.table = table;
            this.splits = splits;
            this.snapshot = snapshot;
            this.declaration = declaration;
            this.definitions = definitions;
            this.schema = NativeDatasetSchemaCompiler.compile(declaration.typedColumns()).schema();
        }

        private void captureGeneratedDeclaration() {
            if (digest == null) {
                parseSnapshot(snapshot);
                digest = WriteDeclarationSnapshot.capture(table, declaration, definitions.toByteArray(), true);
            }
        }

        private void requirePublishedDeclaration(ResolvedRead resolved) {
            var published = resolved.definitionSnapshot().orElseThrow(() ->
                    new IllegalStateException("Generated query publication has no declared generated definition"));
            if (!published.declaration().equals(digest)) {
                throw new IllegalStateException(
                        "Generated query definition differs from the exact published declaration");
            }
        }
    }

    private static void requireSchema(ReadRel read, NamedStruct actual) {
        if (!read.hasBaseSchema() || !(read.getBaseSchema().equals(actual)
                || hasExactRetrievalIdentity(read.getBaseSchema(), actual))) {
            throw new IllegalStateException("Generated query compiled scan differs from its declared SDK schema");
        }
    }

    private static boolean hasExactRetrievalIdentity(NamedStruct read, NamedStruct declared) {
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
