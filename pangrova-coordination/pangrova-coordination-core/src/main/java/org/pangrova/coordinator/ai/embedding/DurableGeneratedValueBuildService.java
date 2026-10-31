package org.pangrova.coordinator.ai.embedding;

import org.pangrova.databuild.assembly.DataBuildStageGraphFactory;

import org.pangrova.databuild.assembly.DataBuildDispatchSpecFactory;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.databuild.SnapshotDataBuildSplitPlanner;
import org.pangrova.coordinator.databuild.template.FrozenStageBindings;
import org.pangrova.coordinator.query.execution.dispatch.dag.binding.AiInvocationOperatorBinder;
import org.pangrova.coordinator.query.execution.dispatch.dag.WorkerSelector;
import org.pangrova.scheduler.core.placement.WorkerCapabilityMatcher;
import org.pangrova.databuild.api.build.DataBuildDispatchSpec;
import org.pangrova.databuild.api.build.DataBuildJob;
import org.pangrova.databuild.api.build.DataBuildPage;
import org.pangrova.databuild.api.DataBuildPageKind;
import org.pangrova.databuild.api.build.DataBuildSplit;
import org.pangrova.databuild.api.build.DataBuildStore;
import org.pangrova.metadata.databuild.GeneratedValueBuildDescriptor;
import org.pangrova.metadata.databuild.GeneratedValueBuildSubmissionStore;
import org.pangrova.metadata.databuild.GeneratedValueBuildSubmission;
import org.pangrova.connector.spi.coordination.WriteCommitEntry;
import org.pangrova.security.protocol.DeferredAuthorizationScopeCodec;
import org.pangrova.planner.databuild.DataBuildPlan;
import org.pangrova.planner.databuild.GeneratedValueDataBuildPlan;
import org.pangrova.planner.databuild.GeneratedValueDataBuildPlanBuilder;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.protocol.metadata.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.GeneratedColumnDefinition;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.NamedValue;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorBindingSet;
import org.pangrova.protocol.v2.TypedValue;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.security.api.UserIdentity;
import org.pangrova.security.api.authz.DeferredAuthorizationScope;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Compiles generated-column definitions into durable, Native Worker work.
 *
 * <p>The Coordinator resolves identities, persists leases and publishes
 * immutable metadata only.  Source scanning, provider batching and sidecar
 * serialization remain in the Native Worker data plane.</p>
 */
public final class DurableGeneratedValueBuildService {

    private static final int DEFAULT_MAX_ATTEMPTS = 5;
    private static final long DEFAULT_MEMORY_BYTES = 1024L * 1024 * 1024;
    private static final long DEFAULT_SPILL_BYTES = 1024L * 1024 * 1024;
    private static final long DEFAULT_TIMEOUT_MILLIS = Duration.ofMinutes(2).toMillis();

    private final DataBuildStore store;
    private final GeneratedValueBuildSubmissionStore submissions;
    private final SnapshotDataBuildSplitPlanner splitPlanner;
    private final WorkerSelector workerSelector;
    private final String baseArtifactUri;
    private final org.pangrova.coordinator.databuild.ProtectedSourceSchemaVerifier sourceSchemaVerifier;
    private volatile GeneratedSourceCompiler sourceCompiler;

    /** Binds the process-owned SQL compiler after QueryDispatcher initialization. */
    public synchronized void bindSourceCompiler(GeneratedSourceCompiler compiler) {
        if (sourceCompiler != null) {
            throw new IllegalStateException("Generated source compiler is already bound");
        }
        sourceCompiler = Objects.requireNonNull(compiler, "compiler");
    }

    GeneratedSourceReads.ObservedRead compileSource(QualifiedTableName table, List<String> columns,
                                                   ConnectorSession session) {
        return GeneratedSourceReads.compile(sourceCompiler, table, columns, session);
    }

    /** Uses the sole durable artifact store and the same metadata database outbox. */
    public DurableGeneratedValueBuildService(
            DataBuildStore store,
            GeneratedValueBuildSubmissionStore submissions,
            SnapshotDataBuildSplitPlanner splitPlanner,
            WorkerSelector workerSelector,
            String baseArtifactUri,
            org.pangrova.metadata.core.catalog.registry.CatalogRegistry catalogs) {
        this.store = Objects.requireNonNull(store, "store must not be null");
        this.submissions = Objects.requireNonNull(
                submissions, "submissions must not be null");
        this.splitPlanner = Objects.requireNonNull(
                splitPlanner, "splitPlanner must not be null");
        this.workerSelector = Objects.requireNonNull(
                workerSelector, "workerSelector must not be null");
        if (baseArtifactUri == null || baseArtifactUri.isBlank()) {
            throw new IllegalArgumentException("baseArtifactUri must not be blank");
        }
        this.baseArtifactUri = stripTrailingSlash(baseArtifactUri);
        this.sourceSchemaVerifier = new org.pangrova.coordinator.databuild.ProtectedSourceSchemaVerifier(catalogs);
    }

    /**
     * Persists one independently retryable generation for each definition.
     * Replaying the same snapshot/model/definition is idempotent.
     */
    public List<String> submitCommitted(
            WriteCommitEntry write,
            GeneratedColumnSet definitions,
            QualifiedTableName table,
            String sourceSnapshot,
            ConnectorSession connectorSession) {
        GeneratedColumnMetadataCodec.validate(definitions);
        Objects.requireNonNull(table, "table must not be null");
        Objects.requireNonNull(connectorSession, "connectorSession must not be null");
        requireText(sourceSnapshot, "sourceSnapshot");
        if (write.state() != WriteCommitEntry.State.COMMITTED || definitions.getDefinitionsCount() == 0
                || !write.authorizationReceipt().declaration().table().equals(table)
                || !write.principal().equals(connectorSession.getUser())
                || !write.tenantId().equals(connectorSession.getProperties().get("tenant_id"))
                || !write.authorizationReceipt().declaration().generatedColumnsSha256().equals(
                        org.pangrova.connector.spi.coordination.WriteDeclarationSnapshot.generatedColumnsDigest(
                                definitions.toByteArray()))) {
            throw new org.pangrova.security.api.AuthorizationDeniedException(
                    "Generated maintenance must preserve its exact committed writer, tenant and declaration");
        }
        var user = new UserIdentity(write.principal(), java.util.Set.of(),
                java.util.Map.of("tenant_id", write.tenantId()), Instant.EPOCH);
        var authorizationScope = DeferredAuthorizationScopeCodec.decode(
                write.authorizationReceipt().authorizationScope());
        List<DataBuildSplit> splits = splitPlanner.enumerate(
                table, sourceSnapshot, connectorSession);
        List<GeneratedValueBuildSubmission> builds = new ArrayList<>(definitions.getDefinitionsCount());
        for (GeneratedColumnDefinition definition : definitions.getDefinitionsList()) {
            builds.add(prepareDefinition(
                    definition, table, sourceSnapshot, splits, user, authorizationScope));
        }
        submissions.submitCommittedBuilds(write, builds);
        return builds.stream().map(build -> build.job().jobId()).toList();
    }

    /**
     * Fails closed unless every definition can be resolved to one exact model
     * and one Native Worker jointly supports model invocation, generated-value
     * construction, and the immutable sidecar format.
     */
    public void requireExecutable(GeneratedColumnSet definitions, UserIdentity user) {
        GeneratedColumnMetadataCodec.validate(definitions);
        for (GeneratedColumnDefinition definition : definitions.getDefinitionsList()) {
            validateDefinitionCapability(definition);
            OperatorBinding model = AiInvocationOperatorBinder.bindTemplate(
                    new org.pangrova.planner.AiInvocationPlanHint(
                            GeneratedValueDataBuildPlanBuilder.MODEL_INVOCATION_ID, "AI_EMBED",
                            definition.getLogicalModelName(),
                            org.pangrova.planner.AiInvocationPlanHint.Operation.EMBEDDING, false),
                    workerSelector, user);
            requireModelDimension(definition, model);
            OperatorBinding sink = OperatorBinding.newBuilder()
                    .setPlanNodeId(GeneratedValueDataBuildPlanBuilder.ARTIFACT_PLAN_NODE_ID)
                    .setCapability(BuiltInCapabilityCatalog.generatedValuesArtifactBuildPlanRef())
                    .setOutputArtifact(ArtifactWriteBinding.newBuilder()
                            .setFormat(BuiltInCapabilityCatalog.generatedValuesArtifactFormat()))
                    .build();
            ensureExecutableTogether(model, sink, tenantId(user));
        }
    }

    /** Cancels all unfinished generations of one generated target. */
    public int cancel(
            QualifiedTableName table, String targetColumn, UserIdentity user, Instant now) {
        return store.cancelArtifactBuilds(
                tenantId(user),
                artifactId(sourceId(table), targetColumn),
                Objects.requireNonNull(now, "now must not be null"));
    }

    private GeneratedValueBuildSubmission prepareDefinition(
            GeneratedColumnDefinition definition,
            QualifiedTableName table,
            String sourceSnapshot,
            List<DataBuildSplit> splits,
            UserIdentity user,
            DeferredAuthorizationScope authorizationScope) {
        validateDefinitionCapability(definition);
        String tenantId = tenantId(user);
        int dimension = vectorDimension(definition);
        var generated = preparePlan(definition, table, sourceSnapshot, user, authorizationScope, dimension);
        DataBuildPlan plan = generated.artifact();
        OperatorBinding model = AiInvocationOperatorBinder.bindTemplate(
                generated.modelInvocation(), workerSelector, user);
        requireModelDimension(definition, model);

        String sourceId = sourceId(table);
        String artifactId = artifactId(sourceId, definition.getTargetColumn());
        String generation = generation(
                definition, sourceSnapshot, model.getModel().getModel().toByteArray(),
                principal(user), authorizationScope);
        String tenantScope = sha256(tenantId.getBytes(StandardCharsets.UTF_8)).substring(0, 32);
        String jobId = "generated-values-build:tenant-" + tenantScope + ":" + artifactId
                + ":generation:" + generation;
        String stagingUri = baseArtifactUri + "/v2/tenants/" + tenantScope + "/artifacts/"
                + artifactId + "/generation-" + generation;
        List<NamedValue> properties = artifactProperties(definition, dimension);
        var identity = new GeneratedValueBuildRequests.Identity(tenantId, sourceId, artifactId, generation,
                jobId, stagingUri, sourceSnapshot);
        ArtifactWriteBinding output = GeneratedValueBuildRequests.output(plan, identity, properties);
        OperatorBinding sink = OperatorBinding.newBuilder()
                .setPlanNodeId(plan.planNodeId())
                .setCapability(plan.capability())
                .setOutputArtifact(output)
                .build();

        ensureExecutableTogether(model, sink, tenantId(user));
        List<DataBuildPage> pages = pages(jobId, splits);
        Instant now = Instant.now();
        DataBuildJob job = GeneratedValueBuildRequests.job(plan, identity, pages.size(), now);
        OperatorBindingSet bindingSet = OperatorBindingSet.newBuilder()
                .addBindings(model)
                .addBindings(sink)
                .build();
        DataBuildDispatchSpec dispatch = dispatch(identity, plan, bindingSet, pages, user, authorizationScope);
        var descriptor = new GeneratedValueBuildDescriptor(
                tenantId, artifactId, sourceId, table.catalog(), table.schema(), table.table(),
                definition.getTargetColumn(),
                sha256(definition.toByteArray()), generation, sourceSnapshot, now);
        return new GeneratedValueBuildSubmission(job, dispatch, pages, descriptor);
    }

    private static DataBuildDispatchSpec dispatch(GeneratedValueBuildRequests.Identity identity,
            DataBuildPlan plan, OperatorBindingSet bindings, List<DataBuildPage> pages,
            UserIdentity user, DeferredAuthorizationScope authorizationScope) {
        var models = authorizationScope.definitionSnapshot().map(DeferredAuthorizationScope.DefinitionSnapshot::models)
                .orElseThrow(() -> new IllegalArgumentException(
                            "Generated durable stage requires observed model authority"));
        var template = FrozenStageBindings.freeze(
                new BoundExecutionPlan(plan.planBytes(), bindings.getBindingsList()), models);
        return DataBuildDispatchSpecFactory.create(
                identity.jobId(),
                DataBuildStageGraphFactory.forArtifactPages(plan.planBytes(), template.bindingBytes(), pages, models),
                identity.tenant(),
                principal(user),
                authorizationScope,
                new DataBuildDispatchSpec.ResourceGrant(
                        DEFAULT_MEMORY_BYTES,
                        1,
                        DEFAULT_SPILL_BYTES,
                        DEFAULT_TIMEOUT_MILLIS));
    }

    private GeneratedValueDataBuildPlan preparePlan(GeneratedColumnDefinition definition,
            QualifiedTableName table, String sourceSnapshot, UserIdentity user,
            DeferredAuthorizationScope authorizationScope, int dimension) {
        var connectorSession = new GeneratedInputSession(table, user);
        var source = compileSource(table, definition.getSourceColumnsList(), connectorSession);
        sourceSchemaVerifier.verify(table, sourceSnapshot, connectorSession, source.relation());
        var declaredReads = authorizationScope.permissions().stream().filter(permission ->
                permission.table().equals(table)
                        && permission.privilege() == org.pangrova.security.api.policy.Privilege.SELECT)
                .toList();
        if (!declaredReads.contains(source.permission())) {
            throw new org.pangrova.security.api.AuthorizationDeniedException(
                    "Generated source policies changed since admission; securely resubmit the write intent");
        }
        return GeneratedValueDataBuildPlanBuilder.build(
                        org.pangrova.planner.databuild.PhysicalRowLocatorPlanAugmenter.prepend(
                                source.relation().planBytes(), List.of(table.catalog(), table.schema(), table.table())),
                        definition.getTargetColumn(),
                        definition.getSourceColumnsList(),
                        definition.getLogicalModelName(),
                        dimension);
    }

    private void ensureExecutableTogether(
            OperatorBinding model, OperatorBinding sink, String tenantId) {
        List<OperatorBinding> bindings = List.of(model, sink);
        var selected = workerSelector.selectForBindings(
                tenantId, List.of(), bindings);
        if (selected == null || !WorkerCapabilityMatcher.supports(selected, bindings)) {
            throw new PangrovaException(
                    PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "No active Native Worker jointly advertises the exact generated-value "
                            + "materializer, artifact format, and model deployment");
        }
    }

    private static List<DataBuildPage> pages(
            String jobId, List<DataBuildSplit> splits) {
        List<DataBuildPage> pages = new ArrayList<>(splits.size());
        for (int ordinal = 0; ordinal < splits.size(); ordinal++) {
            pages.add(new DataBuildPage(
                    jobId,
                    jobId + ":map:" + ordinal,
                    0,
                    ordinal,
                    ordinal,
                    DataBuildPageKind.SOURCE_MAP,
                    List.of(splits.get(ordinal)),
                    List.of(),
                    DEFAULT_MAX_ATTEMPTS));
        }
        return List.copyOf(pages);
    }

    private static List<NamedValue> artifactProperties(
            GeneratedColumnDefinition definition, int dimension) {
        List<NamedValue> properties = new ArrayList<>();
        properties.add(stringProperty("target_column", definition.getTargetColumn()));
        properties.add(stringProperty(
                "output_type", definition.getOutputType().getCanonicalSql()));
        properties.add(intProperty("vector_dimension", dimension));
        properties.add(stringProperty(
                "source_encoding",
                GeneratedValueDataBuildPlanBuilder.SOURCE_ENCODING));
        properties.add(stringProperty(
                "execution_mode", definition.getExecutionMode().name()));
        for (int index = 0; index < definition.getSourceColumnsCount(); index++) {
            properties.add(stringProperty(
                    "source_column." + index,
                    definition.getSourceColumns(index)));
        }
        definition.getOptionsList().stream()
                .sorted(Comparator.comparing(
                        option -> option.getName().toLowerCase(Locale.ROOT)))
                .map(option -> NamedValue.newBuilder()
                        .setName("definition_option." + option.getName())
                        .setValue(option.getValue())
                        .build())
                .forEach(properties::add);
        return List.copyOf(properties);
    }

    private static void validateDefinitionCapability(
            GeneratedColumnDefinition definition) {
        if (!WorkerCapabilityMatcher.matchesCapabilityRef(
                definition.getCapability(),
                BuiltInCapabilityCatalog.aiEmbeddingPlanRef())) {
            throw new PangrovaException(
                    PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Generated VECTOR materialization requires the canonical AI embedding "
                            + "capability: " + definition.getTargetColumn());
        }
        requireText(definition.getLogicalModelName(), "logicalModelName");
    }

    private static int vectorDimension(GeneratedColumnDefinition definition) {
        try {
            return VectorLogicalType.requireDimension(definition.getOutputType());
        } catch (IllegalArgumentException failure) {
            throw new PangrovaException(
                    PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Generated embedding output must have the complete canonical FLOAT32 VECTOR contract",
                    failure);
        }
    }

    private static void requireModelDimension(
            GeneratedColumnDefinition definition, OperatorBinding modelBinding) {
        int declared = vectorDimension(definition);
        int model = VectorLogicalType.requireDimension(
                modelBinding.getModel().getOutputType());
        if (declared != model) {
            throw new PangrovaException(
                    PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Generated VECTOR dimension " + declared
                            + " differs from exact model output dimension " + model
                            + ": " + definition.getLogicalModelName());
        }
    }

    private static String sourceId(QualifiedTableName table) {
        Objects.requireNonNull(table, "table must not be null");
        return String.join(".", table.catalog(), table.schema(), table.table());
    }

    private static String artifactId(String sourceId, String targetColumn) {
        requireText(targetColumn, "targetColumn");
        return "generated-values-" + sha256(
                (sourceId + "\u0000" + targetColumn.toLowerCase(Locale.ROOT))
                        .getBytes(StandardCharsets.UTF_8));
    }

    private static String generation(
            GeneratedColumnDefinition definition,
            String sourceSnapshot,
            byte[] exactModelIdentity, String principal, DeferredAuthorizationScope authorizationScope) {
        MessageDigest digest = sha256Digest();
        digest.update(definition.toByteArray());
        digest.update((byte) 0);
        digest.update(sourceSnapshot.getBytes(StandardCharsets.UTF_8));
        digest.update((byte) 0);
        digest.update(exactModelIdentity);
        digest.update((byte) 0);
        digest.update(principal.getBytes(StandardCharsets.UTF_8));
        digest.update((byte) 0);
        digest.update(DeferredAuthorizationScopeCodec.sha256(authorizationScope).getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(digest.digest());
    }

    private static NamedValue intProperty(String name, long value) {
        return NamedValue.newBuilder()
                .setName(name)
                .setValue(TypedValue.newBuilder()
                        .setType(LogicalType.newBuilder().setCanonicalSql("BIGINT"))
                        .setInt64Value(value))
                .build();
    }

    private static NamedValue stringProperty(String name, String value) {
        return NamedValue.newBuilder()
                .setName(name)
                .setValue(TypedValue.newBuilder()
                        .setType(LogicalType.newBuilder().setCanonicalSql("VARCHAR"))
                        .setStringValue(value))
                .build();
    }

    private static String tenantId(UserIdentity user) {
        String tenantId = user == null ? "" : user.attributes().getOrDefault(
                "tenant_id", user.attributes().getOrDefault("tenantId", ""));
        requireText(tenantId, "tenantId");
        return tenantId;
    }

    private record GeneratedInputSession(QualifiedTableName table, UserIdentity user) implements ConnectorSession {
        @Override
        public java.util.Optional<org.pangrova.connector.spi.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return user.principal(); }
        @Override public java.util.Optional<String> getCatalog() { return java.util.Optional.of(table.catalog()); }
        @Override public java.util.Optional<String> getSchema() { return java.util.Optional.of(table.schema()); }
        @Override public java.util.Map<String, String> getProperties() { return user.attributes(); }
    }

    private static String principal(UserIdentity user) {
        return user == null || user.principal() == null ? "" : user.principal();
    }

    private static String stripTrailingSlash(String value) {
        String result = value;
        while (result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        if (result.isBlank()) {
            throw new IllegalArgumentException(
                    "baseArtifactUri must not be a filesystem root");
        }
        return result;
    }

    private static String sha256(byte[] value) {
        return HexFormat.of().formatHex(sha256Digest().digest(value));
    }

    private static MessageDigest sha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
