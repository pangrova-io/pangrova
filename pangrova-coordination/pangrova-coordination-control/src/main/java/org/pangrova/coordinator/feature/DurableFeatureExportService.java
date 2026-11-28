/*

 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.coordinator.feature;

import org.pangrova.security.api.enums.AuthorizedReadMode;
import org.pangrova.generation.submission.GenerationDispatchSpecFactory;
import org.pangrova.generation.api.graph.GenerationStageGraph;

import com.google.protobuf.ByteString;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.crypto.Sha256Digests;
import org.pangrova.coordinator.generation.SnapshotGenerationSplitPlanner;
import org.pangrova.coordinator.generation.ProtectedSourceSchemaVerifier;
import org.pangrova.planner.analysis.AuthorizedRelation;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.enums.GenerationJobState;
import org.pangrova.generation.api.page.GenerationPage;
import org.pangrova.generation.api.page.GenerationSplit;
import org.pangrova.ai.control.feature.CatalogExportBuildStore;
import org.pangrova.generation.api.dependency.GenerationDependencySet;
import org.pangrova.generation.api.enums.GenerationEmptyPartitionPolicy;
import org.pangrova.generation.api.enums.GenerationExecutionKind;
import org.pangrova.generation.api.constants.GenerationOutputTypeIds;
import org.pangrova.ai.control.feature.FeatureCatalogStore.FeatureViewVersion;
import org.pangrova.ai.control.feature.CatalogExportRequest;
import org.pangrova.ai.control.enums.CatalogExportKind;
import org.pangrova.planner.generation.GenerationPlan;
import org.pangrova.planner.generation.TabularExportPlanBuilder;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.security.codec.authorization.DeferredAuthorizationScopeCodec;
import org.pangrova.security.api.enums.Privilege;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.pangrova.common.validation.Preconditions.requireText;

/** Submits single-source Feature View snapshots to the generic Native export pipeline. */
public final class DurableFeatureExportService {

    private static final int MAX_ATTEMPTS = 5;
    private static final long MEMORY_BYTES = 1024L * 1024 * 1024;
    private static final long SPILL_BYTES = 4L * 1024 * 1024 * 1024;
    private static final long TIMEOUT_MILLIS = Duration.ofHours(1).toMillis();

    private final CatalogExportBuildStore buildStore;
    private final SnapshotGenerationSplitPlanner splitPlanner;
    private final String baseArtifactUri;
    private final ProtectedSourceSchemaVerifier sourceSchemaVerifier;

    /** Uses actual pinned connector splits and the sole durable artifact store. */
    public DurableFeatureExportService(
            CatalogExportBuildStore buildStore,
            SnapshotGenerationSplitPlanner splitPlanner,
            String baseArtifactUri,
            CatalogRegistry catalogRegistry) {
        this.buildStore = Objects.requireNonNull(buildStore, "buildStore must not be null");
        this.splitPlanner = Objects.requireNonNull(splitPlanner, "splitPlanner must not be null");
        if (baseArtifactUri == null || baseArtifactUri.isBlank()) {
            throw new IllegalArgumentException("baseArtifactUri must not be blank");
        }
        this.baseArtifactUri = baseArtifactUri.replaceAll("/+$", "");
        this.sourceSchemaVerifier = new ProtectedSourceSchemaVerifier(catalogRegistry);
    }

    /** Creates or reuses one exact immutable export job. */
    public CatalogExportRequest submit(
            FeatureViewVersion feature,
            String sourceSnapshot,
            ConnectorSession connectorSession,
            String requestedBy,
            AuthorizedRelation compiled) {
        Objects.requireNonNull(feature, "feature must not be null");
        Objects.requireNonNull(connectorSession, "connectorSession must not be null");
        requireText(sourceSnapshot, "sourceSnapshot");
        requireText(requestedBy, "requestedBy");
        var source = parseSource(feature.sourceTable());
        if (!feature.id().tenantId().equals(connectorSession.getProperties().get("tenant_id"))
                || !requestedBy.equals(connectorSession.getUser())
                || compiled.sources().size() != 1
                || !compiled.sources().get(0).qualifiedName().equals(source.toString())) {
            throw new org.pangrova.security.api.exception.AccessDeniedException(
                    "Feature export must bind the current authenticated principal, tenant and exact compiled source");
        }
        sourceSchemaVerifier.verify(source, sourceSnapshot, connectorSession, compiled);
        List<GenerationSplit> splits = splitPlanner.enumerate(
                source, sourceSnapshot, connectorSession);
        GenerationPlan plan = TabularExportPlanBuilder.build(compiled.planBytes());
        DeferredAuthorizationScope authorizationScope = authorizationScope(feature, compiled);
        ExportIdentity identity = exportIdentity(feature, sourceSnapshot, requestedBy, compiled, authorizationScope);
        ArtifactWriteBinding output = outputBinding(
                plan, identity.artifactId(), identity.generation(), identity.sourceId(),
                sourceSnapshot, identity.stagingUri(), identity.dependencies());
        OperatorBinding sink = OperatorBinding.newBuilder()
                .setPlanNodeId(plan.planNodeId())
                .setCapability(plan.capability())
                .setOutputArtifact(output)
                .build();
        var prepared = FeatureExportStageGraph.prepare(compiled.stageGraph(), identity.jobId(), source.toString(),
                splits, sink, MAX_ATTEMPTS);
        List<GenerationPage> pages = prepared.pages();
        Instant now = Instant.now();
        GenerationJob job = buildJob(
                identity, sourceSnapshot, plan, pages.size(), now);
        GenerationDispatchSpec dispatch = dispatchSpec(
                identity, prepared.graph(), requestedBy, authorizationScope);
        CatalogExportRequest request = exportRequest(feature, identity, requestedBy, now);
        buildStore.createCatalogExportJob(request, job, dispatch, pages);
        return request;
    }

    private ExportIdentity exportIdentity(
            FeatureViewVersion feature, String sourceSnapshot, String principal, AuthorizedRelation compiled,
            DeferredAuthorizationScope authorizationScope) {
        String tenantId = feature.id().tenantId();
        String artifactId = "feature-view/" + feature.id().catalogName() + "/"
                + feature.id().schemaName() + "/" + feature.id().viewName()
                + "/v" + feature.version();
        String generation = Sha256Digests.sha256Hex(
                (feature.definitionSha256() + "\n" + sourceSnapshot + "\n" + principal
                        + "\n" + compiled.effectivePlanDigest() + "\n"
                        + Sha256Digests.sha256Hex(compiled.planBytes()) + "\n" + compiled.stageGraph().sha256()
                        + "\n" + DeferredAuthorizationScopeCodec.sha256(authorizationScope))
                        .getBytes(StandardCharsets.UTF_8));
        String tenantScope = Sha256Digests.sha256Hex(tenantId).substring(0, 32);
        String artifactDigest = Sha256Digests.sha256Hex(artifactId);
        String jobId = "tabular-export:tenant-" + tenantScope + ":"
                + artifactDigest.substring(0, 32) + ":" + generation;
        String stagingUri = baseArtifactUri + "/v2/tenants/" + tenantScope
                + "/exports/" + artifactDigest + "/generation-" + generation;
        GenerationDependencySet dependencies = GenerationDependencySet.sourceSnapshot(
                feature.sourceTable(), sourceSnapshot);
        return new ExportIdentity(
                tenantId, feature.sourceTable(), artifactId, generation,
                jobId, stagingUri, dependencies);
    }

    private static GenerationJob buildJob(
            ExportIdentity identity,
            String sourceSnapshot,
            GenerationPlan plan,
            int pageCount,
            Instant now) {
        GenerationJobState state = pageCount == 0
                ? GenerationJobState.READY_TO_PUBLISH
                : GenerationJobState.QUEUED;
        return new GenerationJob(
                identity.tenantId(), identity.jobId(), identity.artifactId(),
                GenerationOutputTypeIds.TABULAR_EXPORT, identity.generation(), identity.sourceId(),
                sourceSnapshot, GenerationExecutionKind.EXPORT, identity.dependencies(),
                GenerationEmptyPartitionPolicy.ALLOW_ZERO_ROWS, identity.stagingUri(),
                plan.format().getFormatId(), plan.format().getMajorVersion(),
                plan.format().getMinorVersion(), HexFormat.of().formatHex(
                        plan.format().getSchemaSha256().toByteArray()),
                plan.format().getWriterBuildId(), "", identity.jobId(), pageCount, 0,
                state, 0, now, now, now);
    }

    private static GenerationDispatchSpec dispatchSpec(
            ExportIdentity identity,
            GenerationStageGraph graph,
            String requestedBy,
            DeferredAuthorizationScope authorizationScope) {
        return GenerationDispatchSpecFactory.create(
                identity.jobId(), graph,
                identity.tenantId(), requestedBy, authorizationScope,
                new GenerationDispatchSpec.ResourceGrant(
                        MEMORY_BYTES, 1, SPILL_BYTES, TIMEOUT_MILLIS));
    }

    private static DeferredAuthorizationScope authorizationScope(
            FeatureViewVersion feature, AuthorizedRelation compiled) {
        var source = compiled.sources().get(0);
        Set<String> columns = Set.copyOf(source.tableColumns());
        var observed = compiled.observedPolicies().get(source.qualifiedName());
        if (columns.isEmpty() || observed == null || !observed.columnMasks().keySet().containsAll(columns)) {
            throw new org.pangrova.security.api.exception.AccessDeniedException(
                    "Feature export requires the actual complete policy observation from secure compilation");
        }
        var read = new DeferredAuthorizationScope.TablePermission(
                new QualifiedTableName(source.catalogName(), source.schemaName(), source.tableName()),
                columns, Privilege.SELECT, AuthorizedReadMode.SECURITY_TRANSFORMED, observed);
        var target = new DeferredAuthorizationScope.TablePermission(
                new QualifiedTableName(feature.id().catalogName(), feature.id().schemaName(), feature.id().viewName()),
                Set.of(), Privilege.ALTER, AuthorizedReadMode.NONE,
                DeferredAuthorizationScope.PolicySnapshot.empty());
        return new DeferredAuthorizationScope(List.of(read, target), java.util.Optional.empty());
    }

    private static CatalogExportRequest exportRequest(
            FeatureViewVersion feature,
            ExportIdentity identity,
            String requestedBy,
            Instant now) {
        return new CatalogExportRequest(
                identity.tenantId(), CatalogExportKind.FEATURE_VIEW,
                feature.id().catalogName(), feature.id().schemaName(),
                feature.id().viewName(), feature.version(), identity.generation(),
                identity.artifactId(), identity.jobId(), identity.dependencies().sha256(),
                requestedBy, now);
    }

    private static ArtifactWriteBinding outputBinding(
            GenerationPlan plan, String artifactId, String generation,
            String sourceId, String snapshot, String stagingUri,
            GenerationDependencySet dependencies) {
        ArtifactWriteBinding.Builder output = ArtifactWriteBinding.newBuilder()
                .setArtifactId(artifactId)
                .setArtifactType(GenerationOutputTypeIds.TABULAR_EXPORT)
                .setGeneration(generation)
                .setSourceId(sourceId)
                .setSourceSnapshot(snapshot)
                .setStagingUri(stagingUri)
                .setFormat(plan.format())
                .setExecutionKind(org.pangrova.protocol.v2.ArtifactExecutionKind
                        .ARTIFACT_EXECUTION_KIND_EXPORT)
                .setDependencySetChecksum(Checksum.newBuilder()
                        .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(
                                HexFormat.of().parseHex(dependencies.sha256()))))
                .setEmptyPartitionPolicy(
                        org.pangrova.protocol.v2.ArtifactEmptyPartitionPolicy
                                .ARTIFACT_EMPTY_PARTITION_POLICY_ALLOW_ZERO_ROWS);
        dependencies.dependencies().forEach(dependency -> output.addDependencies(
                org.pangrova.protocol.v2.ArtifactDependency.newBuilder()
                        .setKind(dependency.kind())
                        .setId(dependency.id())
                        .setVersion(dependency.version())));
        return output.build();
    }

    private static org.pangrova.connector.api.metadata.table.QualifiedTableName parseSource(
            String source) {
        String[] parts = source.split("\\.", -1);
        if (parts.length != 3 || java.util.Arrays.stream(parts).anyMatch(String::isBlank)) {
            throw validation("Feature source must be catalog.schema.table");
        }
        return new org.pangrova.connector.api.metadata.table.QualifiedTableName(
                parts[0], parts[1], parts[2]);
    }

    private static PangrovaException validation(String message) {
        return new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message);
    }

    private record ExportIdentity(
            String tenantId,
            String sourceId,
            String artifactId,
            String generation,
            String jobId,
            String stagingUri,
            GenerationDependencySet dependencies) {}
}
