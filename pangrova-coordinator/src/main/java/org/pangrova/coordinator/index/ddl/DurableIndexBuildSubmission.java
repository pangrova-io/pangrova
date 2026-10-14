package org.pangrova.coordinator.index.ddl;

import com.google.protobuf.ByteString;
import org.pangrova.metadata.artifact.build.ArtifactBuildDispatchSpec;
import org.pangrova.metadata.artifact.build.ArtifactBuildJob;
import org.pangrova.metadata.artifact.build.ArtifactBuildJobState;
import org.pangrova.metadata.artifact.build.ArtifactBuildPage;
import org.pangrova.metadata.artifact.contract.ArtifactDependencySet;
import org.pangrova.metadata.artifact.contract.ArtifactEmptyPartitionPolicy;
import org.pangrova.metadata.artifact.contract.ArtifactExecutionKind;
import org.pangrova.planner.artifact.ArtifactBuildPlan;
import org.pangrova.protocol.v2.ArtifactDependency;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.NamedValue;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorBindingSet;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.index.IndexDescriptor;

import java.time.Instant;
import java.util.HexFormat;
import java.util.List;

/** Builds immutable metadata records for one durable Native index submission. */
record DurableIndexBuildSubmission(
        ArtifactBuildJob job,
        ArtifactBuildDispatchSpec dispatch) {

    static DurableIndexBuildSubmission create(Definition definition) {
        String sourceId = String.join(".",
                definition.table().catalog(), definition.table().schema(),
                definition.table().table());
        ArtifactDependencySet dependencies = ArtifactDependencySet.sourceSnapshot(
                sourceId, definition.descriptor().buildSnapshotId());
        ArtifactWriteBinding output = outputBinding(
                definition, sourceId, dependencies);
        OperatorBinding binding = OperatorBinding.newBuilder()
                .setPlanNodeId(definition.plan().planNodeId())
                .setCapability(definition.plan().capability())
                .setOutputArtifact(output)
                .build();
        ArtifactBuildJob job = buildJob(
                definition, sourceId, dependencies, output);
        ArtifactBuildDispatchSpec dispatch = ArtifactBuildDispatchSpec.of(
                definition.jobId(), org.pangrova.metadata.artifact.build.ArtifactStageGraph.forArtifactPages(
                        definition.plan().planBytes(),
                        OperatorBindingSet.newBuilder().addBindings(binding).build().toByteArray(),
                        definition.pages(), java.util.List.of()),
                definition.tenantId(), definition.principal(),
                org.pangrova.security.authz.DeferredAuthorizationScope.raw(
                        definition.table(), java.util.Set.copyOf(definition.descriptor().columnNames()),
                        definition.table()),
                definition.resourceGrant());
        return new DurableIndexBuildSubmission(job, dispatch);
    }

    private static ArtifactWriteBinding outputBinding(
            Definition definition,
            String sourceId,
            ArtifactDependencySet dependencies) {
        return ArtifactWriteBinding.newBuilder()
                .setArtifactId(definition.descriptor().indexId())
                .setArtifactType(definition.artifactType())
                .setGeneration(definition.generation())
                .setSourceId(sourceId)
                .setSourceSnapshot(definition.descriptor().buildSnapshotId())
                .setStagingUri(definition.stagingUri())
                .setFormat(definition.plan().format())
                .setEncryptionKeyRef(definition.descriptor().properties()
                        .getOrDefault("encryption_key_ref", ""))
                .addAllProperties(definition.buildProperties())
                .setRowLocator(definition.plan().rowLocator())
                .setExecutionKind(org.pangrova.protocol.v2.ArtifactExecutionKind
                        .ARTIFACT_EXECUTION_KIND_BUILD)
                .addDependencies(ArtifactDependency.newBuilder()
                        .setKind("SOURCE_SNAPSHOT")
                        .setId(sourceId)
                        .setVersion(definition.descriptor().buildSnapshotId()))
                .setDependencySetChecksum(Checksum.newBuilder()
                        .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(
                                HexFormat.of().parseHex(dependencies.sha256()))))
                .setEmptyPartitionPolicy(definition.pages().isEmpty()
                        ? org.pangrova.protocol.v2.ArtifactEmptyPartitionPolicy
                                .ARTIFACT_EMPTY_PARTITION_POLICY_ALLOW_ZERO_ROWS
                        : org.pangrova.protocol.v2.ArtifactEmptyPartitionPolicy
                                .ARTIFACT_EMPTY_PARTITION_POLICY_FORBID)
                .build();
    }

    private static ArtifactBuildJob buildJob(
            Definition definition,
            String sourceId,
            ArtifactDependencySet dependencies,
            ArtifactWriteBinding output) {
        ArtifactBuildPlan plan = definition.plan();
        List<ArtifactBuildPage> pages = definition.pages();
        return new ArtifactBuildJob(
                definition.tenantId(), definition.jobId(),
                definition.descriptor().indexId(), definition.artifactType(),
                definition.generation(), sourceId,
                definition.descriptor().buildSnapshotId(), ArtifactExecutionKind.BUILD,
                dependencies, emptyPolicy(definition),
                definition.stagingUri(), plan.format().getFormatId(),
                plan.format().getMajorVersion(), plan.format().getMinorVersion(),
                HexFormat.of().formatHex(plan.format().getSchemaSha256().toByteArray()),
                plan.format().getWriterBuildId(), output.getEncryptionKeyRef(),
                definition.idempotencyKey(), pages.size(), 0,
                pages.isEmpty() ? ArtifactBuildJobState.READY_TO_PUBLISH
                        : ArtifactBuildJobState.QUEUED,
                0, definition.descriptor().updatedAt(),
                definition.now(), definition.now());
    }

    private static ArtifactEmptyPartitionPolicy emptyPolicy(Definition definition) {
        return definition.pages().isEmpty()
                ? ArtifactEmptyPartitionPolicy.ALLOW_ZERO_ROWS
                : ArtifactEmptyPartitionPolicy.FORBID;
    }

    record Definition(
            String tenantId,
            String principal,
            String jobId,
            IndexDescriptor descriptor,
            QualifiedTableName table,
            ArtifactBuildPlan plan,
            String artifactType,
            List<NamedValue> buildProperties,
            List<ArtifactBuildPage> pages,
            String generation,
            String stagingUri,
            String idempotencyKey,
            ArtifactBuildDispatchSpec.ResourceGrant resourceGrant,
            Instant now) {}
}
