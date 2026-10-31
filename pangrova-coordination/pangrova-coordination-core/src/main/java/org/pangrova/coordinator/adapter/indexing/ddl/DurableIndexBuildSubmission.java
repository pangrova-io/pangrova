package org.pangrova.coordinator.adapter.indexing.ddl;

import org.pangrova.databuild.assembly.DataBuildDispatchSpecFactory;

import com.google.protobuf.ByteString;
import org.pangrova.databuild.api.build.DataBuildDispatchSpec;
import org.pangrova.databuild.api.build.DataBuildJob;
import org.pangrova.databuild.api.build.DataBuildJobState;
import org.pangrova.databuild.api.build.DataBuildPage;
import org.pangrova.databuild.api.DataBuildDependencySet;
import org.pangrova.databuild.api.DataBuildEmptyPartitionPolicy;
import org.pangrova.databuild.api.DataBuildExecutionKind;
import org.pangrova.planner.databuild.DataBuildPlan;
import org.pangrova.protocol.v2.ArtifactDependency;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.NamedValue;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorBindingSet;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;

import java.time.Instant;
import java.util.HexFormat;
import java.util.List;

/** Builds immutable metadata records for one durable Native index submission. */
record DurableIndexBuildSubmission(
        DataBuildJob job,
        DataBuildDispatchSpec dispatch) {

    static DurableIndexBuildSubmission create(Definition definition) {
        String sourceId = String.join(".",
                definition.table().catalog(), definition.table().schema(),
                definition.table().table());
        DataBuildDependencySet dependencies = DataBuildDependencySet.sourceSnapshot(
                sourceId, definition.descriptor().buildSnapshotId());
        ArtifactWriteBinding output = outputBinding(
                definition, sourceId, dependencies);
        OperatorBinding binding = OperatorBinding.newBuilder()
                .setPlanNodeId(definition.plan().planNodeId())
                .setCapability(definition.plan().capability())
                .setOutputArtifact(output)
                .build();
        DataBuildJob job = buildJob(
                definition, sourceId, dependencies, output);
        DataBuildDispatchSpec dispatch = DataBuildDispatchSpecFactory.create(
                definition.jobId(), org.pangrova.databuild.assembly.DataBuildStageGraphFactory.forArtifactPages(
                        definition.plan().planBytes(),
                        OperatorBindingSet.newBuilder().addBindings(binding).build().toByteArray(),
                        definition.pages(), java.util.List.of()),
                definition.tenantId(), definition.principal(),
                org.pangrova.security.api.authz.DeferredAuthorizationScope.raw(
                        definition.table(), java.util.Set.copyOf(definition.descriptor().columnNames()),
                        definition.table()),
                definition.resourceGrant());
        return new DurableIndexBuildSubmission(job, dispatch);
    }

    private static ArtifactWriteBinding outputBinding(
            Definition definition,
            String sourceId,
            DataBuildDependencySet dependencies) {
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

    private static DataBuildJob buildJob(
            Definition definition,
            String sourceId,
            DataBuildDependencySet dependencies,
            ArtifactWriteBinding output) {
        DataBuildPlan plan = definition.plan();
        List<DataBuildPage> pages = definition.pages();
        return new DataBuildJob(
                definition.tenantId(), definition.jobId(),
                definition.descriptor().indexId(), definition.artifactType(),
                definition.generation(), sourceId,
                definition.descriptor().buildSnapshotId(), DataBuildExecutionKind.BUILD,
                dependencies, emptyPolicy(definition),
                definition.stagingUri(), plan.format().getFormatId(),
                plan.format().getMajorVersion(), plan.format().getMinorVersion(),
                HexFormat.of().formatHex(plan.format().getSchemaSha256().toByteArray()),
                plan.format().getWriterBuildId(), output.getEncryptionKeyRef(),
                definition.idempotencyKey(), pages.size(), 0,
                pages.isEmpty() ? DataBuildJobState.READY_TO_PUBLISH
                        : DataBuildJobState.QUEUED,
                0, definition.descriptor().updatedAt(),
                definition.now(), definition.now());
    }

    private static DataBuildEmptyPartitionPolicy emptyPolicy(Definition definition) {
        return definition.pages().isEmpty()
                ? DataBuildEmptyPartitionPolicy.ALLOW_ZERO_ROWS
                : DataBuildEmptyPartitionPolicy.FORBID;
    }

    record Definition(
            String tenantId,
            String principal,
            String jobId,
            IndexDescriptor descriptor,
            QualifiedTableName table,
            DataBuildPlan plan,
            String artifactType,
            List<NamedValue> buildProperties,
            List<DataBuildPage> pages,
            String generation,
            String stagingUri,
            String idempotencyKey,
            DataBuildDispatchSpec.ResourceGrant resourceGrant,
            Instant now) {}
}
