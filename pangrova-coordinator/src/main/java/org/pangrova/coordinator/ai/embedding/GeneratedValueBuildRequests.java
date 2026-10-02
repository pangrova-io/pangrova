package org.pangrova.coordinator.ai.embedding;

import org.pangrova.metadata.artifact.build.ArtifactBuildJob;
import org.pangrova.metadata.artifact.build.ArtifactBuildJobState;
import org.pangrova.metadata.artifact.contract.ArtifactDependencySet;
import org.pangrova.metadata.artifact.contract.ArtifactEmptyPartitionPolicy;
import org.pangrova.metadata.artifact.contract.ArtifactExecutionKind;
import org.pangrova.metadata.artifact.contract.ArtifactTypeIds;
import org.pangrova.planner.artifact.ArtifactBuildPlan;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.NamedValue;

import java.time.Instant;
import java.util.HexFormat;
import java.util.List;

/** Canonical immutable generated payloads, without persistence or authorization side effects. */
final class GeneratedValueBuildRequests {
    private GeneratedValueBuildRequests() {}

    record Identity(String tenant, String sourceId, String artifactId, String generation,
                    String jobId, String stagingUri, String snapshot) {}

    static ArtifactWriteBinding output(ArtifactBuildPlan plan, Identity identity, List<NamedValue> properties) {
        return ArtifactWriteBinding.newBuilder()
                .setArtifactId(identity.artifactId())
                .setArtifactType(ArtifactTypeIds.GENERATED_COLUMN_VALUES)
                .setGeneration(identity.generation())
                .setSourceId(identity.sourceId())
                .setSourceSnapshot(identity.snapshot())
                .setStagingUri(identity.stagingUri())
                .setFormat(plan.format())
                .addAllProperties(properties)
                .setRowLocator(plan.rowLocator())
                .setExecutionKind(org.pangrova.protocol.v2.ArtifactExecutionKind
                        .ARTIFACT_EXECUTION_KIND_BUILD)
                .addDependencies(org.pangrova.protocol.v2.ArtifactDependency.newBuilder()
                        .setKind("SOURCE_SNAPSHOT")
                        .setId(identity.sourceId())
                        .setVersion(identity.snapshot()))
                .setDependencySetChecksum(org.pangrova.protocol.v2.Checksum.newBuilder()
                        .setAlgorithm(org.pangrova.protocol.v2.Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(com.google.protobuf.ByteString.copyFrom(
                                HexFormat.of().parseHex(ArtifactDependencySet.sourceSnapshot(
                                        identity.sourceId(), identity.snapshot()).sha256()))))
                .setEmptyPartitionPolicy(org.pangrova.protocol.v2.ArtifactEmptyPartitionPolicy
                        .ARTIFACT_EMPTY_PARTITION_POLICY_FORBID)
                .build();
    }

    static ArtifactBuildJob job(ArtifactBuildPlan plan, Identity identity, int pageCount, Instant now) {
        return new ArtifactBuildJob(
                identity.tenant(),
                identity.jobId(),
                identity.artifactId(),
                ArtifactTypeIds.GENERATED_COLUMN_VALUES,
                identity.generation(),
                identity.sourceId(),
                identity.snapshot(),
                ArtifactExecutionKind.BUILD,
                ArtifactDependencySet.sourceSnapshot(identity.sourceId(), identity.snapshot()),
                ArtifactEmptyPartitionPolicy.FORBID,
                identity.stagingUri(),
                plan.format().getFormatId(),
                plan.format().getMajorVersion(),
                plan.format().getMinorVersion(),
                HexFormat.of().formatHex(
                        plan.format().getSchemaSha256().toByteArray()),
                plan.format().getWriterBuildId(),
                "",
                identity.jobId(),
                pageCount,
                0,
                pageCount == 0
                        ? ArtifactBuildJobState.READY_TO_PUBLISH
                        : ArtifactBuildJobState.QUEUED,
                0,
                now,
                now,
                now);
    }
}
