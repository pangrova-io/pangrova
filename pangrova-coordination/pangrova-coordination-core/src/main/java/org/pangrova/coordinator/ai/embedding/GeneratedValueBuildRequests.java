package org.pangrova.coordinator.ai.embedding;

import org.pangrova.databuild.api.build.DataBuildJob;
import org.pangrova.databuild.api.build.DataBuildJobState;
import org.pangrova.databuild.api.DataBuildDependencySet;
import org.pangrova.databuild.api.DataBuildEmptyPartitionPolicy;
import org.pangrova.databuild.api.DataBuildExecutionKind;
import org.pangrova.databuild.api.DataBuildOutputTypeIds;
import org.pangrova.planner.databuild.DataBuildPlan;
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

    static ArtifactWriteBinding output(DataBuildPlan plan, Identity identity, List<NamedValue> properties) {
        return ArtifactWriteBinding.newBuilder()
                .setArtifactId(identity.artifactId())
                .setArtifactType(DataBuildOutputTypeIds.GENERATED_COLUMN_VALUES)
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
                                HexFormat.of().parseHex(DataBuildDependencySet.sourceSnapshot(
                                        identity.sourceId(), identity.snapshot()).sha256()))))
                .setEmptyPartitionPolicy(org.pangrova.protocol.v2.ArtifactEmptyPartitionPolicy
                        .ARTIFACT_EMPTY_PARTITION_POLICY_FORBID)
                .build();
    }

    static DataBuildJob job(DataBuildPlan plan, Identity identity, int pageCount, Instant now) {
        return new DataBuildJob(
                identity.tenant(),
                identity.jobId(),
                identity.artifactId(),
                DataBuildOutputTypeIds.GENERATED_COLUMN_VALUES,
                identity.generation(),
                identity.sourceId(),
                identity.snapshot(),
                DataBuildExecutionKind.BUILD,
                DataBuildDependencySet.sourceSnapshot(identity.sourceId(), identity.snapshot()),
                DataBuildEmptyPartitionPolicy.FORBID,
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
                        ? DataBuildJobState.READY_TO_PUBLISH
                        : DataBuildJobState.QUEUED,
                0,
                now,
                now,
                now);
    }
}
