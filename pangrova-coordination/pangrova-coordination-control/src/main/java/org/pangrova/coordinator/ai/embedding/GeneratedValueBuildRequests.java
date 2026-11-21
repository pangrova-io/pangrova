package org.pangrova.coordinator.ai.embedding;

import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.enums.GenerationJobState;
import org.pangrova.generation.api.dependency.GenerationDependencySet;
import org.pangrova.generation.api.enums.GenerationEmptyPartitionPolicy;
import org.pangrova.generation.api.enums.GenerationExecutionKind;
import org.pangrova.generation.api.constants.GenerationOutputTypeIds;
import org.pangrova.planner.generation.GenerationPlan;
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

    static ArtifactWriteBinding output(GenerationPlan plan, Identity identity, List<NamedValue> properties) {
        return ArtifactWriteBinding.newBuilder()
                .setArtifactId(identity.artifactId())
                .setArtifactType(GenerationOutputTypeIds.GENERATED_COLUMN_VALUES)
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
                                HexFormat.of().parseHex(GenerationDependencySet.sourceSnapshot(
                                        identity.sourceId(), identity.snapshot()).sha256()))))
                .setEmptyPartitionPolicy(org.pangrova.protocol.v2.ArtifactEmptyPartitionPolicy
                        .ARTIFACT_EMPTY_PARTITION_POLICY_FORBID)
                .build();
    }

    static GenerationJob job(GenerationPlan plan, Identity identity, int pageCount, Instant now) {
        return new GenerationJob(
                identity.tenant(),
                identity.jobId(),
                identity.artifactId(),
                GenerationOutputTypeIds.GENERATED_COLUMN_VALUES,
                identity.generation(),
                identity.sourceId(),
                identity.snapshot(),
                GenerationExecutionKind.BUILD,
                GenerationDependencySet.sourceSnapshot(identity.sourceId(), identity.snapshot()),
                GenerationEmptyPartitionPolicy.FORBID,
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
                        ? GenerationJobState.READY_TO_PUBLISH
                        : GenerationJobState.QUEUED,
                0,
                now,
                now,
                now);
    }
}
