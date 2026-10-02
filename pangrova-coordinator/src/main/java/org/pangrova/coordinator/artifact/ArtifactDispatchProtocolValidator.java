package org.pangrova.coordinator.artifact;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.proto.Plan;
import org.pangrova.metadata.artifact.build.ArtifactBuildJob;
import org.pangrova.metadata.artifact.build.ArtifactBuildSplit;
import org.pangrova.planner.substrait.SubstraitProtocolVersion;
import org.pangrova.protocol.DataSplitProtoCodec;
import org.pangrova.protocol.RowLocatorContractValidator;
import org.pangrova.protocol.v2.ArtifactWorkKind;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.DataSplit;

import java.util.HexFormat;
import java.util.List;

/** Validates durable artifact metadata against the canonical v2 protocol contract. */
@org.pangrova.spi.annotation.Internal
public final class ArtifactDispatchProtocolValidator {

    private ArtifactDispatchProtocolValidator() {}

    /** Validates the actual immutable output identity against its durable job. */
    public static void validateTemplateIdentity(
            ArtifactBuildJob job, ArtifactWriteBinding output) {
        if (!templateIdentityMatches(job, output)) {
            throw new IllegalArgumentException(
                    "artifact binding template differs from durable job identity");
        }
        if (output.hasRowLocator()) {
            RowLocatorContractValidator.validate(output.getRowLocator());
        }
    }

    private static boolean templateIdentityMatches(
            ArtifactBuildJob job, ArtifactWriteBinding output) {
        return coreIdentityMatches(job, output)
                && output.getEncryptionKeyRef().equals(job.encryptionKeyRef())
                && formatMatches(job, output)
                && (output.hasRowLocator() != job.artifactType().equals(
                        org.pangrova.metadata.artifact.contract.ArtifactTypeIds.TABULAR_EXPORT))
                && outputStateIsUnbound(output)
                && output.getExecutionKind() == protocolExecutionKind(job)
                && output.getEmptyPartitionPolicy() == protocolEmptyPolicy(job)
                && dependenciesMatch(job, output);
    }

    private static boolean coreIdentityMatches(
            ArtifactBuildJob job, ArtifactWriteBinding output) {
        return output.getArtifactId().equals(job.artifactId())
                && output.getArtifactType().equals(job.artifactType())
                && output.getGeneration().equals(job.stagingGeneration())
                && output.getSourceId().equals(job.sourceId())
                && output.getSourceSnapshot().equals(job.sourceSnapshot())
                && output.getStagingUri().equals(job.stagingUri());
    }

    private static boolean outputStateIsUnbound(ArtifactWriteBinding output) {
        return output.getExpectedSourceObjectIdsCount() == 0
                && output.getPublicationFencingToken() == 0
                && output.getIdempotencyKey().isEmpty()
                && output.getWorkUnitId().isEmpty()
                && output.getWorkKind() == ArtifactWorkKind.ARTIFACT_WORK_KIND_UNSPECIFIED
                && output.getOutputScopeId().isEmpty();
    }

    private static boolean formatMatches(
            ArtifactBuildJob job, ArtifactWriteBinding output) {
        return output.hasFormat()
                && output.getFormat().getFormatId().equals(job.formatId())
                && output.getFormat().getMajorVersion() == job.formatMajor()
                && output.getFormat().getMinorVersion() == job.formatMinor()
                && HexFormat.of().formatHex(
                        output.getFormat().getSchemaSha256().toByteArray())
                        .equalsIgnoreCase(job.schemaSha256())
                && output.getFormat().getWriterBuildId().equals(job.writerBuildId());
    }

    private static boolean dependenciesMatch(
            ArtifactBuildJob job, ArtifactWriteBinding output) {
        if (output.getDependenciesCount() != job.dependencySet().dependencies().size()
                || !output.hasDependencySetChecksum()
                || !output.getDependencySetChecksum().equals(
                        checksumFromHex(job.dependencySet().sha256()))) {
            return false;
        }
        for (int index = 0; index < output.getDependenciesCount(); index++) {
            var actual = output.getDependencies(index);
            var expected = job.dependencySet().dependencies().get(index);
            if (!actual.getKind().equals(expected.kind())
                    || !actual.getId().equals(expected.id())
                    || !actual.getVersion().equals(expected.version())) {
                return false;
            }
        }
        return true;
    }

    private static Checksum checksumFromHex(String hexadecimal) {
        return Checksum.newBuilder()
                .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                .setDigest(ByteString.copyFrom(HexFormat.of().parseHex(hexadecimal)))
                .build();
    }

    private static org.pangrova.protocol.v2.ArtifactExecutionKind protocolExecutionKind(
            ArtifactBuildJob job) {
        return switch (job.executionKind()) {
            case BUILD -> org.pangrova.protocol.v2.ArtifactExecutionKind
                    .ARTIFACT_EXECUTION_KIND_BUILD;
            case EXPORT -> org.pangrova.protocol.v2.ArtifactExecutionKind
                    .ARTIFACT_EXECUTION_KIND_EXPORT;
        };
    }

    private static org.pangrova.protocol.v2.ArtifactEmptyPartitionPolicy protocolEmptyPolicy(
            ArtifactBuildJob job) {
        return switch (job.emptyPartitionPolicy()) {
            case FORBID -> org.pangrova.protocol.v2.ArtifactEmptyPartitionPolicy
                    .ARTIFACT_EMPTY_PARTITION_POLICY_FORBID;
            case ALLOW_ZERO_ROWS -> org.pangrova.protocol.v2.ArtifactEmptyPartitionPolicy
                    .ARTIFACT_EMPTY_PARTITION_POLICY_ALLOW_ZERO_ROWS;
        };
    }

    /** Validates only the canonical executable stage bytes, never SQL reconstructed during replay. */
    public static void validatePlan(org.pangrova.metadata.artifact.build.ArtifactStageGraph.StagePlan stage) {
        try {
            Plan plan = Plan.parseFrom(stage.planBytes());
            if (!plan.getUnknownFields().asMap().isEmpty()
                    || plan.getRelationsCount() != 1) {
                throw new IllegalArgumentException(
                        "artifact plan is not a single canonical root");
            }
            SubstraitProtocolVersion.validate(plan);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("artifact Substrait plan is malformed", e);
        }
    }

    /** Requires every exact persisted source allocation to match the executable storage contract. */
    public static void validateSplits(
            ArtifactBuildJob job, List<ArtifactBuildSplit> stored) {
        for (ArtifactBuildSplit durable : stored) {
            try {
                DataSplit split = DataSplit.parseFrom(durable.canonicalSplitBytes());
                validateSplitIdentity(job, durable, split);
                DataSplitProtoCodec.fileDetail(split);
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalArgumentException(
                        "durable canonical split is malformed", e);
            }
        }
    }

    private static void validateSplitIdentity(
            ArtifactBuildJob job, ArtifactBuildSplit durable, DataSplit split) {
        if (!split.getUnknownFields().asMap().isEmpty()
                || split.getSplitId().isBlank()
                || split.getConnectorId().isBlank()
                || !split.getSourceId().equals(job.sourceId())
                || !split.getSourceSnapshot().equals(job.sourceSnapshot())
                || split.getOpaqueLocation().isBlank()
                || !split.getDataObjectId().equals(durable.dataObjectId())) {
            throw new IllegalArgumentException(
                    "durable canonical split identity is incomplete or mismatched");
        }
    }

}
