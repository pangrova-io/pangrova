package org.pangrova.coordinator.databuild.template;

import com.google.protobuf.ByteString;
import org.pangrova.databuild.api.build.DataBuildDispatchSpec;
import org.pangrova.databuild.api.build.DataBuildJob;
import org.pangrova.databuild.api.build.DataBuildLease;
import org.pangrova.databuild.api.build.DataBuildOutput;
import org.pangrova.databuild.api.DataBuildOutputScope;
import org.pangrova.databuild.api.DataBuildPageKind;
import org.pangrova.databuild.spi.storage.DataBuildStageExecutionStore.PreparedPartition;
import org.pangrova.coordinator.databuild.DataBuildDispatchProtocolValidator;
import org.pangrova.coordinator.databuild.PublishedGenerationBindingResolver;
import org.pangrova.coordinator.query.execution.dispatch.dag.binding.DataBuildPartitionResources;
import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.protocol.databuild.PublishedGenerationBindingChecksum;
import org.pangrova.protocol.v2.ArtifactBinding;
import org.pangrova.protocol.v2.ArtifactObject;
import org.pangrova.protocol.v2.ArtifactObjectScope;
import org.pangrova.protocol.v2.ArtifactWorkKind;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.CoveragePolicy;
import org.pangrova.protocol.v2.ExecutionPolicy;
import org.pangrova.protocol.v2.FallbackPolicy;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorResourceRequirement;
import org.pangrova.protocol.v2.RetryClass;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Pure exact-partition binding of a frozen plan using evidence read on the claim connection. */
@org.pangrova.common.api.annotation.Internal
public final class DataBuildPartitionBindings {
    private DataBuildPartitionBindings() {}

    /** Does not borrow a connection, discover a model, resolve a publication, or perform any network I/O. */
    public static BoundExecutionPlan bind(PreparedPartition prepared, Instant now) {
        var lease = prepared.lease();
        var job = prepared.job();
        var spec = prepared.authority().dispatchSpec();
        var stage = spec.stageGraph().requireStage(lease.stageId());
        if (stage.pageKind() != lease.kind() || lease.partitionId() >= stage.parallelism()) {
            throw new IllegalArgumentException("claimed page differs from its persisted stage contract");
        }
        DataBuildDispatchProtocolValidator.validatePlan(stage);
        DataBuildDispatchProtocolValidator.validateSplits(job, lease.splits());
        var frozen = FrozenStageBindings.read(stage, spec.stageGraph().modelSnapshots());
        var materialized = PublishedGenerationBindingResolver.materializePrepared(frozen.operatorBindings(),
                lease.splits().stream().map(split -> java.util.Base64.getEncoder()
                        .encodeToString(split.canonicalSplitBytes())).toList(), prepared.publications());
        List<OperatorBinding> bindings = bindAttempt(job, spec, lease, now,
                DataBuildPartitionResources.bind(materialized), prepared.dependencyOutputs());
        return new BoundExecutionPlan(frozen.planBytes(), bindings, frozen.resultColumnTypes());
    }

    private static List<OperatorBinding> bindAttempt(
            DataBuildJob job, DataBuildDispatchSpec spec,
            DataBuildLease lease, Instant now, List<OperatorBinding> templates,
            List<DataBuildOutput> dependencies) {
        // Database renewals do not renew credentials already dispatched to Native.
        long deadline = Math.min(lease.expiresAt().toEpochMilli(),
                Math.addExact(now.toEpochMilli(), spec.taskTimeoutMillis()));
        if (deadline <= now.toEpochMilli()) {
            throw new IllegalStateException("artifact execution lease expired before dispatch");
        }
        List<OperatorBinding> bound = new ArrayList<>(templates.size());
        int outputCount = 0;
        for (OperatorBinding template : templates) {
            if (template.hasOutputArtifact()) {
                outputCount++;
                bound.add(bindOutputAttempt(job, spec, template, lease, deadline, dependencies));
            } else {
                bound.add(bindModelAttempt(job, template, lease, deadline));
            }
        }
        int expectedOutputs = lease.kind() == DataBuildPageKind.RELATIONAL_COMPUTE ? 0 : 1;
        if (outputCount != expectedOutputs) {
            throw new IllegalArgumentException(
                    "artifact binding output count differs from its immutable stage purpose");
        }
        return List.copyOf(bound);
    }

    private static OperatorBinding bindOutputAttempt(
            DataBuildJob job, DataBuildDispatchSpec spec,
            OperatorBinding template,
            DataBuildLease lease, long deadline, List<DataBuildOutput> dependencies) {
        ArtifactWriteBinding output = template.getOutputArtifact();
        if (template.hasModel()) {
            throw new IllegalArgumentException("Artifact output cannot also invoke a model");
        }
        DataBuildDispatchProtocolValidator.validateTemplateIdentity(job, output);
        String idempotencyKey = idempotencyKey(job, lease, template);
        ArtifactWriteBinding boundOutput = output.toBuilder()
                .clearExpectedSourceObjectIds()
                .addAllExpectedSourceObjectIds(lease.kind() == DataBuildPageKind.RELATIONAL_PARTITION
                        ? List.of() : lease.sourceObjectIds())
                .setPublicationFencingToken(lease.fencingToken())
                .setIdempotencyKey(idempotencyKey)
                .setWorkUnitId(lease.pageId())
                .setWorkKind(toProtocolKind(lease.kind()))
                .setOutputScopeId(outputScopeId(job, lease))
                .build();
        OperatorBinding.Builder binding = template.toBuilder()
                .setResources(template.hasResources() ? template.getResources()
                        : OperatorResourceRequirement.newBuilder()
                        .setObjectStoreReadBytes(spec.maxMemoryBytes())
                        .setObjectStoreWriteBytes(spec.maxSpillBytes()).build())
                .setExecutionPolicy(template.hasExecutionPolicy()
                        ? template.getExecutionPolicy().toBuilder().setDeadlineEpochMs(deadline)
                                .setIdempotencyKey(idempotencyKey).build()
                        : executionPolicy(deadline, idempotencyKey, RetryClass.RETRY_CLASS_IDEMPOTENT))
                .setOutputArtifact(boundOutput);
        if (!lease.dependencyPageIds().isEmpty()
                && lease.kind() != DataBuildPageKind.RELATIONAL_PARTITION) {
            binding.addArtifacts(dependencyBinding(job, output, lease, dependencies));
        }
        return binding.build();
    }

    private static OperatorBinding bindModelAttempt(
            DataBuildJob job, OperatorBinding template,
            DataBuildLease lease, long deadline) {
        String idempotencyKey = idempotencyKey(job, lease, template);
        var policy = template.hasExecutionPolicy() ? template.getExecutionPolicy().toBuilder()
                : executionPolicy(deadline, idempotencyKey,
                        RetryClass.RETRY_CLASS_PROVIDER_IDEMPOTENCY_KEY).toBuilder();
        return template.toBuilder().setExecutionPolicy(policy.setDeadlineEpochMs(deadline)
                .setIdempotencyKey(idempotencyKey)).build();
    }

    private static ExecutionPolicy executionPolicy(
            long deadline, String idempotencyKey, RetryClass retryClass) {
        return ExecutionPolicy.newBuilder()
                .setCoveragePolicy(CoveragePolicy.COVERAGE_POLICY_REQUIRE_COMPLETE)
                .setFallbackPolicy(FallbackPolicy.FALLBACK_POLICY_FORBID)
                .setRetryClass(retryClass)
                .setDeadlineEpochMs(deadline)
                .setIdempotencyKey(idempotencyKey)
                .build();
    }

    private static String idempotencyKey(
            DataBuildJob job, DataBuildLease lease,
            OperatorBinding template) {
        return job.idempotencyKey() + ":" + lease.pageId()
                + ":" + template.getPlanNodeId();
    }

    private static ArtifactBinding dependencyBinding(
            DataBuildJob job, ArtifactWriteBinding output,
            DataBuildLease lease, List<DataBuildOutput> dependencies) {
        List<DataBuildOutput> inputs = dependencies.stream()
                .filter(object -> object.scope() == DataBuildOutputScope.WORK_UNIT)
                .toList();
        Set<String> inputPages = inputs.stream()
                .map(DataBuildOutput::sourceObjectId)
                .collect(Collectors.toUnmodifiableSet());
        if (!inputPages.equals(Set.copyOf(lease.dependencyPageIds()))) {
            throw new IllegalStateException(
                    "artifact reduction inputs do not cover every dependency page");
        }
        String digest = dependencyDigest(inputs);
        ArtifactBinding.Builder builder = dependencyBindingBuilder(job, output, digest);
        inputs.stream()
                .sorted(Comparator.comparing(DataBuildOutput::sourceObjectId)
                        .thenComparingInt(DataBuildOutput::ordinal))
                .map(DataBuildPartitionBindings::toProtocolObject)
                .forEach(builder::addSplitObjects);
        return PublishedGenerationBindingChecksum.sign(builder.build());
    }

    private static ArtifactBinding.Builder dependencyBindingBuilder(
            DataBuildJob job, ArtifactWriteBinding output, String digest) {
        return ArtifactBinding.newBuilder()
                .setArtifactId(job.artifactId())
                .setArtifactType(job.artifactType())
                .setGeneration(job.stagingGeneration())
                .setSourceId(job.sourceId())
                .setSourceSnapshot(job.sourceSnapshot())
                .setArtifactCreatedAtEpochMs(job.createdAt().toEpochMilli())
                .setReadEpoch(digest)
                .setManifestUri("metadata://artifact-work-input/" + digest)
                .setManifestChecksum(checksumFromHex(digest))
                .setFormat(output.getFormat())
                .addAllProperties(output.getPropertiesList())
                .setRowLocator(output.getRowLocator())
                .addAllDependencies(output.getDependenciesList())
                .setDependencySetChecksum(output.getDependencySetChecksum())
                .setExecutionKind(output.getExecutionKind())
                .setEmptyPartitionPolicy(output.getEmptyPartitionPolicy());
    }

    private static String outputScopeId(
            DataBuildJob job, DataBuildLease lease) {
        return switch (lease.kind()) {
            case SOURCE_MAP -> "";
            case DEPENDENCY_REDUCE -> lease.pageId();
            case GENERATION_FINALIZE -> job.stagingGeneration();
            case RELATIONAL_PARTITION -> lease.pageId();
            case RELATIONAL_COMPUTE -> throw new IllegalArgumentException(
                    "compute stages have no artifact output scope");
        };
    }

    private static ArtifactWorkKind toProtocolKind(DataBuildPageKind kind) {
        return switch (kind) {
            case SOURCE_MAP -> ArtifactWorkKind.ARTIFACT_WORK_KIND_SOURCE_MAP;
            case DEPENDENCY_REDUCE ->
                    ArtifactWorkKind.ARTIFACT_WORK_KIND_DEPENDENCY_REDUCE;
            case GENERATION_FINALIZE ->
                    ArtifactWorkKind.ARTIFACT_WORK_KIND_GENERATION_FINALIZE;
            case RELATIONAL_PARTITION -> ArtifactWorkKind.ARTIFACT_WORK_KIND_RELATIONAL_PARTITION;
            case RELATIONAL_COMPUTE -> throw new IllegalArgumentException("compute stages have no artifact work kind");
        };
    }

    private static String dependencyDigest(List<DataBuildOutput> inputs) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            inputs.stream()
                    .sorted(Comparator.comparing(DataBuildOutput::sourceObjectId)
                            .thenComparingInt(DataBuildOutput::ordinal))
                    .map(DataBuildPartitionBindings::toProtocolObject)
                    .map(ArtifactObject::toByteArray)
                    .forEach(bytes -> updateDigest(digest, bytes));
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static void updateDigest(MessageDigest digest, byte[] bytes) {
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private static ArtifactObject toProtocolObject(DataBuildOutput output) {
        return ArtifactObject.newBuilder()
                .setObjectId(output.objectId())
                .setOpaqueUri(output.opaqueUri())
                .setSizeBytes(output.sizeBytes())
                .setChecksum(checksumFromHex(output.checksumSha256()))
                .setEncryptionKeyRef(output.encryptionKeyRef())
                .setObjectRole(output.objectRole())
                .setSourceObjectId(output.sourceObjectId())
                .setRowCount(output.rowCount())
                .setOrdinal(output.ordinal())
                .setScope(protocolScope(output.scope()))
                .build();
    }

    private static ArtifactObjectScope protocolScope(DataBuildOutputScope scope) {
        return switch (scope) {
            case SOURCE_OBJECT -> ArtifactObjectScope.ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT;
            case WORK_UNIT -> ArtifactObjectScope.ARTIFACT_OBJECT_SCOPE_WORK_UNIT;
            case GENERATION -> ArtifactObjectScope.ARTIFACT_OBJECT_SCOPE_GENERATION;
        };
    }

    private static Checksum checksumFromHex(String hexadecimal) {
        return Checksum.newBuilder()
                .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                .setDigest(ByteString.copyFrom(HexFormat.of().parseHex(hexadecimal)))
                .build();
    }

}
