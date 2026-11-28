package org.pangrova.generation.execution.binding;

import org.pangrova.common.crypto.Sha256Digests;

import org.pangrova.generation.execution.template.FrozenStageBindings;

import com.google.protobuf.ByteString;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.page.GenerationLease;
import org.pangrova.generation.api.output.GenerationOutput;
import org.pangrova.generation.api.enums.GenerationOutputScope;
import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.generation.api.graph.GenerationStageGraph.StagePlan;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore.PreparedPartition;
import org.pangrova.generation.execution.validation.GenerationDispatchProtocolValidator;
import org.pangrova.generation.publication.read.PublishedGenerationBindingResolver;
import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.protocol.generation.PublishedGenerationBindingChecksum;
import org.pangrova.protocol.v2.ArtifactBinding;
import org.pangrova.protocol.v2.ArtifactObject;
import org.pangrova.protocol.v2.ArtifactObjectScope;
import org.pangrova.protocol.v2.ArtifactWorkKind;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.CoveragePolicy;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.ExecutionPolicy;
import org.pangrova.protocol.v2.FallbackPolicy;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorResourceRequirement;
import org.pangrova.protocol.v2.RetryClass;
import org.pangrova.protocol.v2.ResultColumnLogicalType;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.ModelSnapshot;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Pure exact-partition binding of a frozen plan using evidence read on the claim connection. */
@org.pangrova.common.api.annotation.Internal
public final class GenerationPartitionBindings {
    private GenerationPartitionBindings() {}

    public record BoundPartition(List<OperatorBinding> operatorBindings, List<DataSplit> splits) {
        public BoundPartition {
            operatorBindings = List.copyOf(operatorBindings);
            splits = List.copyOf(splits);
        }
    }

    /** Canonically decoded and validated stage content shared across exact partition claims. */
    public static final class StageBindings {
        private final StagePlan stage;
        private final List<ModelSnapshot> models;
        private final BoundExecutionPlan templates;

        /** Decodes one persisted stage against its captured model declarations. */
        public StageBindings(StagePlan stage, List<ModelSnapshot> models) {
            this.stage = stage;
            this.models = List.copyOf(models);
            templates = FrozenStageBindings.read(stage, this.models);
            GenerationDispatchProtocolValidator.validatePlan(templates.planBytes());
        }

        public byte[] planBytes() {
            return templates.planBytes();
        }

        public List<ResultColumnLogicalType> resultColumnTypes() {
            return templates.resultColumnTypes();
        }

        List<OperatorBinding> requireStage(StagePlan current, List<ModelSnapshot> currentModels) {
            if (!stage.equals(current) || !models.equals(currentModels)) {
                throw new IllegalArgumentException("Partition authority differs from its validated stage contract");
            }
            return templates.operatorBindings();
        }
    }

    /** Does not borrow a connection, discover a model, resolve a publication, or perform any network I/O. */
    public static BoundPartition bind(PreparedPartition prepared, StageBindings frozen, Instant now) {
        var lease = prepared.lease();
        var job = prepared.job();
        var spec = prepared.authority().dispatchSpec();
        var stage = spec.stageGraph().requireStage(lease.stageId());
        if (stage.pageKind() != lease.kind() || lease.partitionId() >= stage.parallelism()) {
            throw new IllegalArgumentException("claimed page differs from its persisted stage contract");
        }
        var templates = frozen.requireStage(stage, spec.stageGraph().modelSnapshots());
        var splits = GenerationDispatchProtocolValidator.parseSplits(job, lease.splits());
        var materialized = PublishedGenerationBindingResolver.materializePrepared(templates,
                splits, prepared.publications());
        List<OperatorBinding> bindings = bindAttempt(job, spec, lease, now,
                GenerationPartitionResources.bind(materialized), prepared.dependencyOutputs());
        return new BoundPartition(bindings, splits);
    }

    private static List<OperatorBinding> bindAttempt(
            GenerationJob job, GenerationDispatchSpec spec,
            GenerationLease lease, Instant now, List<OperatorBinding> templates,
            List<GenerationOutput> dependencies) {
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
        int expectedOutputs = lease.kind() == GenerationPageKind.RELATIONAL_COMPUTE ? 0 : 1;
        if (outputCount != expectedOutputs) {
            throw new IllegalArgumentException(
                    "artifact binding output count differs from its immutable stage purpose");
        }
        return List.copyOf(bound);
    }

    private static OperatorBinding bindOutputAttempt(
            GenerationJob job, GenerationDispatchSpec spec,
            OperatorBinding template,
            GenerationLease lease, long deadline, List<GenerationOutput> dependencies) {
        ArtifactWriteBinding output = template.getOutputArtifact();
        if (template.hasModel()) {
            throw new IllegalArgumentException("Artifact output cannot also invoke a model");
        }
        GenerationDispatchProtocolValidator.validateTemplateIdentity(job, output);
        String idempotencyKey = idempotencyKey(job, lease, template);
        ArtifactWriteBinding boundOutput = output.toBuilder()
                .clearExpectedSourceObjectIds()
                .addAllExpectedSourceObjectIds(lease.kind() == GenerationPageKind.RELATIONAL_PARTITION
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
                && lease.kind() != GenerationPageKind.RELATIONAL_PARTITION) {
            binding.addArtifacts(dependencyBinding(job, output, lease, dependencies));
        }
        return binding.build();
    }

    private static OperatorBinding bindModelAttempt(
            GenerationJob job, OperatorBinding template,
            GenerationLease lease, long deadline) {
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
            GenerationJob job, GenerationLease lease,
            OperatorBinding template) {
        return job.idempotencyKey() + ":" + lease.pageId()
                + ":" + template.getPlanNodeId();
    }

    private static ArtifactBinding dependencyBinding(
            GenerationJob job, ArtifactWriteBinding output,
            GenerationLease lease, List<GenerationOutput> dependencies) {
        List<GenerationOutput> inputs = dependencies.stream()
                .filter(object -> object.scope() == GenerationOutputScope.WORK_UNIT)
                .toList();
        Set<String> inputPages = inputs.stream()
                .map(GenerationOutput::sourceObjectId)
                .collect(Collectors.toUnmodifiableSet());
        if (!inputPages.equals(Set.copyOf(lease.dependencyPageIds()))) {
            throw new IllegalStateException(
                    "artifact reduction inputs do not cover every dependency page");
        }
        String digest = dependencyDigest(inputs);
        ArtifactBinding.Builder builder = dependencyBindingBuilder(job, output, digest);
        inputs.stream()
                .sorted(Comparator.comparing(GenerationOutput::sourceObjectId)
                        .thenComparingInt(GenerationOutput::ordinal))
                .map(GenerationPartitionBindings::toProtocolObject)
                .forEach(builder::addSplitObjects);
        return PublishedGenerationBindingChecksum.sign(builder.build());
    }

    private static ArtifactBinding.Builder dependencyBindingBuilder(
            GenerationJob job, ArtifactWriteBinding output, String digest) {
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
            GenerationJob job, GenerationLease lease) {
        return switch (lease.kind()) {
            case SOURCE_MAP -> "";
            case DEPENDENCY_REDUCE -> lease.pageId();
            case GENERATION_FINALIZE -> job.stagingGeneration();
            case RELATIONAL_PARTITION -> lease.pageId();
            case RELATIONAL_COMPUTE -> throw new IllegalArgumentException(
                    "compute stages have no artifact output scope");
        };
    }

    private static ArtifactWorkKind toProtocolKind(GenerationPageKind kind) {
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

    private static String dependencyDigest(List<GenerationOutput> inputs) {
        MessageDigest digest = Sha256Digests.sha256();
        inputs.stream()
                .sorted(Comparator.comparing(GenerationOutput::sourceObjectId)
                        .thenComparingInt(GenerationOutput::ordinal))
                .map(GenerationPartitionBindings::toProtocolObject)
                .map(ArtifactObject::toByteArray)
                .forEach(bytes -> updateDigest(digest, bytes));
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void updateDigest(MessageDigest digest, byte[] bytes) {
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private static ArtifactObject toProtocolObject(GenerationOutput output) {
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

    private static ArtifactObjectScope protocolScope(GenerationOutputScope scope) {
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
