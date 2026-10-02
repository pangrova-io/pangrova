package org.pangrova.coordinator.artifact;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.metadata.artifact.contract.ArtifactFormatContracts;
import org.pangrova.metadata.artifact.contract.ArtifactFormatContracts.FormatContract;
import org.pangrova.metadata.artifact.ArtifactGenerationManifest;
import org.pangrova.metadata.artifact.build.ArtifactBuildOutput;
import org.pangrova.metadata.artifact.contract.ArtifactBuildOutputScope;
import org.pangrova.protocol.v2.ArtifactBinding;
import org.pangrova.protocol.v2.ArtifactObject;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.DataSplit;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Enforces immutable publication and object-coverage contracts for artifact reads. */
final class PublishedArtifactBindingContract {

    private PublishedArtifactBindingContract() {}

    static void validateManifest(
            PublishedArtifactBindingResolver.ArtifactResolutionIntent intent,
            ArtifactGenerationManifest manifest) {
        if (!manifest.artifactId().equals(intent.artifactId())
                || !manifest.tenantId().equals(intent.tenantId())
                || !manifest.artifactType().equals(intent.artifactType())
                || !manifest.generation().equals(intent.generation())
                || !manifest.sourceId().equals(intent.sourceId())
                || !manifest.sourceSnapshot().equals(intent.sourceSnapshot())) {
            throw new IllegalStateException(
                    "published artifact identity differs from the query intent");
        }
    }

    static void validateWriteTemplate(
            ArtifactGenerationManifest manifest, ArtifactWriteBinding output) {
        if (!writeIdentityMatches(manifest, output)
                || !writeExecutionMatches(manifest, output)
                || !dependenciesMatch(manifest, output)
                || !output.hasRowLocator()
                || !outputStateIsUnbound(output)) {
            throw new IllegalStateException(
                    "published artifact format or source contract differs from its build template");
        }
    }

    private static boolean writeIdentityMatches(
            ArtifactGenerationManifest manifest, ArtifactWriteBinding output) {
        return writeCoreIdentityMatches(manifest, output)
                && output.hasFormat()
                && output.getFormat().getFormatId().equals(manifest.formatId())
                && output.getFormat().getMajorVersion() == manifest.formatMajor()
                && output.getFormat().getMinorVersion() == manifest.formatMinor()
                && schemaMatches(manifest, output)
                && output.getFormat().getWriterBuildId()
                        .equals(manifest.writerBuildId());
    }

    private static boolean writeCoreIdentityMatches(
            ArtifactGenerationManifest manifest, ArtifactWriteBinding output) {
        return output.getArtifactId().equals(manifest.artifactId())
                && output.getArtifactType().equals(manifest.artifactType())
                && output.getGeneration().equals(manifest.generation())
                && output.getSourceId().equals(manifest.sourceId())
                && output.getSourceSnapshot().equals(manifest.sourceSnapshot());
    }

    private static boolean schemaMatches(
            ArtifactGenerationManifest manifest, ArtifactWriteBinding output) {
        return HexFormat.of().formatHex(
                output.getFormat().getSchemaSha256().toByteArray())
                .equalsIgnoreCase(manifest.schemaSha256());
    }

    private static boolean outputStateIsUnbound(ArtifactWriteBinding output) {
        return output.getExpectedSourceObjectIdsCount() == 0
                && output.getPublicationFencingToken() == 0
                && output.getIdempotencyKey().isEmpty()
                && output.getWorkUnitId().isEmpty()
                && output.getWorkKindValue() == 0
                && output.getOutputScopeId().isEmpty();
    }

    private static boolean writeExecutionMatches(
            ArtifactGenerationManifest manifest, ArtifactWriteBinding output) {
        return output.getExecutionKindValue() == protocolExecutionKind(manifest)
                && output.getEmptyPartitionPolicyValue() == protocolEmptyPolicy(manifest);
    }

    private static boolean dependenciesMatch(
            ArtifactGenerationManifest manifest, ArtifactWriteBinding output) {
        if (output.getDependenciesCount() != manifest.dependencySet().dependencies().size()
                || !output.hasDependencySetChecksum()
                || !output.getDependencySetChecksum().equals(
                        sha256(manifest.dependencySet().sha256()))) {
            return false;
        }
        return dependencyValuesMatch(manifest, output.getDependenciesList());
    }

    static void validateTemplateAgainstManifest(
            ArtifactBinding template, ArtifactGenerationManifest manifest) {
        if (!templateIdentityMatches(template, manifest)
                || !templatePublicationMatches(template, manifest)
                || !templateDependencyMatches(template, manifest)) {
            throw new IllegalStateException(
                    "artifact template changed after its generation was pinned");
        }
    }

    private static boolean templateIdentityMatches(
            ArtifactBinding template, ArtifactGenerationManifest manifest) {
        return template.getArtifactId().equals(manifest.artifactId())
                && template.getArtifactType().equals(manifest.artifactType())
                && template.getGeneration().equals(manifest.generation())
                && template.getSourceId().equals(manifest.sourceId())
                && template.getSourceSnapshot().equals(manifest.sourceSnapshot());
    }

    private static boolean templatePublicationMatches(
            ArtifactBinding template, ArtifactGenerationManifest manifest) {
        return template.getManifestUri().equals(manifest.manifestUri())
                && template.hasManifestChecksum()
                && template.getManifestChecksum().equals(
                        sha256(manifest.manifestSha256()))
                && template.getArtifactCreatedAtEpochMs()
                        == manifest.publishedAt().toEpochMilli()
                && template.getReadEpoch().equals(manifest.manifestSha256())
                && template.getExecutionKindValue() == protocolExecutionKind(manifest)
                && template.getEmptyPartitionPolicyValue()
                        == protocolEmptyPolicy(manifest);
    }

    private static boolean templateDependencyMatches(
            ArtifactBinding template, ArtifactGenerationManifest manifest) {
        return template.getDependenciesCount()
                        == manifest.dependencySet().dependencies().size()
                && template.hasDependencySetChecksum()
                && template.getDependencySetChecksum().equals(
                        sha256(manifest.dependencySet().sha256()))
                && dependencyValuesMatch(manifest, template.getDependenciesList());
    }

    private static boolean dependencyValuesMatch(
            ArtifactGenerationManifest manifest,
            List<org.pangrova.protocol.v2.ArtifactDependency> actual) {
        if (actual.size() != manifest.dependencySet().dependencies().size()) {
            return false;
        }
        for (int index = 0; index < actual.size(); index++) {
            var expected = manifest.dependencySet().dependencies().get(index);
            if (!actual.get(index).getKind().equals(expected.kind())
                    || !actual.get(index).getId().equals(expected.id())
                    || !actual.get(index).getVersion().equals(expected.version())) {
                return false;
            }
        }
        return true;
    }

    static void validateObjectCoverage(
            ArtifactBinding template, Set<String> expectedSources,
            List<ArtifactBuildOutput> outputs) {
        if (outputs.isEmpty()) {
            throw new IllegalStateException(
                    "published artifact has no objects for this task partition");
        }
        ObjectCoverage coverage = collectObjectCoverage(expectedSources, outputs);
        FormatContract contract = requireFormatContract(
                template.getFormat().getFormatId(), template.getArtifactType());
        validateSourceBundles(expectedSources, coverage.sourceBundles(), contract);
        validateGenerationBundle(coverage.generationBundle(), contract);
    }

    private static ObjectCoverage collectObjectCoverage(
            Set<String> expectedSources, List<ArtifactBuildOutput> outputs) {
        Map<String, Map<String, ArtifactBuildOutput>> bundles = new LinkedHashMap<>();
        Map<String, ArtifactBuildOutput> generation = new HashMap<>();
        Set<String> objectIds = new HashSet<>();
        for (ArtifactBuildOutput output : outputs) {
            validateUniqueObject(expectedSources, bundles, generation, objectIds, output);
        }
        return new ObjectCoverage(bundles, generation);
    }

    private static void validateUniqueObject(
            Set<String> expectedSources,
            Map<String, Map<String, ArtifactBuildOutput>> bundles,
            Map<String, ArtifactBuildOutput> generation,
            Set<String> objectIds,
            ArtifactBuildOutput output) {
        if (!objectIds.add(output.objectId())) {
            throw new IllegalStateException(
                    "artifact object lookup returned an unexpected or duplicate object");
        }
        if (output.scope() == ArtifactBuildOutputScope.GENERATION) {
            if (generation.put(output.objectRole(), output) != null) {
                throw new IllegalStateException(
                        "artifact generation contains a duplicate object role");
            }
            return;
        }
        if (output.scope() != ArtifactBuildOutputScope.SOURCE_OBJECT
                || !expectedSources.contains(output.sourceObjectId())) {
            throw new IllegalStateException(
                    "artifact object lookup returned an unexpected scope");
        }
        ArtifactBuildOutput previous = bundles.computeIfAbsent(
                        output.sourceObjectId(), ignored -> new HashMap<>())
                .put(output.objectRole(), output);
        if (previous != null) {
            throw new IllegalStateException(
                    "artifact source bundle contains a duplicate object role");
        }
    }

    private static void validateSourceBundles(
            Set<String> expectedSources,
            Map<String, Map<String, ArtifactBuildOutput>> bundles,
            FormatContract contract) {
        if (!bundles.keySet().equals(expectedSources)) {
            throw new IllegalStateException(
                    "artifact object coverage differs from task source objects");
        }
        Map<String, ArtifactFormatContracts.RoleContract> sourceRoles =
                contract.sourceMapRoles().entrySet().stream()
                        .filter(entry -> entry.getValue().scope()
                                == ArtifactBuildOutputScope.SOURCE_OBJECT)
                        .collect(Collectors.toUnmodifiableMap(
                                Map.Entry::getKey, Map.Entry::getValue));
        for (Map<String, ArtifactBuildOutput> bundle : bundles.values()) {
            validateSourceBundle(bundle, sourceRoles);
        }
    }

    private static void validateSourceBundle(
            Map<String, ArtifactBuildOutput> bundle,
            Map<String, ArtifactFormatContracts.RoleContract> sourceRoles) {
        if (!bundle.keySet().equals(sourceRoles.keySet())) {
            throw new IllegalStateException("Artifact source bundle roles are incomplete");
        }
        long rows = bundle.values().iterator().next().rowCount();
        for (Map.Entry<String, ArtifactBuildOutput> entry : bundle.entrySet()) {
            if (entry.getValue().ordinal() != sourceRoles.get(entry.getKey()).ordinal()
                    || entry.getValue().rowCount() != rows) {
                throw new IllegalStateException(
                        "Artifact source bundle ordinal or row count mismatch");
            }
        }
    }

    private static void validateGenerationBundle(
            Map<String, ArtifactBuildOutput> generation,
            FormatContract contract) {
        if (!generation.keySet().equals(contract.generationFinalizeRoles().keySet())) {
            throw new IllegalStateException(
                    "Artifact generation bundle roles are incomplete");
        }
        for (Map.Entry<String, ArtifactBuildOutput> entry : generation.entrySet()) {
            ArtifactFormatContracts.RoleContract role =
                    contract.generationFinalizeRoles().get(entry.getKey());
            if (entry.getValue().ordinal() != role.ordinal()
                    || entry.getValue().scope() != role.scope()) {
                throw new IllegalStateException(
                        "Artifact generation role ordinal or scope mismatch");
            }
        }
    }

    static ArtifactObject toProtocolObject(ArtifactBuildOutput output) {
        return ArtifactObject.newBuilder()
                .setObjectId(output.objectId())
                .setOpaqueUri(output.opaqueUri())
                .setSizeBytes(output.sizeBytes())
                .setChecksum(sha256(output.checksumSha256()))
                .setEncryptionKeyRef(output.encryptionKeyRef())
                .setObjectRole(output.objectRole())
                .setSourceObjectId(output.sourceObjectId())
                .setRowCount(output.rowCount())
                .setOrdinal(output.ordinal())
                .setScope(protocolScope(output.scope()))
                .build();
    }

    private static org.pangrova.protocol.v2.ArtifactObjectScope protocolScope(
            ArtifactBuildOutputScope scope) {
        return switch (scope) {
            case SOURCE_OBJECT -> org.pangrova.protocol.v2.ArtifactObjectScope
                    .ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT;
            case WORK_UNIT -> org.pangrova.protocol.v2.ArtifactObjectScope
                    .ARTIFACT_OBJECT_SCOPE_WORK_UNIT;
            case GENERATION -> org.pangrova.protocol.v2.ArtifactObjectScope
                    .ARTIFACT_OBJECT_SCOPE_GENERATION;
        };
    }

    static Checksum sha256(String hexadecimal) {
        try {
            byte[] digest = HexFormat.of().parseHex(hexadecimal);
            if (digest.length != 32) {
                throw new IllegalArgumentException("SHA-256 digest must be 32 bytes");
            }
            return Checksum.newBuilder()
                    .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                    .setDigest(ByteString.copyFrom(digest))
                    .build();
        } catch (IllegalArgumentException failure) {
            throw new IllegalStateException("invalid artifact SHA-256 identity", failure);
        }
    }

    static byte[] sha256Bytes(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    static List<DataSplit> parseSplits(List<String> serializedSplits) {
        if (serializedSplits == null || serializedSplits.isEmpty()) {
            return List.of();
        }
        return serializedSplits.stream()
                .map(PublishedArtifactBindingContract::parseSplit)
                .toList();
    }

    private static DataSplit parseSplit(String encoded) {
        try {
            DataSplit split = DataSplit.parseFrom(Base64.getDecoder().decode(encoded));
            validateSplit(split);
            return split;
        } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
            throw new IllegalArgumentException(
                    "cannot parse split for artifact resolution", failure);
        }
    }

    private static void validateSplit(DataSplit split) {
        if (!split.getUnknownFields().asMap().isEmpty()
                || split.getSplitId().isBlank()
                || split.getSourceId().isBlank()
                || split.getSourceSnapshot().isBlank()
                || split.getDataObjectId().isBlank()) {
            throw new IllegalArgumentException(
                    "artifact resolution requires a canonical split identity");
        }
    }

    static FormatContract requireFormatContract(String formatId, String artifactType) {
        return ArtifactFormatContracts.require(formatId, artifactType);
    }

    private static int protocolExecutionKind(ArtifactGenerationManifest manifest) {
        return switch (manifest.executionKind()) {
            case BUILD -> org.pangrova.protocol.v2.ArtifactExecutionKind
                    .ARTIFACT_EXECUTION_KIND_BUILD_VALUE;
            case EXPORT -> org.pangrova.protocol.v2.ArtifactExecutionKind
                    .ARTIFACT_EXECUTION_KIND_EXPORT_VALUE;
        };
    }

    private static int protocolEmptyPolicy(ArtifactGenerationManifest manifest) {
        return switch (manifest.emptyPartitionPolicy()) {
            case FORBID -> org.pangrova.protocol.v2.ArtifactEmptyPartitionPolicy
                    .ARTIFACT_EMPTY_PARTITION_POLICY_FORBID_VALUE;
            case ALLOW_ZERO_ROWS -> org.pangrova.protocol.v2.ArtifactEmptyPartitionPolicy
                    .ARTIFACT_EMPTY_PARTITION_POLICY_ALLOW_ZERO_ROWS_VALUE;
        };
    }

    private record ObjectCoverage(
            Map<String, Map<String, ArtifactBuildOutput>> sourceBundles,
            Map<String, ArtifactBuildOutput> generationBundle) {}
}
