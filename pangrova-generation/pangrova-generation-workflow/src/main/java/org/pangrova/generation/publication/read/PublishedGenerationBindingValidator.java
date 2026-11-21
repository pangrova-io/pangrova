package org.pangrova.generation.publication.read;

import org.pangrova.common.util.DigestUtils;

import org.pangrova.generation.api.output.GenerationOutputRole;

import com.google.protobuf.ByteString;
import org.pangrova.generation.codec.format.BuiltInGenerationOutputFormats;
import org.pangrova.generation.api.output.GenerationOutputFormat;
import org.pangrova.generation.api.publication.GenerationManifest;
import org.pangrova.generation.api.output.GenerationOutput;
import org.pangrova.generation.api.enums.GenerationOutputScope;
import org.pangrova.protocol.v2.ArtifactBinding;
import org.pangrova.protocol.v2.ArtifactObject;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.Checksum;

import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Enforces immutable publication and object-coverage contracts for artifact reads. */
final class PublishedGenerationBindingValidator {

    private PublishedGenerationBindingValidator() {}

    static void validateManifest(
            PublishedGenerationBindingResolver.GenerationResolutionIntent intent,
            GenerationManifest manifest) {
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
            GenerationManifest manifest, ArtifactWriteBinding output) {
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
            GenerationManifest manifest, ArtifactWriteBinding output) {
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
            GenerationManifest manifest, ArtifactWriteBinding output) {
        return output.getArtifactId().equals(manifest.artifactId())
                && output.getArtifactType().equals(manifest.artifactType())
                && output.getGeneration().equals(manifest.generation())
                && output.getSourceId().equals(manifest.sourceId())
                && output.getSourceSnapshot().equals(manifest.sourceSnapshot());
    }

    private static boolean schemaMatches(
            GenerationManifest manifest, ArtifactWriteBinding output) {
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
            GenerationManifest manifest, ArtifactWriteBinding output) {
        return output.getExecutionKindValue() == protocolExecutionKind(manifest)
                && output.getEmptyPartitionPolicyValue() == protocolEmptyPolicy(manifest);
    }

    private static boolean dependenciesMatch(
            GenerationManifest manifest, ArtifactWriteBinding output) {
        if (output.getDependenciesCount() != manifest.dependencySet().dependencies().size()
                || !output.hasDependencySetChecksum()
                || !output.getDependencySetChecksum().equals(
                        sha256(manifest.dependencySet().sha256()))) {
            return false;
        }
        return dependencyValuesMatch(manifest, output.getDependenciesList());
    }

    static void validateTemplateAgainstManifest(
            ArtifactBinding template, GenerationManifest manifest) {
        if (!templateIdentityMatches(template, manifest)
                || !templatePublicationMatches(template, manifest)
                || !templateDependencyMatches(template, manifest)) {
            throw new IllegalStateException(
                    "artifact template changed after its generation was pinned");
        }
    }

    private static boolean templateIdentityMatches(
            ArtifactBinding template, GenerationManifest manifest) {
        return template.getArtifactId().equals(manifest.artifactId())
                && template.getArtifactType().equals(manifest.artifactType())
                && template.getGeneration().equals(manifest.generation())
                && template.getSourceId().equals(manifest.sourceId())
                && template.getSourceSnapshot().equals(manifest.sourceSnapshot());
    }

    private static boolean templatePublicationMatches(
            ArtifactBinding template, GenerationManifest manifest) {
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
            ArtifactBinding template, GenerationManifest manifest) {
        return template.getDependenciesCount()
                        == manifest.dependencySet().dependencies().size()
                && template.hasDependencySetChecksum()
                && template.getDependencySetChecksum().equals(
                        sha256(manifest.dependencySet().sha256()))
                && dependencyValuesMatch(manifest, template.getDependenciesList());
    }

    private static boolean dependencyValuesMatch(
            GenerationManifest manifest,
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
            List<GenerationOutput> outputs) {
        if (outputs.isEmpty()) {
            throw new IllegalStateException(
                    "published artifact has no objects for this task partition");
        }
        ObjectCoverage coverage = collectObjectCoverage(expectedSources, outputs);
        GenerationOutputFormat contract = requireFormatContract(
                template.getFormat().getFormatId(), template.getArtifactType());
        validateSourceBundles(expectedSources, coverage.sourceBundles(), contract);
        validateGenerationBundle(coverage.generationBundle(), contract);
    }

    private static ObjectCoverage collectObjectCoverage(
            Set<String> expectedSources, List<GenerationOutput> outputs) {
        Map<String, Map<String, GenerationOutput>> bundles = new LinkedHashMap<>();
        Map<String, GenerationOutput> generation = new HashMap<>();
        Set<String> objectIds = new HashSet<>();
        for (GenerationOutput output : outputs) {
            validateUniqueObject(expectedSources, bundles, generation, objectIds, output);
        }
        return new ObjectCoverage(bundles, generation);
    }

    private static void validateUniqueObject(
            Set<String> expectedSources,
            Map<String, Map<String, GenerationOutput>> bundles,
            Map<String, GenerationOutput> generation,
            Set<String> objectIds,
            GenerationOutput output) {
        if (!objectIds.add(output.objectId())) {
            throw new IllegalStateException(
                    "artifact object lookup returned an unexpected or duplicate object");
        }
        if (output.scope() == GenerationOutputScope.GENERATION) {
            if (generation.put(output.objectRole(), output) != null) {
                throw new IllegalStateException(
                        "artifact generation contains a duplicate object role");
            }
            return;
        }
        if (output.scope() != GenerationOutputScope.SOURCE_OBJECT
                || !expectedSources.contains(output.sourceObjectId())) {
            throw new IllegalStateException(
                    "artifact object lookup returned an unexpected scope");
        }
        GenerationOutput previous = bundles.computeIfAbsent(
                        output.sourceObjectId(), ignored -> new HashMap<>())
                .put(output.objectRole(), output);
        if (previous != null) {
            throw new IllegalStateException(
                    "artifact source bundle contains a duplicate object role");
        }
    }

    private static void validateSourceBundles(
            Set<String> expectedSources,
            Map<String, Map<String, GenerationOutput>> bundles,
            GenerationOutputFormat contract) {
        if (!bundles.keySet().equals(expectedSources)) {
            throw new IllegalStateException(
                    "artifact object coverage differs from task source objects");
        }
        Map<String, GenerationOutputRole> sourceRoles =
                contract.sourceMapRoles().entrySet().stream()
                        .filter(entry -> entry.getValue().scope()
                                == GenerationOutputScope.SOURCE_OBJECT)
                        .collect(Collectors.toUnmodifiableMap(
                                Map.Entry::getKey, Map.Entry::getValue));
        for (Map<String, GenerationOutput> bundle : bundles.values()) {
            validateSourceBundle(bundle, sourceRoles);
        }
    }

    private static void validateSourceBundle(
            Map<String, GenerationOutput> bundle,
            Map<String, GenerationOutputRole> sourceRoles) {
        if (!bundle.keySet().equals(sourceRoles.keySet())) {
            throw new IllegalStateException("Artifact source bundle roles are incomplete");
        }
        long rows = bundle.values().iterator().next().rowCount();
        for (Map.Entry<String, GenerationOutput> entry : bundle.entrySet()) {
            if (entry.getValue().ordinal() != sourceRoles.get(entry.getKey()).ordinal()
                    || entry.getValue().rowCount() != rows) {
                throw new IllegalStateException(
                        "Artifact source bundle ordinal or row count mismatch");
            }
        }
    }

    private static void validateGenerationBundle(
            Map<String, GenerationOutput> generation,
            GenerationOutputFormat contract) {
        if (!generation.keySet().equals(contract.generationFinalizeRoles().keySet())) {
            throw new IllegalStateException(
                    "Artifact generation bundle roles are incomplete");
        }
        for (Map.Entry<String, GenerationOutput> entry : generation.entrySet()) {
            GenerationOutputRole role =
                    contract.generationFinalizeRoles().get(entry.getKey());
            if (entry.getValue().ordinal() != role.ordinal()
                    || entry.getValue().scope() != role.scope()) {
                throw new IllegalStateException(
                        "Artifact generation role ordinal or scope mismatch");
            }
        }
    }

    static ArtifactObject toProtocolObject(GenerationOutput output) {
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
            GenerationOutputScope scope) {
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
        return DigestUtils.sha256().digest(value);
    }

    static GenerationOutputFormat requireFormatContract(String formatId, String artifactType) {
        return BuiltInGenerationOutputFormats.require(formatId, artifactType);
    }

    private static int protocolExecutionKind(GenerationManifest manifest) {
        return switch (manifest.executionKind()) {
            case BUILD -> org.pangrova.protocol.v2.ArtifactExecutionKind
                    .ARTIFACT_EXECUTION_KIND_BUILD_VALUE;
            case EXPORT -> org.pangrova.protocol.v2.ArtifactExecutionKind
                    .ARTIFACT_EXECUTION_KIND_EXPORT_VALUE;
        };
    }

    private static int protocolEmptyPolicy(GenerationManifest manifest) {
        return switch (manifest.emptyPartitionPolicy()) {
            case FORBID -> org.pangrova.protocol.v2.ArtifactEmptyPartitionPolicy
                    .ARTIFACT_EMPTY_PARTITION_POLICY_FORBID_VALUE;
            case ALLOW_ZERO_ROWS -> org.pangrova.protocol.v2.ArtifactEmptyPartitionPolicy
                    .ARTIFACT_EMPTY_PARTITION_POLICY_ALLOW_ZERO_ROWS_VALUE;
        };
    }

    private record ObjectCoverage(
            Map<String, Map<String, GenerationOutput>> sourceBundles,
            Map<String, GenerationOutput> generationBundle) {}
}
