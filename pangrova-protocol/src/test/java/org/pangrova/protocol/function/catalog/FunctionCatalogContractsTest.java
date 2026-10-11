package org.pangrova.protocol.function.catalog;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.CanonicalCapabilityInventoryTestFixture;
import org.pangrova.protocol.CapabilityInventoryCodec;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.protocol.v2.FunctionAvailability;
import org.pangrova.protocol.v2.FunctionCatalogManifest;
import org.pangrova.protocol.v2.FunctionImplementationFamily;

import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FunctionCatalogContractsTest {
    @Test
    void registrationCodecCannotBypassActualFunctionAdmission() {
        var actual = actual();
        assertThat(CapabilityInventoryCodec.decode(CapabilityInventoryCodec.encode(actual))).isEqualTo(actual);
        var absent = actual.toBuilder().clearFunctionCatalog().build();
        assertThatThrownBy(() -> CapabilityInventoryCodec.encode(absent))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("catalogue is absent");
        String encoded = Base64.getEncoder().encodeToString(absent.toByteArray());
        assertThatThrownBy(() -> CapabilityInventoryCodec.decode(encoded))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("catalogue is absent");
    }

    @Test
    void admitsEveryEntryOfTheUnmodifiedActualNativeArtifactWithinTheEncodedBound() {
        var actual = actual();
        assertThatCode(() -> FunctionCatalogContracts.validate(actual)).doesNotThrowAnyException();
        assertThat(actual.getFunctionCatalog().getRegisteredFunctionsCount()).isGreaterThan(900);
        assertThat(actual.getFunctionCatalog().getDefinitionsCount()).isGreaterThan(100);
        assertThat(Base64.getEncoder().encode(actual.toByteArray()).length).isLessThan(4 * 1024 * 1024);
        assertThat(actual.getFunctionCatalog().getManifestSha256()).isEqualTo(FunctionContractHashes.digest(
                actual.getFunctionCatalog().toBuilder().clearManifestSha256().build()));
    }

    @Test
    void absentCatalogueCannotAuthorizeAnyFunction() {
        reject(actual().toBuilder().clearFunctionCatalog().build(), "catalogue is absent");
    }

    @Test
    void inventoryEngineCannotDetachFromTheCapturedBuild() {
        reject(actual().toBuilder().setNativeEngineVersion("different-build").build(), "build differs");
    }

    @Test
    void declarationAndSelectorIdentitiesMustMatchTheSoleLocalGeneratedContract() {
        var actual = actual();
        reject(reseal(actual, actual.getFunctionCatalog().toBuilder()
                .setBindingDeclarationSha256(ByteString.copyFrom(new byte[32]))), "sole local declaration");
        reject(reseal(actual, actual.getFunctionCatalog().toBuilder()
                .setExecutionSelectionSha256(ByteString.copyFrom(new byte[32]))), "selector identity");
    }

    @Test
    void duplicateAndUnknownAvailabilityNeverAuthorizeExecution() {
        var actual = actual();
        var first = actual.getFunctions(0);
        reject(actual.toBuilder().addFunctions(first).build(), "Duplicate or unknown");
        var unknown = first.toBuilder().setRef(first.getRef().toBuilder().setName("unknown_function"));
        reject(actual.toBuilder().setFunctions(0, unknown).build(), "no exact definition");
    }

    @Test
    void availabilityMustRetainItsActualReferenceHashAndBuild() {
        var actual = actual();
        var first = actual.getFunctions(0);
        reject(actual.toBuilder().setFunctions(0, first.toBuilder().setRef(first.getRef().toBuilder()
                .setContractSha256(ByteString.copyFrom(new byte[32])))).build(), "no exact definition");
        reject(actual.toBuilder().setFunctions(0, first.toBuilder()
                .setImplementationBuildId("detached-build")).build(), "detached from the manifest");
    }

    @Test
    void missingExecutableAvailabilityIsNotSilentlyConvertedIntoAnUnsupportedFunction() {
        var actual = actual();
        reject(actual.toBuilder().removeFunctions(0).build(), "Complete executable availability");
    }

    @Test
    void absentOfflineCallbacksRemainDiagnosticAndCannotBeAdvertisedAsAvailable() {
        var actual = actual();
        var definition = actual.getFunctionCatalog().getDefinitionsList().stream()
                .filter(value -> !value.getRegistrationDependency().isEmpty())
                .filter(value -> actual.getFunctionsList().stream().noneMatch(available ->
                        available.getRef().equals(value.getRef())))
                .findFirst().orElseThrow();
        reject(actual.toBuilder().addFunctions(FunctionAvailability.newBuilder().setRef(definition.getRef())
                .setImplementationBuildId(actual.getFunctionCatalog().getImplementationBuildId())).build(),
                "advertised as executable");
    }

    @Test
    void rewritingRequiresTheExactDirectSoleDeclarationRatherThanAnImplementationAlias() {
        var actual = actual();
        var manifest = actual.getFunctionCatalog().toBuilder();
        int ordinal = rewriteOrdinal(actual.getFunctionCatalog());
        var definition = manifest.getDefinitions(ordinal).toBuilder();
        var rewrite = definition.getRewrite().toBuilder();
        rewrite.setNodes(0, rewrite.getNodes(0).toBuilder().setImplementationName("not"));
        manifest.setDefinitions(ordinal, definition.setRewrite(rewrite));
        reject(reseal(actual, manifest), "ambiguous actual dependency");

        definition = actual.getFunctionCatalog().getDefinitions(ordinal).toBuilder();
        rewrite = definition.getRewrite().toBuilder();
        rewrite.setNodes(0, rewrite.getNodes(0).toBuilder().setTarget(rewrite.getNodes(0).getTarget()
                .toBuilder().setName("unknown_direct_target")));
        manifest.setDefinitions(ordinal, definition.setRewrite(rewrite));
        reject(reseal(actual, manifest), "no direct sole declaration");
    }

    @Test
    void actualRegistrationMutationCannotKeepTheOriginalContractIdentity() {
        var actual = actual();
        var manifest = actual.getFunctionCatalog().toBuilder();
        manifest.setRegisteredFunctions(0, manifest.getRegisteredFunctions(0).toBuilder()
                .setImplementationName("modified_actual_registration"));
        reject(reseal(actual, manifest), "registration digest mismatch");
    }

    @Test
    void absentNullInputPolicyCannotAuthorizeAnActualScalarEntryEvenWithFreshDigests() {
        var actual = actual();
        var manifest = actual.getFunctionCatalog().toBuilder();
        for (int ordinal = 0; ordinal < manifest.getRegisteredFunctionsCount(); ordinal++) {
            var registration = manifest.getRegisteredFunctions(ordinal).toBuilder();
            for (int entryOrdinal = 0; entryOrdinal < registration.getImplementationsCount(); entryOrdinal++) {
                var entry = registration.getImplementations(entryOrdinal).toBuilder();
                if (entry.getFamily() == FunctionImplementationFamily.FUNCTION_IMPLEMENTATION_FAMILY_SPECIAL_FORM) {
                    continue;
                }
                assertThat(entry.getMetadata().hasDefaultNullBehavior()).isTrue();
                entry.setMetadata(entry.getMetadata().toBuilder().clearDefaultNullBehavior());
                entry.clearContractSha256();
                entry.setContractSha256(FunctionContractHashes.digest(entry.build()));
                registration.setImplementations(entryOrdinal, entry);
                registration.clearContractSha256();
                registration.setContractSha256(FunctionContractHashes.digest(registration.build()));
                manifest.setRegisteredFunctions(ordinal, registration);
                reject(resealRegistry(actual, manifest), "no signature or execution metadata");
                return;
            }
        }
        throw new IllegalStateException("The actual Native artifact must retain scalar implementation metadata");
    }

    @Test
    void absentNullInputPolicyCannotAuthorizeAnActualCompanionEvenWithFreshDigests() {
        var actual = actual();
        var manifest = actual.getFunctionCatalog().toBuilder();
        assertThat(manifest.getVectorCompanionsCount()).isPositive();
        var companion = manifest.getVectorCompanions(0).toBuilder();
        assertThat(companion.getMetadata().hasDefaultNullBehavior()).isTrue();
        companion.setMetadata(companion.getMetadata().toBuilder().clearDefaultNullBehavior());
        companion.clearContractSha256();
        companion.setContractSha256(FunctionContractHashes.digest(companion.build()));
        manifest.setVectorCompanions(0, companion);
        reject(resealRegistry(actual, manifest), "Companion has no actual metadata");
    }

    private static CapabilityInventory resealRegistry(
            CapabilityInventory actual, FunctionCatalogManifest.Builder manifest) {
        var projection = FunctionCatalogManifest.newBuilder()
                .addAllRegisteredFunctions(manifest.getRegisteredFunctionsList())
                .addAllVectorCompanions(manifest.getVectorCompanionsList()).build();
        manifest.setRegistrationSha256(FunctionContractHashes.digest(projection));
        return reseal(actual, manifest);
    }

    private static int rewriteOrdinal(FunctionCatalogManifest manifest) {
        for (int ordinal = 0; ordinal < manifest.getDefinitionsCount(); ordinal++) {
            if (manifest.getDefinitions(ordinal).hasRewrite()) return ordinal;
        }
        throw new IllegalStateException("The real Native artifact must retain its declared rewrites");
    }

    private static CapabilityInventory actual() {
        return CanonicalCapabilityInventoryTestFixture.inventory();
    }

    private static CapabilityInventory reseal(CapabilityInventory actual, FunctionCatalogManifest.Builder manifest) {
        manifest.clearManifestSha256();
        manifest.setManifestSha256(FunctionContractHashes.digest(manifest.build()));
        return actual.toBuilder().setFunctionCatalog(manifest).build();
    }

    private static void reject(CapabilityInventory actual, String reason) {
        assertThatThrownBy(() -> FunctionCatalogContracts.validate(actual))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(reason);
    }
}
