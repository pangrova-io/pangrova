package org.pangrova.protocol.capability.inventory;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.CapabilityDescriptor;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.protocol.v2.CapabilityKind;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the complete, unmodified actual JNI capability inventory and negative mutations. */
class CapabilityInventoryActualNativeContractTest {
    @Test
    void validatesAndRoundTripsEveryActualNativeCapabilityWithoutRemovingAny() throws IOException {
        var actual = actualInventory();
        var materializer = materializer(actual);
        assertThat(materializer.getLimits().getMaxCandidateRows()).isZero();
        assertThat(materializer.getLimits().getMaxCandidateBytes()).isZero();
        assertThat(materializer.getLimits().getMaxInputBytes()).isPositive();
        assertThat(materializer.getLimits().getMaxOutputBytes()).isPositive();
        assertThatCode(() -> CapabilityInventoryCodec.validate(actual)).doesNotThrowAnyException();
        var decoded = CapabilityInventoryCodec.decode(CapabilityInventoryCodec.encode(actual));
        assertThat(decoded).isEqualTo(actual);
        assertThat(decoded.getCapabilitiesList()).containsExactlyElementsOf(actual.getCapabilitiesList());
        assertThat(decoded.getFunctionsList()).containsExactlyElementsOf(actual.getFunctionsList());
    }

    @Test
    void stillRejectsAnActualRetrievalCapabilityWhoseCandidateBoundWasRemoved() throws IOException {
        var actual = actualInventory();
        var source = actual.getCapabilitiesList().stream().filter(value -> value.getKind()
                == CapabilityKind.CAPABILITY_KIND_RETRIEVAL_SOURCE).findFirst().orElseThrow();
        var invalid = replace(actual, source.toBuilder()
                .setLimits(source.getLimits().toBuilder().clearMaxCandidateRows()).build());
        assertThatThrownBy(() -> CapabilityInventoryCodec.validate(invalid))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("candidate bound");
    }

    @Test
    void rejectsCandidateBoundsThatCannotBeRepresentedAsAnAdmissibleJavaBudget() throws IOException {
        var actual = actualInventory();
        var source = actual.getCapabilitiesList().stream().filter(value -> value.getKind()
                == CapabilityKind.CAPABILITY_KIND_RETRIEVAL_SOURCE).findFirst().orElseThrow();
        var invalid = replace(actual, source.toBuilder()
                .setLimits(source.getLimits().toBuilder().setMaxCandidateRows(Long.MIN_VALUE)).build());
        assertThatThrownBy(() -> CapabilityInventoryCodec.validate(invalid))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("candidate bound");
    }

    @Test
    void rejectsMaterializationWithoutItsActualInputAndOutputByteBudgets() throws IOException {
        var actual = actualInventory();
        var materializer = materializer(actual);
        for (boolean input : new boolean[] {true, false}) {
            var limits = materializer.getLimits().toBuilder();
            if (input) {
                limits.clearMaxInputBytes();
            } else {
                limits.clearMaxOutputBytes();
            }
            var invalid = replace(actual, materializer.toBuilder().setLimits(limits).build());
            assertThatThrownBy(() -> CapabilityInventoryCodec.validate(invalid))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("materializer")
                    .hasMessageContaining("resource limits");
        }
    }

    @Test
    void rejectsACandidateGrantAttachedToRowPreservingMaterialization() throws IOException {
        var actual = actualInventory();
        var materializer = materializer(actual);
        for (boolean rows : new boolean[] {true, false}) {
            var limits = materializer.getLimits().toBuilder();
            if (rows) {
                limits.setMaxCandidateRows(1);
            } else {
                limits.setMaxCandidateBytes(1);
            }
            var invalid = replace(actual, materializer.toBuilder().setLimits(limits).build());
            assertThatThrownBy(() -> CapabilityInventoryCodec.validate(invalid))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("materializer")
                    .hasMessageContaining("resource limits");
        }
    }

    @Test
    void stillRejectsTheActualInventoryWithoutItsTypedRelationContract() throws IOException {
        var invalid = actualInventory().toBuilder().clearSupportedRelationTypeUrls().build();
        assertThatThrownBy(() -> CapabilityInventoryCodec.validate(invalid))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("typed relation contract");
    }

    @Test
    void rejectsUnknownKindsRatherThanTreatingThemAsOrdinaryCapabilities() throws IOException {
        var actual = actualInventory();
        var invalid = replace(actual, materializer(actual).toBuilder().setKindValue(999).build());
        assertThatThrownBy(() -> CapabilityInventoryCodec.validate(invalid))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("execution contract");
    }

    private static CapabilityInventory actualInventory() throws IOException {
        try (var input = CapabilityInventoryActualNativeContractTest.class.getResourceAsStream(
                "/capability-contracts/actual-native-capability-inventory.bin")) {
            if (input == null) {
                throw new IllegalStateException("The complete actual JNI capability inventory is required");
            }
            return CapabilityInventory.parseFrom(input);
        }
    }

    private static CapabilityDescriptor materializer(CapabilityInventory actual) {
        return actual.getCapabilitiesList().stream().filter(value -> value.getKind()
                == CapabilityKind.CAPABILITY_KIND_MATERIALIZER).findFirst().orElseThrow();
    }

    private static CapabilityInventory replace(CapabilityInventory actual, CapabilityDescriptor changed) {
        var builder = actual.toBuilder();
        for (int index = 0; index < actual.getCapabilitiesCount(); index++) {
            if (actual.getCapabilities(index).getRef().equals(changed.getRef())) {
                return builder.setCapabilities(index, changed).build();
            }
        }
        throw new IllegalArgumentException("Negative mutation must retain an existing actual capability identity");
    }
}
