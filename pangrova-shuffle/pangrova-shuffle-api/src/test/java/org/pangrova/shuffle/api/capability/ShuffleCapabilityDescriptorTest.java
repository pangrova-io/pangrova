package org.pangrova.shuffle.api.capability;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.pangrova.shuffle.api.enums.ShuffleCapability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.shuffle.api.enums.ShuffleCapability.*;

class ShuffleCapabilityDescriptorTest {
    @ParameterizedTest
    @EnumSource(ShuffleCapability.class)
    void guaranteesAreIndependent(ShuffleCapability declared) {
        var descriptor = ShuffleCapabilityDescriptor.of(declared);
        for (ShuffleCapability candidate : ShuffleCapability.values()) {
            assertThat(descriptor.supports(candidate)).as("%s must not imply %s", declared, candidate)
                    .isEqualTo(declared == candidate);
        }
    }

    @Test
    void effectiveGuaranteesRequireImplementationDeploymentAndExchangeEvidence() {
        var implementation = ShuffleCapabilityDescriptor.of(
                BLOCKING_READ, CONSUMER_RETRY, PRODUCER_RETRY,
                READ_AFTER_COMPUTE_WORKER_EXIT, READ_AFTER_STORAGE_WORKER_FAILURE);
        // A single storage-node deployment does not inherit the implementation's replication guarantee.
        var deployment = ShuffleCapabilityDescriptor.of(
                BLOCKING_READ, CONSUMER_RETRY, PRODUCER_RETRY, READ_AFTER_COMPUTE_WORKER_EXIT);
        // This exchange is not published and has no retryable/readable committed input yet.
        var producing = ShuffleCapabilityDescriptor.of(PRODUCER_RETRY);
        assertThat(implementation.intersect(deployment).intersect(producing).capabilities())
                .containsExactly(PRODUCER_RETRY);
        var committed = ShuffleCapabilityDescriptor.of(BLOCKING_READ, CONSUMER_RETRY,
                READ_AFTER_COMPUTE_WORKER_EXIT, READ_AFTER_STORAGE_WORKER_FAILURE);
        assertThat(implementation.intersect(deployment).intersect(committed).capabilities())
                .containsExactlyInAnyOrder(BLOCKING_READ, CONSUMER_RETRY, READ_AFTER_COMPUTE_WORKER_EXIT);
        assertThat(implementation.intersect(deployment).intersect(ShuffleCapabilityDescriptor.none())
                .capabilities()).isEmpty();
    }

    @Test
    void restrictionsAreCommutativeAssociativeAndNeverBroadenEitherInput() {
        var all = EnumSet.allOf(ShuffleCapability.class);
        for (ShuffleCapability missing : ShuffleCapability.values()) {
            var restricted = EnumSet.copyOf(all);
            restricted.remove(missing);
            var left = new ShuffleCapabilityDescriptor(restricted);
            var right = ShuffleCapabilityDescriptor.of(CONSUMER_RETRY, RETENTION, CLEANUP);
            var third = ShuffleCapabilityDescriptor.of(CONSUMER_RETRY, PARTITION_READ);
            var intersection = left.intersect(right);
            assertThat(intersection).isEqualTo(right.intersect(left));
            assertThat(left.intersect(right).intersect(third)).isEqualTo(left.intersect(right.intersect(third)));
            assertThat(intersection.capabilities()).isSubsetOf(left.capabilities()).isSubsetOf(right.capabilities());
            assertThat(intersection.supports(missing)).isFalse();
            assertThat(left.intersect(left)).isEqualTo(left);
        }
    }

    @Test
    void callerMutationCannotPromoteOrRemoveGuarantees() {
        Set<ShuffleCapability> input = new HashSet<>(Set.of(PARTITION_READ));
        var descriptor = new ShuffleCapabilityDescriptor(input);
        input.clear();
        input.add(CONSUMER_RETRY);
        assertThat(descriptor.capabilities()).containsExactly(PARTITION_READ);
        assertThatThrownBy(() -> descriptor.capabilities().add(CONSUMER_RETRY))
                .isInstanceOf(UnsupportedOperationException.class);
        var array = new ShuffleCapability[]{PARTITION_READ};
        var fromArray = ShuffleCapabilityDescriptor.of(array);
        array[0] = CONSUMER_RETRY;
        assertThat(fromArray).isEqualTo(descriptor);
    }

    @Test
    void unknownGuaranteesAreEmptyNotImplicitlySupported() {
        var unknown = ShuffleCapabilityDescriptor.none();
        assertThat(unknown.capabilities()).isEmpty();
        for (ShuffleCapability capability : ShuffleCapability.values()) {
            assertThat(unknown.supports(capability)).isFalse();
        }
        assertThat(new ShuffleCapabilityDescriptor(EnumSet.allOf(ShuffleCapability.class)).intersect(unknown))
                .isEqualTo(unknown);
    }

    @Test
    void duplicateDeclarationsDoNotChangeValueSemantics() {
        assertThat(ShuffleCapabilityDescriptor.of(PARTITION_READ, PARTITION_READ, BLOCKING_READ))
                .isEqualTo(ShuffleCapabilityDescriptor.of(BLOCKING_READ, PARTITION_READ));
    }

    @Test
    void nullInputsAreRejectedRatherThanTreatedAsAllOrUnknown() {
        assertThatThrownBy(() -> new ShuffleCapabilityDescriptor(null)).isInstanceOf(NullPointerException.class);
        var containingNull = new HashSet<ShuffleCapability>();
        containingNull.add(null);
        assertThatThrownBy(() -> new ShuffleCapabilityDescriptor(containingNull)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> ShuffleCapabilityDescriptor.of((ShuffleCapability[]) null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> ShuffleCapabilityDescriptor.of((ShuffleCapability) null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> ShuffleCapabilityDescriptor.none().supports(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> ShuffleCapabilityDescriptor.none().intersect(null)).isInstanceOf(NullPointerException.class);
    }
}
