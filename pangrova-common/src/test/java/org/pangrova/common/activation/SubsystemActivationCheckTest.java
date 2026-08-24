package org.pangrova.common.activation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;

import org.junit.jupiter.api.Test;

class SubsystemActivationCheckTest {

    @Test
    void inventoryResourceLoadsAll170Ids() {
        SubsystemActivationCheck check = SubsystemActivationCheck.fromInventory();
        assertThat(check.inventorySize()).isEqualTo(170);
    }

    @Test
    void allPassShimWhenNoPredicatesRegistered() {
        SubsystemActivationCheck check = SubsystemActivationCheck.fromInventory();
        SubsystemActivationCheck.Result result = check.verify();
        assertThat(result.allActive()).isTrue();
        assertThat(result.shimmedCount()).isEqualTo(170);
        assertThat(result.activeCount()).isZero();
        // does not throw
        check.runOrAbort();
    }

    @Test
    void registeredPassingPredicateCountsAsActive() {
        SubsystemActivationCheck check = SubsystemActivationCheck.forIds(Set.of("A", "B", "C"));
        check.register("A", "a active", () -> true);
        SubsystemActivationCheck.Result result = check.verify();
        assertThat(result.activeCount()).isEqualTo(1);
        assertThat(result.shimmedCount()).isEqualTo(2);
        assertThat(result.allActive()).isTrue();
    }

    @Test
    void failingPredicateAbortsWithOffendingId() {
        SubsystemActivationCheck check = SubsystemActivationCheck.forIds(Set.of("A", "B"));
        check.register("A", "a active", () -> false);
        assertThatThrownBy(check::runOrAbort)
                .isInstanceOf(SubsystemActivationException.class)
                .hasMessageContaining("A");
        assertThat(check.verify().failedIds()).containsExactly("A");
    }

    @Test
    void throwingPredicateIsTreatedAsFailure() {
        SubsystemActivationCheck check = SubsystemActivationCheck.forIds(Set.of("A"));
        check.register("A", "boom", () -> {
            throw new IllegalStateException("boom");
        });
        assertThat(check.verify().failedIds()).containsExactly("A");
    }

    @Test
    void registeringUnknownIdIsRejected() {
        SubsystemActivationCheck check = SubsystemActivationCheck.forIds(Set.of("A"));
        assertThatThrownBy(() -> check.register("NOT-IN-INVENTORY", "x", () -> true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void duplicateRegistrationIsRejected() {
        SubsystemActivationCheck check = SubsystemActivationCheck.forIds(Set.of("A"));
        check.register("A", "x", () -> true);
        assertThatThrownBy(() -> check.register("A", "y", () -> true))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
