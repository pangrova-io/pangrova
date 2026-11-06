package org.pangrova.planner.core.validation;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link NativeCapabilityRegistry#supportsDistinctSetOps()} (R10.11).
 */
class NativeCapabilityRegistryDistinctSetOpsTest {

    @Test
    void defaultsDoNotSupportDistinctSetOps() {
        NativeCapabilityRegistry registry = NativeCapabilityRegistry.defaults();

        assertThat(registry.supportsDistinctSetOps())
                .as("Default capabilities should not include distinct set ops")
                .isFalse();
    }

    @Test
    void supportsDistinctSetOpsReturnsTrueWhenCapabilityPresent() {
        NativeCapabilityRegistry registry = NativeCapabilityRegistry.defaults();
        registry.update(
                Set.of("SetRel", "SetRel.DISTINCT", "ReadRel"),
                Set.of("Literal"),
                Set.of("add"));

        assertThat(registry.supportsDistinctSetOps()).isTrue();
    }

    @Test
    void supportsDistinctSetOpsReturnsFalseAfterUpdateWithoutCapability() {
        NativeCapabilityRegistry registry = NativeCapabilityRegistry.defaults();
        registry.update(
                Set.of("SetRel", "ReadRel"),
                Set.of("Literal"),
                Set.of("add"));

        assertThat(registry.supportsDistinctSetOps()).isFalse();
    }
}
