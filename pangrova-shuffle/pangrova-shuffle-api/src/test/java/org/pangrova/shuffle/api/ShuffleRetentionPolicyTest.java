package org.pangrova.shuffle.api;

import org.pangrova.shuffle.api.enums.DecommissionReadiness;

import org.junit.jupiter.api.Test;

import java.util.List;

import org.pangrova.shuffle.api.enums.ShuffleRetentionPolicy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link ShuffleRetentionPolicy} and the {@link DecommissionAssessment} /
 * {@link ShuffleDataRef} value types (G-RES-08).
 */
class ShuffleRetentionPolicyTest {
    @Test
    void unknownAssessmentNeverAuthorizesRemovalAndApiHasNoOptimisticDefault() throws Exception {
        assertThat(DecommissionAssessment.UNKNOWN.isSafe()).isFalse();
        assertThat(DecommissionAssessment.UNKNOWN.isSafeWithDrain()).isFalse();
        assertThat(DecommissionAssessment.UNKNOWN.requiresReExecution()).isFalse();
        assertThat(ShuffleDataInventory.class.getMethod("assessDecommission", String.class).isDefault()).isFalse();
    }

    @Test
    void reportedReferenceCannotUseBlankOrNegativeIdentity() {
        assertThatThrownBy(() -> new ShuffleDataRef(" ", 0, "w", ShuffleRetentionPolicy.EPHEMERAL, 0, false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ShuffleDataRef("q", -1, "w", ShuffleRetentionPolicy.EPHEMERAL, 0, false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ShuffleDataRef("q", 0, " ", ShuffleRetentionPolicy.EPHEMERAL, 0, false))
                .isInstanceOf(IllegalArgumentException.class);
    }


    // ── ShuffleRetentionPolicy enum ──

    @Test
    void retentionPolicyHasThreeValues() {
        assertThat(ShuffleRetentionPolicy.values()).containsExactly(
                ShuffleRetentionPolicy.EPHEMERAL,
                ShuffleRetentionPolicy.DETACHED,
                ShuffleRetentionPolicy.PERSISTENT);
    }

    // ── ShuffleDataRef ──

    @Test
    void shuffleDataRefValidConstruction() {
        var ref = new ShuffleDataRef("q1", 2, "w1",
                ShuffleRetentionPolicy.EPHEMERAL, 1024, false);
        assertThat(ref.queryId()).isEqualTo("q1");
        assertThat(ref.stageId()).isEqualTo(2);
        assertThat(ref.workerId()).isEqualTo("w1");
        assertThat(ref.retention()).isEqualTo(ShuffleRetentionPolicy.EPHEMERAL);
        assertThat(ref.sizeBytes()).isEqualTo(1024);
        assertThat(ref.consumed()).isFalse();
        assertThat(ref.isLive()).isTrue();
        assertThat(ref.isEphemeral()).isTrue();
    }

    @Test
    void shuffleDataRefConsumedIsNotLive() {
        var ref = new ShuffleDataRef("q1", 1, "w1",
                ShuffleRetentionPolicy.DETACHED, 512, true);
        assertThat(ref.isLive()).isFalse();
        assertThat(ref.consumed()).isTrue();
    }

    @Test
    void shuffleDataRefPersistentIsNotEphemeral() {
        var ref = new ShuffleDataRef("q1", 1, "w1",
                ShuffleRetentionPolicy.PERSISTENT, 0, false);
        assertThat(ref.isEphemeral()).isFalse();
    }

    @Test
    void shuffleDataRefNullQueryIdThrows() {
        assertThatThrownBy(() -> new ShuffleDataRef(null, 1, "w1",
                ShuffleRetentionPolicy.EPHEMERAL, 0, false))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shuffleDataRefNullWorkerIdThrows() {
        assertThatThrownBy(() -> new ShuffleDataRef("q1", 1, null,
                ShuffleRetentionPolicy.EPHEMERAL, 0, false))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shuffleDataRefNegativeSizeBytesThrows() {
        assertThatThrownBy(() -> new ShuffleDataRef("q1", 1, "w1",
                ShuffleRetentionPolicy.EPHEMERAL, -1, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ── DecommissionAssessment ──

    @Test
    void safeAssessmentIsSafe() {
        assertThat(DecommissionAssessment.SAFE.isSafe()).isTrue();
        assertThat(DecommissionAssessment.SAFE.isSafeWithDrain()).isTrue();
        assertThat(DecommissionAssessment.SAFE.requiresReExecution()).isFalse();
        assertThat(DecommissionAssessment.SAFE.ephemeralData()).isEmpty();
        assertThat(DecommissionAssessment.SAFE.ephemeralBytes()).isZero();
    }

    @Test
    void safeWithDrainAssessment() {
        var assessment = new DecommissionAssessment(
                DecommissionReadiness.SAFE_WITH_DRAIN, List.of(), 0);
        assertThat(assessment.isSafe()).isFalse();
        assertThat(assessment.isSafeWithDrain()).isTrue();
        assertThat(assessment.requiresReExecution()).isFalse();
    }

    @Test
    void requiresReExecutionAssessment() {
        var ref = new ShuffleDataRef("q1", 1, "w1",
                ShuffleRetentionPolicy.EPHEMERAL, 2048, false);
        var assessment = new DecommissionAssessment(
                DecommissionReadiness.REQUIRES_RE_EXECUTION, List.of(ref), 2048);
        assertThat(assessment.isSafe()).isFalse();
        assertThat(assessment.isSafeWithDrain()).isFalse();
        assertThat(assessment.requiresReExecution()).isTrue();
        assertThat(assessment.ephemeralData()).hasSize(1);
        assertThat(assessment.ephemeralBytes()).isEqualTo(2048);
    }

    @Test
    void decommissionAssessmentNullReadinessThrows() {
        assertThatThrownBy(() -> new DecommissionAssessment(null, List.of(), 0))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void decommissionAssessmentNegativeBytesThrows() {
        assertThatThrownBy(() -> new DecommissionAssessment(
                DecommissionReadiness.SAFE, List.of(), -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void decommissionAssessmentDefensiveCopy() {
        var ref = new ShuffleDataRef("q1", 1, "w1",
                ShuffleRetentionPolicy.EPHEMERAL, 100, false);
        var mutableList = new java.util.ArrayList<>(List.of(ref));
        var assessment = new DecommissionAssessment(
                DecommissionReadiness.REQUIRES_RE_EXECUTION, mutableList, 100);
        mutableList.clear();
        // The assessment should retain its own copy
        assertThat(assessment.ephemeralData()).hasSize(1);
    }
}
