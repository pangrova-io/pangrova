package org.pangrova.materialization.kernel.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.pangrova.materialization.api.domain.AttemptState;
import org.pangrova.materialization.api.domain.GenerationState;
import org.pangrova.materialization.api.enums.MaterializationKind;
import org.pangrova.materialization.api.domain.ObjectState;
import org.pangrova.materialization.api.domain.RefreshKind;
import org.pangrova.materialization.api.enums.RefreshMode;
import org.pangrova.materialization.api.enums.SecurityMode;
import org.pangrova.materialization.api.domain.StrategyId;

class StateMachineTest {

    private static final List<ObjectState> OBJECT_STATES = List.of(
            ObjectState.Creating.INSTANCE,
            ObjectState.Active.INSTANCE,
            ObjectState.Suspended.INSTANCE,
            ObjectState.Failed.INSTANCE,
            ObjectState.Dropping.INSTANCE,
            ObjectState.Dropped.INSTANCE);

    private static final List<AttemptState> ATTEMPT_STATES = List.of(
            AttemptState.Preparing.INSTANCE,
            AttemptState.Building.INSTANCE,
            AttemptState.Committed.INSTANCE,
            AttemptState.Published.INSTANCE,
            AttemptState.Failed.INSTANCE,
            AttemptState.Abandoned.INSTANCE,
            AttemptState.Quarantined.INSTANCE,
            AttemptState.Superseded.INSTANCE);

    private static final List<GenerationState> GENERATION_STATES = List.of(
            GenerationState.Candidate.INSTANCE,
            GenerationState.Committed.INSTANCE,
            GenerationState.Published.INSTANCE,
            GenerationState.Retired.INSTANCE,
            GenerationState.Quarantined.INSTANCE,
            GenerationState.Collected.INSTANCE);

    @Test
    void objectStateGraphIsExactlyTheDeclaredGraph() {
        Map<ObjectState, Set<ObjectState>> allowed = new LinkedHashMap<>();
        allowed.put(ObjectState.Creating.INSTANCE,
                Set.of(ObjectState.Active.INSTANCE, ObjectState.Failed.INSTANCE, ObjectState.Dropping.INSTANCE));
        allowed.put(ObjectState.Active.INSTANCE,
                Set.of(ObjectState.Suspended.INSTANCE, ObjectState.Failed.INSTANCE, ObjectState.Dropping.INSTANCE));
        allowed.put(ObjectState.Suspended.INSTANCE,
                Set.of(ObjectState.Active.INSTANCE, ObjectState.Dropping.INSTANCE));
        allowed.put(ObjectState.Failed.INSTANCE,
                Set.of(ObjectState.Active.INSTANCE, ObjectState.Dropping.INSTANCE));
        allowed.put(ObjectState.Dropping.INSTANCE, Set.of(ObjectState.Dropped.INSTANCE));
        allowed.put(ObjectState.Dropped.INSTANCE, Set.of());

        for (ObjectState from : OBJECT_STATES) {
            for (ObjectState to : OBJECT_STATES) {
                assertThat(from.canTransitionTo(to))
                        .as("%s -> %s", from, to)
                        .isEqualTo(allowed.get(from).contains(to));
            }
        }
        assertThat(OBJECT_STATES).filteredOn(ObjectState::isTerminal)
                .containsExactly(ObjectState.Dropped.INSTANCE);
    }

    @Test
    void attemptStateGraphIsExactlyTheDeclaredGraph() {
        Map<AttemptState, Set<AttemptState>> allowed = new LinkedHashMap<>();
        allowed.put(AttemptState.Preparing.INSTANCE,
                Set.of(AttemptState.Building.INSTANCE, AttemptState.Failed.INSTANCE,
                        AttemptState.Abandoned.INSTANCE));
        allowed.put(AttemptState.Building.INSTANCE,
                Set.of(AttemptState.Committed.INSTANCE, AttemptState.Failed.INSTANCE,
                        AttemptState.Abandoned.INSTANCE, AttemptState.Quarantined.INSTANCE));
        allowed.put(AttemptState.Committed.INSTANCE,
                Set.of(AttemptState.Published.INSTANCE, AttemptState.Quarantined.INSTANCE,
                        AttemptState.Superseded.INSTANCE, AttemptState.Failed.INSTANCE));
        allowed.put(AttemptState.Published.INSTANCE, Set.of());
        allowed.put(AttemptState.Failed.INSTANCE, Set.of());
        allowed.put(AttemptState.Abandoned.INSTANCE, Set.of());
        allowed.put(AttemptState.Quarantined.INSTANCE,
                Set.of(AttemptState.Committed.INSTANCE, AttemptState.Abandoned.INSTANCE));
        allowed.put(AttemptState.Superseded.INSTANCE, Set.of());

        for (AttemptState from : ATTEMPT_STATES) {
            for (AttemptState to : ATTEMPT_STATES) {
                assertThat(from.canTransitionTo(to))
                        .as("%s -> %s", from, to)
                        .isEqualTo(allowed.get(from).contains(to));
            }
        }
        assertThat(ATTEMPT_STATES).filteredOn(AttemptState::isTerminal)
                .containsExactlyInAnyOrder(
                        AttemptState.Published.INSTANCE,
                        AttemptState.Failed.INSTANCE,
                        AttemptState.Abandoned.INSTANCE,
                        AttemptState.Superseded.INSTANCE);
    }

    @Test
    void generationStateGraphIsExactlyTheDeclaredGraph() {
        Map<GenerationState, Set<GenerationState>> allowed = new LinkedHashMap<>();
        allowed.put(GenerationState.Candidate.INSTANCE,
                Set.of(GenerationState.Committed.INSTANCE, GenerationState.Quarantined.INSTANCE,
                        GenerationState.Collected.INSTANCE));
        allowed.put(GenerationState.Committed.INSTANCE,
                Set.of(GenerationState.Published.INSTANCE, GenerationState.Quarantined.INSTANCE,
                        GenerationState.Collected.INSTANCE));
        allowed.put(GenerationState.Published.INSTANCE, Set.of(GenerationState.Retired.INSTANCE));
        allowed.put(GenerationState.Retired.INSTANCE, Set.of(GenerationState.Collected.INSTANCE));
        allowed.put(GenerationState.Quarantined.INSTANCE,
                Set.of(GenerationState.Committed.INSTANCE, GenerationState.Collected.INSTANCE));
        allowed.put(GenerationState.Collected.INSTANCE, Set.of());

        for (GenerationState from : GENERATION_STATES) {
            for (GenerationState to : GENERATION_STATES) {
                assertThat(from.canTransitionTo(to))
                        .as("%s -> %s", from, to)
                        .isEqualTo(allowed.get(from).contains(to));
            }
        }
        assertThat(GENERATION_STATES).filteredOn(GenerationState::isTerminal)
                .containsExactly(GenerationState.Collected.INSTANCE);
    }

    @Test
    void refreshKindsMapToDisjointStrategiesAndModes() {
        assertThat(RefreshKind.Full.INSTANCE.isIncremental()).isFalse();
        assertThat(RefreshKind.Full.INSTANCE.strategyId()).isEqualTo(StrategyId.FULL);
        assertThat(RefreshKind.IncrementalAppendOnly.INSTANCE.strategyId()).isEqualTo(StrategyId.APPEND_ONLY);
        assertThat(RefreshKind.IncrementalPartitionReplace.INSTANCE.strategyId())
                .isEqualTo(StrategyId.PARTITION_REPLACE);
        assertThat(RefreshKind.IncrementalAffectedKey.INSTANCE.strategyId()).isEqualTo(StrategyId.AFFECTED_KEY);
        assertThat(RefreshKind.IncrementalCdc.INSTANCE.strategyId()).isEqualTo(StrategyId.CDC);

        assertThat(RefreshKind.Full.INSTANCE.satisfies(RefreshMode.FULL)).isTrue();
        assertThat(RefreshKind.IncrementalAppendOnly.INSTANCE.satisfies(RefreshMode.FULL)).isFalse();
        assertThat(RefreshKind.IncrementalAppendOnly.INSTANCE.satisfies(RefreshMode.INCREMENTAL)).isTrue();
        assertThat(RefreshKind.Full.INSTANCE.satisfies(RefreshMode.INCREMENTAL)).isFalse();
    }

    @Test
    void onlyDefinerSecurityModeIsSupportedInTheFirstKernel() {
        assertThat(SecurityMode.DEFINER.isSupported()).isTrue();
        assertThat(SecurityMode.INVOKER.isSupported()).isFalse();
        assertThat(MaterializationKind.DYNAMIC_TABLE.participatesInTransparentRewrite()).isFalse();
        assertThat(MaterializationKind.MATERIALIZED_VIEW.participatesInTransparentRewrite()).isTrue();
    }
}
