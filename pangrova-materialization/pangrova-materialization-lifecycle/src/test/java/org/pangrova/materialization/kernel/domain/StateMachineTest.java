package org.pangrova.materialization.kernel.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.pangrova.materialization.api.enums.AttemptState;
import org.pangrova.materialization.api.enums.GenerationState;
import org.pangrova.materialization.api.enums.MaterializationKind;
import org.pangrova.materialization.api.enums.ObjectState;
import org.pangrova.materialization.api.enums.RefreshKind;
import org.pangrova.materialization.api.enums.RefreshMode;
import org.pangrova.materialization.api.enums.SecurityMode;
import org.pangrova.materialization.api.identity.StrategyId;

class StateMachineTest {

    private static final List<ObjectState> OBJECT_STATES = List.of(
            ObjectState.CREATING,
            ObjectState.ACTIVE,
            ObjectState.SUSPENDED,
            ObjectState.FAILED,
            ObjectState.DROPPING,
            ObjectState.DROPPED);

    private static final List<AttemptState> ATTEMPT_STATES = List.of(
            AttemptState.PREPARING,
            AttemptState.BUILDING,
            AttemptState.COMMITTED,
            AttemptState.PUBLISHED,
            AttemptState.FAILED,
            AttemptState.ABANDONED,
            AttemptState.QUARANTINED,
            AttemptState.SUPERSEDED);

    private static final List<GenerationState> GENERATION_STATES = List.of(
            GenerationState.CANDIDATE,
            GenerationState.COMMITTED,
            GenerationState.PUBLISHED,
            GenerationState.RETIRED,
            GenerationState.QUARANTINED,
            GenerationState.COLLECTED);

    @Test
    void objectStateGraphIsExactlyTheDeclaredGraph() {
        Map<ObjectState, Set<ObjectState>> allowed = new LinkedHashMap<>();
        allowed.put(ObjectState.CREATING,
                Set.of(ObjectState.ACTIVE, ObjectState.FAILED, ObjectState.DROPPING));
        allowed.put(ObjectState.ACTIVE,
                Set.of(ObjectState.SUSPENDED, ObjectState.FAILED, ObjectState.DROPPING));
        allowed.put(ObjectState.SUSPENDED,
                Set.of(ObjectState.ACTIVE, ObjectState.DROPPING));
        allowed.put(ObjectState.FAILED,
                Set.of(ObjectState.ACTIVE, ObjectState.DROPPING));
        allowed.put(ObjectState.DROPPING, Set.of(ObjectState.DROPPED));
        allowed.put(ObjectState.DROPPED, Set.of());

        for (ObjectState from : OBJECT_STATES) {
            for (ObjectState to : OBJECT_STATES) {
                assertThat(from.canTransitionTo(to))
                        .as("%s -> %s", from, to)
                        .isEqualTo(allowed.get(from).contains(to));
            }
        }
        assertThat(OBJECT_STATES).filteredOn(ObjectState::isTerminal)
                .containsExactly(ObjectState.DROPPED);
    }

    @Test
    void attemptStateGraphIsExactlyTheDeclaredGraph() {
        Map<AttemptState, Set<AttemptState>> allowed = new LinkedHashMap<>();
        allowed.put(AttemptState.PREPARING,
                Set.of(AttemptState.BUILDING, AttemptState.FAILED,
                        AttemptState.ABANDONED));
        allowed.put(AttemptState.BUILDING,
                Set.of(AttemptState.COMMITTED, AttemptState.FAILED,
                        AttemptState.ABANDONED, AttemptState.QUARANTINED));
        allowed.put(AttemptState.COMMITTED,
                Set.of(AttemptState.PUBLISHED, AttemptState.QUARANTINED,
                        AttemptState.SUPERSEDED, AttemptState.FAILED));
        allowed.put(AttemptState.PUBLISHED, Set.of());
        allowed.put(AttemptState.FAILED, Set.of());
        allowed.put(AttemptState.ABANDONED, Set.of());
        allowed.put(AttemptState.QUARANTINED,
                Set.of(AttemptState.COMMITTED, AttemptState.ABANDONED));
        allowed.put(AttemptState.SUPERSEDED, Set.of());

        for (AttemptState from : ATTEMPT_STATES) {
            for (AttemptState to : ATTEMPT_STATES) {
                assertThat(from.canTransitionTo(to))
                        .as("%s -> %s", from, to)
                        .isEqualTo(allowed.get(from).contains(to));
            }
        }
        assertThat(ATTEMPT_STATES).filteredOn(AttemptState::isTerminal)
                .containsExactlyInAnyOrder(
                        AttemptState.PUBLISHED,
                        AttemptState.FAILED,
                        AttemptState.ABANDONED,
                        AttemptState.SUPERSEDED);
    }

    @Test
    void generationStateGraphIsExactlyTheDeclaredGraph() {
        Map<GenerationState, Set<GenerationState>> allowed = new LinkedHashMap<>();
        allowed.put(GenerationState.CANDIDATE,
                Set.of(GenerationState.COMMITTED, GenerationState.QUARANTINED,
                        GenerationState.COLLECTED));
        allowed.put(GenerationState.COMMITTED,
                Set.of(GenerationState.PUBLISHED, GenerationState.QUARANTINED,
                        GenerationState.COLLECTED));
        allowed.put(GenerationState.PUBLISHED, Set.of(GenerationState.RETIRED));
        allowed.put(GenerationState.RETIRED, Set.of(GenerationState.COLLECTED));
        allowed.put(GenerationState.QUARANTINED,
                Set.of(GenerationState.COMMITTED, GenerationState.COLLECTED));
        allowed.put(GenerationState.COLLECTED, Set.of());

        for (GenerationState from : GENERATION_STATES) {
            for (GenerationState to : GENERATION_STATES) {
                assertThat(from.canTransitionTo(to))
                        .as("%s -> %s", from, to)
                        .isEqualTo(allowed.get(from).contains(to));
            }
        }
        assertThat(GENERATION_STATES).filteredOn(GenerationState::isTerminal)
                .containsExactly(GenerationState.COLLECTED);
    }

    @Test
    void refreshKindsMapToDisjointStrategiesAndModes() {
        assertThat(RefreshKind.FULL.isIncremental()).isFalse();
        assertThat(RefreshKind.FULL.strategyId()).isEqualTo(StrategyId.FULL);
        assertThat(RefreshKind.INCREMENTAL_APPEND_ONLY.strategyId()).isEqualTo(StrategyId.APPEND_ONLY);
        assertThat(RefreshKind.INCREMENTAL_PARTITION_REPLACE.strategyId())
                .isEqualTo(StrategyId.PARTITION_REPLACE);
        assertThat(RefreshKind.INCREMENTAL_AFFECTED_KEY.strategyId()).isEqualTo(StrategyId.AFFECTED_KEY);
        assertThat(RefreshKind.INCREMENTAL_CDC.strategyId()).isEqualTo(StrategyId.CDC);

        assertThat(RefreshKind.FULL.satisfies(RefreshMode.FULL)).isTrue();
        assertThat(RefreshKind.INCREMENTAL_APPEND_ONLY.satisfies(RefreshMode.FULL)).isFalse();
        assertThat(RefreshKind.INCREMENTAL_APPEND_ONLY.satisfies(RefreshMode.INCREMENTAL)).isTrue();
        assertThat(RefreshKind.FULL.satisfies(RefreshMode.INCREMENTAL)).isFalse();
    }

    @Test
    void onlyDefinerSecurityModeIsSupportedInTheFirstKernel() {
        assertThat(SecurityMode.DEFINER.isSupported()).isTrue();
        assertThat(SecurityMode.INVOKER.isSupported()).isFalse();
        assertThat(MaterializationKind.DYNAMIC_TABLE.participatesInTransparentRewrite()).isFalse();
        assertThat(MaterializationKind.MATERIALIZED_VIEW.participatesInTransparentRewrite()).isTrue();
    }
}
