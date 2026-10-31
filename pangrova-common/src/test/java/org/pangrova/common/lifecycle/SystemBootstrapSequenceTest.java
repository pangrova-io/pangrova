package org.pangrova.common.lifecycle;

import org.pangrova.common.exception.PangrovaException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for SystemBootstrapSequence phase ordering and failure handling.
 */
class SystemBootstrapSequenceTest {

    @Test
    void executesAllPhasesInOrder() {
        List<BootstrapPhase> executed = new ArrayList<>();
        SystemBootstrapSequence sequence = new SystemBootstrapSequence(
                List.of(executed::add));

        sequence.run();

        assertThat(executed).containsExactly(
                BootstrapPhase.INFRASTRUCTURE,
                BootstrapPhase.COORDINATORS,
                BootstrapPhase.WORKERS,
                BootstrapPhase.READINESS);
    }

    @Test
    void haltsOnPhaseFailureWithContext() {
        SystemBootstrapSequence sequence = new SystemBootstrapSequence(List.of(
                phase -> {
                    if (phase == BootstrapPhase.COORDINATORS) {
                        throw new RuntimeException("metadata init failed");
                    }
                }
        ));

        assertThatThrownBy(sequence::run)
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("COORDINATORS")
                .hasMessageContaining("metadata init failed");
    }

    @Test
    void multipleExecutorsRunInOrder() {
        List<String> log = new ArrayList<>();
        SystemBootstrapSequence sequence = new SystemBootstrapSequence(List.of(
                phase -> log.add("A:" + phase),
                phase -> log.add("B:" + phase)
        ));

        sequence.run();

        assertThat(log).startsWith(
                "A:INFRASTRUCTURE", "B:INFRASTRUCTURE",
                "A:COORDINATORS", "B:COORDINATORS");
    }

    @Test
    void subsequentPhasesSkippedAfterFailure() {
        List<BootstrapPhase> executed = new ArrayList<>();
        SystemBootstrapSequence sequence = new SystemBootstrapSequence(List.of(
                phase -> {
                    if (phase == BootstrapPhase.WORKERS) {
                        throw new RuntimeException("native init failed");
                    }
                    executed.add(phase);
                }
        ));

        assertThatThrownBy(sequence::run).isInstanceOf(PangrovaException.class);
        assertThat(executed).containsExactly(
                BootstrapPhase.INFRASTRUCTURE,
                BootstrapPhase.COORDINATORS);
    }
}
