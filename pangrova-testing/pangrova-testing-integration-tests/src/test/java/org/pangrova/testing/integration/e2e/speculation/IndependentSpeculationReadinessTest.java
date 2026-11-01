package org.pangrova.testing.integration.e2e.speculation;

import org.junit.jupiter.api.Test;
import org.pangrova.common.json.jackson.JsonMapperFactory;

import static org.assertj.core.api.Assertions.assertThat;

class IndependentSpeculationReadinessTest {

    @Test
    void registrationAloneDoesNotMakeTwoWorkersSchedulable() throws Exception {
        var registered = JsonMapperFactory.shared().readTree("""
                [
                  {"workerId":"local-worker-1","healthState":"ACTIVE",
                   "memoryPressure":"NONE","availableMemoryBytes":0},
                  {"workerId":"local-worker-2","healthState":"ACTIVE",
                   "memoryPressure":"NONE","availableMemoryBytes":0}
                ]
                """);
        assertThat(IndependentSpeculationProcesses.twoWorkersSchedulable(registered)).isFalse();

        var afterFirstHeartbeat = JsonMapperFactory.shared().readTree("""
                [
                  {"workerId":"local-worker-1","healthState":"ACTIVE",
                   "memoryPressure":"NONE","availableMemoryBytes":536870912},
                  {"workerId":"local-worker-2","healthState":"ACTIVE",
                   "memoryPressure":"NONE","availableMemoryBytes":0}
                ]
                """);
        assertThat(IndependentSpeculationProcesses.twoWorkersSchedulable(afterFirstHeartbeat)).isFalse();

        var afterBothHeartbeats = JsonMapperFactory.shared().readTree("""
                [
                  {"workerId":"local-worker-1","healthState":"ACTIVE",
                   "memoryPressure":"NONE","availableMemoryBytes":536870912},
                  {"workerId":"local-worker-2","healthState":"ACTIVE",
                   "memoryPressure":"NONE","availableMemoryBytes":536870912}
                ]
                """);
        assertThat(IndependentSpeculationProcesses.twoWorkersSchedulable(afterBothHeartbeats)).isTrue();
    }

    @Test
    void criticalPressureIsNotSchedulableDespiteReportedMemory() throws Exception {
        var workers = JsonMapperFactory.shared().readTree("""
                [
                  {"workerId":"local-worker-1","healthState":"ACTIVE",
                   "memoryPressure":"CRITICAL","availableMemoryBytes":536870912},
                  {"workerId":"local-worker-2","healthState":"ACTIVE",
                   "memoryPressure":"NONE","availableMemoryBytes":536870912}
                ]
                """);
        assertThat(IndependentSpeculationProcesses.twoWorkersSchedulable(workers)).isFalse();
    }
}
