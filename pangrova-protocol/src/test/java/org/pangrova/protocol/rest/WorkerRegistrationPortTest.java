package org.pangrova.protocol.rest;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.protocol.CanonicalCapabilityInventoryTestFixture;
import org.pangrova.protocol.execution.identity.WorkerExecutionIdentity;
import org.pangrova.protocol.rest.worker.WorkerRegistrationRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerRegistrationPortTest {
    @ParameterizedTest(name = "{0} rejects port {1}")
    @MethodSource("invalidPorts")
    void rejectsPortsOutsideTcpRange(int index, int value) {
        int[] ports = {8080, 9090, 9091, 8443};
        ports[index] = value;
        assertThatThrownBy(() -> request(ports)).isInstanceOf(IllegalArgumentException.class);
    }

    static Stream<Arguments> invalidPorts() {
        return Stream.of(0, 1, 2, 3).flatMap(index ->
                Stream.of(-1, 65536, Integer.MAX_VALUE).map(port -> Arguments.of(index, port)));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 65535})
    void preservesDisabledAndMaximumPorts(int port) {
        assertThat(request(new int[] {port, port, port, port}).copyTaskTlsPort()).isEqualTo(port);
    }

    private static WorkerRegistrationRequest request(int[] ports) {
        return new WorkerRegistrationRequest("worker", "localhost", ports[0], ports[1], ports[2], 2,
                null, List.of(), null, CanonicalCapabilityInventoryTestFixture.encodedInventory(), ports[3],
                new WorkerExecutionIdentity("00000000-0000-0000-0000-000000000001",
                        "00000000-0000-0000-0000-000000000002"));
    }
}
