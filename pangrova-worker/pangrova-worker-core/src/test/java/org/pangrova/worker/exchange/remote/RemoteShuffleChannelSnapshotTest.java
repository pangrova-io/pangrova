package org.pangrova.worker.exchange.remote;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RemoteShuffleChannelSnapshotTest {
    @Test
    void callerCannotChangeCapturedChannelCredits() {
        var endpoint = new RemoteShuffleChannelAdmission.Endpoint("worker.example", 8443, "worker-1");
        var credits = new HashMap<>(Map.of(endpoint, 2));
        var snapshot = new RemoteShuffleChannelAdmission.Snapshot(2, 1, credits);
        credits.clear();
        assertThat(snapshot.channelsByEndpoint()).containsEntry(endpoint, 2);
        assertThatThrownBy(() -> snapshot.channelsByEndpoint().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(snapshot.ownedAndCreatingChannels()).isEqualTo(2);
        assertThat(snapshot.endpointCount()).isEqualTo(1);
    }
}
