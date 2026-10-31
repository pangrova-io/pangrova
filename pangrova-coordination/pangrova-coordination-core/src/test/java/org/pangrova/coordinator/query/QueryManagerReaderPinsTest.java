package org.pangrova.coordinator.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.lifecycle.ReaderPinLifecycle;

/** Query lifecycle ownership of materialization reader-pin leases. */
class QueryManagerReaderPinsTest {

    private QueryTerminalControlFixture fixture;
    private final List<String> released = new ArrayList<>();
    private final List<Collection<String>> renewals = new ArrayList<>();

    @BeforeEach
    void wireLifecycle() {
        fixture = new QueryTerminalControlFixture(null);
        fixture.manager.setReaderPinLifecycle(new ReaderPinLifecycle() {
            @Override
            public void release(String queryId) {
                released.add(queryId);
            }

            @Override
            public void renew(Collection<String> queryIds) {
                renewals.add(List.copyOf(queryIds));
            }
        });
    }

    @AfterEach
    void closeOwners() {
        fixture.close();
    }

    @Test
    void terminalCleanupReleasesTheQueryReaderPins() {
        String queryId = fixture.registerRunningQuery();

        fixture.manager.onAllTasksFinished(queryId);

        assertThat(released).contains(queryId);
    }

    @Test
    void renewalCoversLiveQueriesOnly() {
        String live = fixture.registerRunningQuery();

        fixture.retry();

        assertThat(renewals).isNotEmpty();
        assertThat(renewals.get(renewals.size() - 1)).contains(live);
    }
}
