package org.pangrova.coordinator.query;

import org.pangrova.persistence.query.memory.state.InMemoryQueryStateJournal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.query.spi.retention.ReaderPinLifecycle;

/** Query lifecycle ownership of materialization reader-pin leases. */
class QueryManagerReaderPinsTest {

    private QueryTerminalControlFixture fixture;
    private final List<String> released = new ArrayList<>();
    private final List<Collection<String>> renewals = new ArrayList<>();

    @BeforeEach
    void wireLifecycle() {
        fixture = new QueryTerminalControlFixture(new InMemoryQueryStateJournal(), java.util.Optional.of(new ReaderPinLifecycle() {
            @Override
            public void release(String queryId) {
                released.add(queryId);
            }

            @Override
            public void renew(Collection<String> queryIds) {
                renewals.add(List.copyOf(queryIds));
            }
            @Override
            public long renewIntervalNanos() {
                return java.time.Duration.ofSeconds(20).toNanos();
            }
        }));
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
    void renewalCoversPhysicallyLiveQueries() {
        String live = fixture.registerRunningQuery();

        fixture.retry();

        assertThat(renewals).isNotEmpty();
        assertThat(renewals.get(renewals.size() - 1)).contains(live);
    }
    @Test
    void cancelledQueryRenewsPinsUntilItsRealIssuanceOperationExits() {
        String queryId = fixture.registerRunningQuery();
        var issuance = fixture.enter(queryId);
        fixture.manager.cancelQuery(queryId);
        assertThat(fixture.manager.getQueryState(queryId))
                .contains(org.pangrova.query.api.lifecycle.QueryState.CANCELLED);
        assertThat(released).doesNotContain(queryId);

        fixture.retry();
        assertThat(renewals.get(renewals.size() - 1)).contains(queryId);
        assertThat(released).doesNotContain(queryId);

        issuance.close();
        fixture.retry();
        assertThat(released).containsExactly(queryId);
        renewals.clear();
        fixture.retry();
        assertThat(renewals).isEmpty();
    }

    @Test
    void finishedQueryStillRenewsWhilePhysicalCleanupWaits() {
        String queryId = fixture.registerRunningQuery();
        var issuance = fixture.enter(queryId);
        fixture.manager.onAllTasksFinished(queryId);
        fixture.retry();
        assertThat(renewals.get(renewals.size() - 1)).contains(queryId);
        assertThat(released).doesNotContain(queryId);
        issuance.close();
        fixture.retry();
        assertThat(released).containsExactly(queryId);
    }

}
