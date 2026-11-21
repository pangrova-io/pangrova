package org.pangrova.materialization.kernel.usecase;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.materialization.kernel.InMemoryMaterializationRepository;
import org.pangrova.materialization.api.domain.GenerationId;
import org.pangrova.execution.api.identity.QueryId;
import org.pangrova.materialization.api.domain.ReaderId;
import org.pangrova.materialization.api.domain.ReaderPin;
import org.pangrova.materialization.api.port.Clock;
import org.pangrova.materialization.usecase.ReaderPinLeases;

class ReaderPinLeasesTest {

    private static final Duration LEASE = Duration.ofMinutes(5);
    private static final Instant NOW = Instant.parse("2026-03-01T00:00:00Z");

    private final MutableClock clock = new MutableClock(NOW);
    private final GenerationId first = new GenerationId(UUID.randomUUID());
    private final GenerationId second = new GenerationId(UUID.randomUUID());
    private InMemoryMaterializationRepository repository;
    private ReaderPinLeases leases;

    @BeforeEach
    void setUp() {
        repository = new InMemoryMaterializationRepository(clock);
        leases = new ReaderPinLeases(repository, clock, LEASE);
    }

    @Test
    void leasesEveryScannedGenerationAndReleasesThemTogether() {
        QueryId query = new QueryId(UUID.randomUUID().toString());

        leases.acquire(first, query, "alice");
        leases.acquire(second, query, "alice");

        assertThat(repository.findReaderPins(first))
                .singleElement()
                .satisfies(pin -> {
                    assertThat(pin.reader()).isEqualTo(ReaderId.query(query));
                    assertThat(pin.owner()).isEqualTo("alice");
                    assertThat(pin.expiresAt()).isEqualTo(NOW.plus(LEASE));
                    assertThat(pin.heartbeatAt()).isEqualTo(NOW);
                });
        assertThat(repository.findReaderPins(second)).hasSize(1);

        leases.release(query);

        assertThat(repository.findReaderPins(first)).isEmpty();
        assertThat(repository.findReaderPins(second)).isEmpty();
    }

    @Test
    void renewExtendsTheLeaseOfLiveQueries() {
        QueryId query = new QueryId(UUID.randomUUID().toString());
        leases.acquire(first, query, "alice");
        clock.advance(LEASE.dividedBy(2));

        leases.renew(List.of(query));

        ReaderPin pin = repository.findReaderPins(first).get(0);
        assertThat(pin.expiresAt()).isEqualTo(clock.now().plus(LEASE));
        assertThat(pin.heartbeatAt()).isEqualTo(clock.now());
    }

    @Test
    void releaseIgnoresQueriesThisCoordinatorNeverLeased() {
        QueryId foreign = new QueryId(UUID.randomUUID().toString());
        repository.pinReader(new ReaderPin(first, ReaderId.query(foreign), "bob",
                NOW.plus(LEASE), NOW));

        leases.release(foreign);

        assertThat(repository.findReaderPins(first)).hasSize(1);
    }

    @Test
    void purgeRemovesOnlyExpiredLeases() {
        QueryId live = new QueryId(UUID.randomUUID().toString());
        QueryId dead = new QueryId(UUID.randomUUID().toString());
        leases.acquire(first, live, "alice");
        leases.acquire(second, dead, "bob");
        clock.advance(LEASE.plusSeconds(1));

        int purged = leases.purgeExpired();

        assertThat(purged).isEqualTo(2);
        assertThat(repository.findReaderPins(first)).isEmpty();
        assertThat(repository.findReaderPins(second)).isEmpty();
    }

    @Test
    void renewalPeriodStaysWellInsideTheLease() {
        assertThat(leases.renewInterval()).isEqualTo(LEASE.dividedBy(3));
    }

    private static final class MutableClock implements Clock {
        private Instant now;

        private MutableClock(Instant now) {
            this.now = now;
        }

        @Override
        public Instant now() {
            return now;
        }

        private void advance(Duration duration) {
            now = now.plus(duration);
        }
    }
}
