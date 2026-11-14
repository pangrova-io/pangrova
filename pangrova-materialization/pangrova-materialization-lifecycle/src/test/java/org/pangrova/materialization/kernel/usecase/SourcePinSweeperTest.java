package org.pangrova.materialization.kernel.usecase;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.materialization.kernel.InMemoryMaterializationRepository;
import org.pangrova.materialization.kernel.MaterializationTestFixtures;
import org.pangrova.materialization.api.domain.AttemptId;
import org.pangrova.materialization.api.domain.Generation;
import org.pangrova.materialization.api.domain.GenerationId;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.enums.MaterializationKind;
import org.pangrova.materialization.api.domain.MaterializationObject;
import org.pangrova.materialization.api.domain.MaterializationWriteCapability;
import org.pangrova.materialization.api.domain.PhysicalTableRef;
import org.pangrova.materialization.api.enums.PinState;
import org.pangrova.materialization.api.domain.RefreshAttempt;
import org.pangrova.materialization.api.domain.RefreshKind;
import org.pangrova.materialization.api.enums.RefreshMode;
import org.pangrova.materialization.api.enums.SecurityMode;
import org.pangrova.materialization.api.domain.SourcePin;
import org.pangrova.materialization.api.domain.SourceVector;
import org.pangrova.materialization.api.port.Clock;
import org.pangrova.materialization.api.port.MaterializationEvent;
import org.pangrova.materialization.api.enums.MaterializationEventType;
import org.pangrova.connector.api.materialization.PinnedSource;
import org.pangrova.connector.api.materialization.SourceRef;
import org.pangrova.materialization.usecase.SourcePinSweeper;

class SourcePinSweeperTest {

    private static final MaterializationId OBJECT = MaterializationTestFixtures.objectId("orders_dt");
    private static final Duration GRACE = Duration.ofMinutes(2);

    private MutableClock clock;
    private InMemoryMaterializationRepository repository;
    private MaterializationTestFixtures.RecordingPins pins;
    private SourcePinSweeper sweeper;
    private int candidateDiscriminator;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(MaterializationTestFixtures.NOW);
        repository = new InMemoryMaterializationRepository(clock);
        pins = new MaterializationTestFixtures.RecordingPins();
        sweeper = new SourcePinSweeper(repository, pins, clock);
    }

    @Test
    void reclaimsSettledAttemptsOnlyAfterTheGracePeriod() {
        SourceRef orders = MaterializationTestFixtures.source("orders");
        SourceRef customers = MaterializationTestFixtures.source("customers");
        Reserved reserved = reserve(OBJECT, List.of(orders, customers),
                MaterializationTestFixtures.NOW.plus(Duration.ofHours(2)));
        repository.putAttempt(reserved.attempt().fail(
                "execution failed", MaterializationTestFixtures.NOW.plusSeconds(30)));

        clock.set(MaterializationTestFixtures.NOW.plus(GRACE));
        assertThat(sweeper.sweep(GRACE, 10))
                .isEqualTo(new SourcePinSweeper.SweepReport(0, 0, 0));
        assertThat(repository.sourcePins(reserved.attemptId()))
                .allSatisfy(pin -> assertThat(pin.state()).isEqualTo(PinState.ACQUIRED));

        clock.set(MaterializationTestFixtures.NOW.plus(Duration.ofMinutes(5)));
        assertThat(sweeper.sweep(GRACE, 10))
                .isEqualTo(new SourcePinSweeper.SweepReport(2, 2, 0));

        assertThat(pins.released()).containsExactlyInAnyOrder(
                held(reserved, orders), held(reserved, customers));
        assertThat(repository.sourcePins(reserved.attemptId()))
                .allSatisfy(pin -> assertThat(pin.state()).isEqualTo(PinState.RELEASED));
    }

    @Test
    void keepsHoldsOfLiveAttemptsWithinTheirTtl() {
        Reserved reserved = reserve(OBJECT, List.of(MaterializationTestFixtures.source("orders")),
                MaterializationTestFixtures.NOW.plus(Duration.ofHours(2)));

        clock.set(MaterializationTestFixtures.NOW.plus(Duration.ofMinutes(10)));
        assertThat(sweeper.sweep(GRACE, 10))
                .isEqualTo(new SourcePinSweeper.SweepReport(0, 0, 0));

        assertThat(pins.released()).isEmpty();
        assertThat(repository.sourcePins(reserved.attemptId()))
                .allSatisfy(pin -> assertThat(pin.state()).isEqualTo(PinState.ACQUIRED));
    }

    @Test
    void reclaimsExpiredHoldsOfUnsettledAttempts() {
        Reserved reserved = reserve(OBJECT, List.of(MaterializationTestFixtures.source("orders")),
                MaterializationTestFixtures.NOW.plus(Duration.ofMinutes(1)));

        clock.set(MaterializationTestFixtures.NOW.plus(Duration.ofMinutes(2)));
        assertThat(sweeper.sweep(GRACE, 10))
                .isEqualTo(new SourcePinSweeper.SweepReport(1, 1, 0));

        assertThat(pins.released()).hasSize(1);
        assertThat(repository.sourcePins(reserved.attemptId()))
                .allSatisfy(pin -> assertThat(pin.state()).isEqualTo(PinState.RELEASED));
    }

    @Test
    void keepsDurablePinsAcquiredWhenTheProviderFailsAndRetriesThem() {
        Reserved reserved = reserve(OBJECT, List.of(MaterializationTestFixtures.source("orders")),
                MaterializationTestFixtures.NOW.plus(Duration.ofHours(2)));
        repository.putAttempt(reserved.attempt().fail(
                "execution failed", MaterializationTestFixtures.NOW.plusSeconds(30)));
        clock.set(MaterializationTestFixtures.NOW.plus(Duration.ofMinutes(5)));
        pins.failUnpin(new IllegalStateException("tag release failed"));

        assertThat(sweeper.sweep(GRACE, 10))
                .isEqualTo(new SourcePinSweeper.SweepReport(1, 0, 1));
        assertThat(repository.sourcePins(reserved.attemptId()))
                .allSatisfy(pin -> assertThat(pin.state()).isEqualTo(PinState.ACQUIRED));

        pins.recoverUnpins();
        assertThat(sweeper.sweep(GRACE, 10))
                .isEqualTo(new SourcePinSweeper.SweepReport(1, 1, 0));
        assertThat(repository.sourcePins(reserved.attemptId()))
                .allSatisfy(pin -> assertThat(pin.state()).isEqualTo(PinState.RELEASED));
    }

    @Test
    void capsTheNumberOfAttemptsExaminedPerTick() {
        Reserved first = reserve(OBJECT, List.of(MaterializationTestFixtures.source("orders")),
                MaterializationTestFixtures.NOW.plus(Duration.ofHours(2)));
        Reserved second = reserve(MaterializationTestFixtures.objectId("customers_dt"),
                List.of(MaterializationTestFixtures.source("customers")),
                MaterializationTestFixtures.NOW.plus(Duration.ofHours(3)));
        Instant settledAt = MaterializationTestFixtures.NOW.plusSeconds(30);
        repository.putAttempt(first.attempt().fail("execution failed", settledAt));
        repository.putAttempt(second.attempt().fail("execution failed", settledAt));
        clock.set(MaterializationTestFixtures.NOW.plus(Duration.ofMinutes(5)));

        assertThat(sweeper.sweep(GRACE, 1))
                .isEqualTo(new SourcePinSweeper.SweepReport(1, 1, 0));

        assertThat(repository.sourcePins(first.attemptId()))
                .allSatisfy(pin -> assertThat(pin.state()).isEqualTo(PinState.RELEASED));
        assertThat(repository.sourcePins(second.attemptId()))
                .allSatisfy(pin -> assertThat(pin.state()).isEqualTo(PinState.ACQUIRED));
    }

    private PinnedSource held(Reserved reserved, SourceRef source) {
        return new PinnedSource(MaterializationTestFixtures.TENANT.value(), source,
                reserved.vector().snapshots().get(source), reserved.attemptId().value().toString());
    }

    private Reserved reserve(MaterializationId object, List<SourceRef> sources, Instant expiresAt) {
        SourceVector vector = MaterializationTestFixtures.vector(sources.toArray(SourceRef[]::new));
        MaterializationObject header = MaterializationObject.creating(
                object, MaterializationKind.DYNAMIC_TABLE,
                MaterializationTestFixtures.INCARNATION, MaterializationTestFixtures.DEFINER,
                SecurityMode.DEFINER, RefreshMode.FULL, Optional.empty(),
                MaterializationTestFixtures.NOW);
        repository.putObject(header);
        AttemptId attemptId = new AttemptId(UUID.randomUUID());
        GenerationId generationId = new GenerationId(UUID.randomUUID());
        PhysicalTableRef candidate =
                MaterializationTestFixtures.candidate(candidateDiscriminator++);
        RefreshAttempt attempt = MaterializationTestFixtures.attempt(object, attemptId,
                candidate, RefreshKind.Full.INSTANCE, vector, Optional.empty());
        Generation generation = MaterializationTestFixtures.candidateGeneration(object,
                generationId, attemptId, candidate, RefreshKind.Full.INSTANCE, vector,
                Optional.empty());
        MaterializationWriteCapability capability = new MaterializationWriteCapability(
                object, MaterializationTestFixtures.INCARNATION, 1, attemptId, candidate,
                generationId, vector.canonicalHash(), MaterializationTestFixtures.DEFINER, 0,
                MaterializationTestFixtures.NOW,
                MaterializationTestFixtures.NOW.plus(Duration.ofMinutes(30)));
        List<SourcePin> durable = vector.snapshots().entrySet().stream()
                .map(entry -> SourcePin.acquired(
                        attemptId, entry.getKey(), entry.getValue(), expiresAt))
                .toList();
        repository.reserveAttempt(attempt, generation, durable, capability, 0,
                new MaterializationEvent(UUID.randomUUID().toString(),
                        MaterializationEventType.ATTEMPT_RESERVED, object, Optional.empty(),
                        Optional.of(attemptId), Optional.of(generationId), Optional.empty(),
                        MaterializationTestFixtures.REQUESTER,
                        MaterializationTestFixtures.DEFINER, "reserved", MaterializationTestFixtures.NOW));
        return new Reserved(attempt, attemptId, generationId, vector);
    }

    private record Reserved(RefreshAttempt attempt, AttemptId attemptId,
            GenerationId generationId, SourceVector vector) {
    }

    private static final class MutableClock implements Clock {

        private Instant now;

        private MutableClock(Instant now) {
            this.now = now;
        }

        private void set(Instant now) {
            this.now = now;
        }

        @Override
        public Instant now() {
            return now;
        }
    }
}
