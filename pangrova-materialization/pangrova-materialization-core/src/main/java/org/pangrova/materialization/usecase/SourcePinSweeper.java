package org.pangrova.materialization.usecase;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.pangrova.materialization.domain.AttemptId;
import org.pangrova.materialization.port.Clock;
import org.pangrova.materialization.port.MaterializationRepository;
import org.pangrova.materialization.port.ReclaimableSourcePin;
import org.pangrova.materialization.port.SourcePinning;
import org.pangrova.spi.connector.materialization.PinnedSource;

/**
 * Reclaims source snapshot holds left behind by settled or expired attempts.
 *
 * <p>Every physical hold is released before its durable pins leave
 * {@code ACQUIRED}, so a provider or database failure is retried on the next
 * maintenance tick instead of forgetting a live hold. Providers must make
 * {@link SourcePinning#unpin} idempotent for the attempt-scoped pin owner
 * because a crash can leave a durable row without a matching physical hold.
 */
public final class SourcePinSweeper {

    private final MaterializationRepository repository;
    private final SourcePinning sourcePinning;
    private final Clock clock;

    /**
     * Creates a sweeper over durable source holds.
     *
     * @param repository durable repository
     * @param sourcePinning source pin release port
     * @param clock time source
     */
    public SourcePinSweeper(MaterializationRepository repository,
            SourcePinning sourcePinning, Clock clock) {
        if (repository == null || sourcePinning == null || clock == null) {
            throw new IllegalArgumentException("repository, sourcePinning, and clock are required");
        }
        this.repository = repository;
        this.sourcePinning = sourcePinning;
        this.clock = clock;
    }

    /**
     * Releases every reclaimable hold that is visible to this tick.
     *
     * @param settledGrace minimum age of a settled attempt before its pins are reclaimed
     * @param attemptLimit maximum number of distinct attempts to examine
     * @return sweep counters
     */
    public SweepReport sweep(Duration settledGrace, int attemptLimit) {
        if (settledGrace == null || settledGrace.isNegative()) {
            throw new IllegalArgumentException("settledGrace must not be negative");
        }
        if (attemptLimit < 1) {
            throw new IllegalArgumentException("attemptLimit must be positive");
        }
        Instant now = clock.now();
        List<ReclaimableSourcePin> pins = repository.findReclaimableSourcePins(
                now, now.minus(settledGrace), attemptLimit);
        Map<AttemptId, List<ReclaimableSourcePin>> byAttempt = new LinkedHashMap<>();
        for (ReclaimableSourcePin pin : pins) {
            byAttempt.computeIfAbsent(pin.attempt(), key -> new ArrayList<>()).add(pin);
        }
        int examined = 0;
        int released = 0;
        int failed = 0;
        for (Map.Entry<AttemptId, List<ReclaimableSourcePin>> entry : byAttempt.entrySet()) {
            List<ReclaimableSourcePin> attemptPins = entry.getValue();
            examined += attemptPins.size();
            RuntimeException failure = null;
            for (ReclaimableSourcePin pin : attemptPins) {
                try {
                    sourcePinning.unpin(new PinnedSource(pin.tenant().value(), pin.source(),
                            pin.snapshot(), pin.pinOwner()));
                } catch (RuntimeException unpinFailure) {
                    if (failure == null) {
                        failure = unpinFailure;
                    } else {
                        failure.addSuppressed(unpinFailure);
                    }
                }
            }
            if (failure != null) {
                failed += attemptPins.size();
                continue;
            }
            try {
                repository.releaseSourcePins(entry.getKey(), now);
                released += attemptPins.size();
            } catch (RuntimeException releaseFailure) {
                failed += attemptPins.size();
            }
        }
        return new SweepReport(examined, released, failed);
    }

    /**
     * Sweep counters.
     *
     * @param examined reclaimable pins seen
     * @param released pins whose physical hold and durable row were released
     * @param failed pins left acquired for the next sweep
     */
    public record SweepReport(int examined, int released, int failed) {
    }
}
