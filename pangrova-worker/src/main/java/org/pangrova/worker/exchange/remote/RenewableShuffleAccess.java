package org.pangrova.worker.exchange.remote;

import java.util.concurrent.atomic.AtomicReference;

import org.pangrova.protocol.v2.ShuffleAccessCapability;
import org.pangrova.protocol.v2.shuffle.ShuffleAccessOperation;
import org.pangrova.shuffle.api.exchange.ExchangeTaskAttempt;

/** Task-local capability rotation at complete-batch boundaries. */
final class RenewableShuffleAccess {
    private static final long RENEWAL_MARGIN_MS = 30_000L;

    private final RemoteShuffleClient client;
    private final String shuffleId;
    private final ExchangeTaskAttempt attempt;
    private final ShuffleAccessOperation operation;
    private final AtomicReference<ShuffleAccessCapability> current;

    RenewableShuffleAccess(
            RemoteShuffleClient client,
            String shuffleId,
            ExchangeTaskAttempt attempt,
            ShuffleAccessOperation operation,
            ShuffleAccessCapability initial) {
        this.client = client;
        this.shuffleId = shuffleId;
        this.attempt = attempt;
        this.operation = operation;
        this.current = new AtomicReference<>(initial);
    }

    ShuffleAccessCapability currentAtBoundary(long taskDeadlineEpochMs) {
        ShuffleAccessCapability previous = current.get();
        long now = System.currentTimeMillis();
        if (previous.getToken().isEmpty() || previous.getExpiresAtEpochMs() <= now) {
            throw new RemoteShuffleClientException(
                    "Shuffle capability expired before renewal boundary");
        }
        if (previous.getExpiresAtEpochMs() - now > RENEWAL_MARGIN_MS) {
            return previous;
        }
        ShuffleAccessCapability renewed = client.renewDataAccess(
                shuffleId, attempt, operation, previous, taskDeadlineEpochMs);
        current.accumulateAndGet(renewed, (oldValue, newValue) ->
                oldValue.getExpiresAtEpochMs() >= newValue.getExpiresAtEpochMs()
                        ? oldValue : newValue);
        return current.get();
    }
}
