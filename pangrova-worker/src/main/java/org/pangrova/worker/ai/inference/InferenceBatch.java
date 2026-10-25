package org.pangrova.worker.ai.inference;

import org.pangrova.spi.ai.AiInvocationException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.IntFunction;

/** Owns every accepted child until it returns, including after partial submission failure. */
final class InferenceBatch {
    private InferenceBatch() {}

    static <T> List<T> execute(int size, Executor executor, IntFunction<T> invocation) {
        var accepted = new ArrayList<CompletableFuture<T>>(size);
        var failures = new ArrayList<Throwable>();
        for (int index = 0; index < size; index++) {
            int ordinal = index;
            try {
                accepted.add(CompletableFuture.supplyAsync(() -> invocation.apply(ordinal), executor));
            } catch (RuntimeException | Error failure) {
                failures.add(failure);
                break;
            }
        }
        var results = new ArrayList<T>(accepted.size());
        for (var child : accepted) {
            try {
                // Do not cancel the future: cancellation is not proof that its supplier exited.
                results.add(child.join());
            } catch (CompletionException failure) {
                failures.add(unwrap(failure));
            }
        }
        if (!failures.isEmpty()) {
            throw aggregate(failures);
        }
        return List.copyOf(results);
    }

    private static RuntimeException aggregate(List<Throwable> failures) {
        Throwable primary = failures.get(0);
        for (var failure : failures) {
            if (priority(failure) > priority(primary)) {
                primary = failure;
            }
        }
        RuntimeException aggregate = primary instanceof AiInvocationException typed
                ? new AiInvocationException(typed.category(), typed.retryable(),
                        "AI batch failed after all accepted requests exited", primary)
                : new CompletionException("AI batch failed after all accepted requests exited", primary);
        for (var failure : failures) {
            if (failure != primary) {
                aggregate.addSuppressed(failure);
            }
        }
        return aggregate;
    }

    private static int priority(Throwable failure) {
        if (failure instanceof Error) {
            return 2;
        }
        return failure instanceof AiInvocationException typed
                && typed.category() == AiInvocationException.Category.PROVIDER_OUTCOME_UNCERTAIN ? 1 : 0;
    }

    private static Throwable unwrap(Throwable failure) {
        while (failure instanceof CompletionException && failure.getCause() != null) {
            failure = failure.getCause();
        }
        return failure;
    }
}
