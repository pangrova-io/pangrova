package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CoordinatorResourceReleaseTest {
    @Test
    void preservesStartupFailureAndReleasesEveryRemainingResourceInOrder() {
        var startup = new IllegalStateException("failed to bind service");
        var cleanup = new IllegalStateException("failed to close resource");
        List<String> order = new ArrayList<>();
        CoordinatorResourceRelease.close(startup, () -> order.add("http"), () -> {
            order.add("query");
            throw cleanup;
        }, () -> order.add("metadata"));
        assertThat(order).containsExactly("http", "query", "metadata");
        assertThat(startup.getSuppressed()).containsExactly(cleanup);
    }

    @Test
    void normalShutdownPropagatesTheFirstFailureOnlyAfterAllReleases() {
        var first = new IllegalStateException("first release");
        var second = new IllegalArgumentException("second release");
        var released = new AtomicInteger();
        assertThatThrownBy(() -> CoordinatorResourceRelease.close(null,
                () -> { throw first; }, () -> { throw second; }, released::incrementAndGet))
                .isSameAs(first);
        assertThat(released).hasValue(1);
        assertThat(first.getSuppressed()).containsExactly(second);
    }

    @Test
    void cleanupErrorDoesNotPreventLaterResourcesFromBeingReleased() {
        var error = new AssertionError("resource shutdown error");
        var released = new AtomicInteger();
        assertThatThrownBy(() -> CoordinatorResourceRelease.close(null,
                () -> { throw error; }, released::incrementAndGet)).isSameAs(error);
        assertThat(released).hasValue(1);
    }

    @Test
    void nullResourcesAreNotInvokedAndRealResourceIdentityIsPreserved() {
        var resource = new AtomicInteger();
        CoordinatorResourceRelease.close(null, CoordinatorResourceRelease.present(null, missing -> {
            throw new AssertionError("Uninitialized resources must not be released");
        }), CoordinatorResourceRelease.present(resource, AtomicInteger::incrementAndGet));
        assertThat(resource).hasValue(1);
    }

    @Test
    void anIdenticalFailureIsNotSelfSuppressed() {
        var failure = new IllegalStateException("shared startup failure");
        CoordinatorResourceRelease.close(failure, () -> { throw failure; });
        assertThat(failure.getSuppressed()).isEmpty();
    }
}
