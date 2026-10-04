package org.pangrova.worker.bootstrap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.pangrova.worker.bootstrap.phase.NativeFunctionBootstrap;
import org.pangrova.worker.jni.NativeFunctionConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Closeable-owner boundary tests, not function execution or Native E2E. */
class NativeFunctionResourceOwnershipTest {
    @Test void closesAllIndependentOwnersInReverseOrderAndRetainsOnlyFailedOwners() {
        var refs = new WorkerComponentRefs();
        var order = new ArrayList<String>();
        var blocked = new AtomicBoolean(true);
        AutoCloseable first = () -> order.add("first");
        AutoCloseable middle = () -> {
            order.add("middle");
            if (blocked.get()) throw new IOException("close remains blocked");
        };
        AutoCloseable last = () -> order.add("last");
        refs.retainNativeFunctionResource(first);
        refs.retainNativeFunctionResource(middle);
        refs.retainNativeFunctionResource(last);
        assertThatThrownBy(() -> NativeFunctionBootstrap.close(refs)).isInstanceOf(IllegalStateException.class)
                .hasCauseInstanceOf(IOException.class);
        assertThat(order).containsExactly("last", "middle", "first");
        assertThat(refs.nativeFunctionResources()).containsExactly(middle);
        blocked.set(false);
        NativeFunctionBootstrap.close(refs);
        NativeFunctionBootstrap.close(refs);
        assertThat(order).containsExactly("last", "middle", "first", "middle");
        assertThat(refs.nativeFunctionResources()).isEmpty();
    }

    @Test void errorDoesNotLoseTheExactFailedOwnerOrSkipOtherOwners() {
        var refs = new WorkerComponentRefs();
        var actualError = new AssertionError("resource close error");
        var blocked = new AtomicBoolean(true);
        var otherClosed = new AtomicBoolean();
        AutoCloseable failed = () -> { if (blocked.get()) throw actualError; };
        refs.retainNativeFunctionResource(() -> otherClosed.set(true));
        refs.retainNativeFunctionResource(failed);
        assertThatThrownBy(() -> NativeFunctionBootstrap.close(refs)).isSameAs(actualError);
        assertThat(otherClosed).isTrue();
        assertThat(refs.nativeFunctionResources()).containsExactly(failed);
        blocked.set(false);
        NativeFunctionBootstrap.close(refs);
        assertThat(refs.nativeFunctionResources()).isEmpty();
    }

    @Test void outstandingOwnerPreventsAnotherFunctionInitialization() throws IOException {
        var refs = new WorkerComponentRefs();
        var pipe = java.nio.channels.Pipe.open();
        try (var actualOwner = pipe.source(); var sink = pipe.sink()) {
            refs.retainNativeFunctionResource(actualOwner);
            assertThatThrownBy(() -> NativeFunctionBootstrap.initialize(refs, NativeFunctionConfig.DEFAULT))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("owners have not drained");
            assertThat(refs.nativeFunctionResources()).isEqualTo(List.of(actualOwner));
            assertThat(refs.nativeFunctionBridge()).isNull();
            NativeFunctionBootstrap.close(refs);
            assertThat(actualOwner.isOpen()).isFalse();
        }
    }
}
