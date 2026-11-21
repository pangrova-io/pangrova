package org.pangrova.testing.testkit;

import net.jqwik.api.lifecycle.SkipExecutionHook;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ContainerTestBaseTest {

    @Test
    void protectedNetworkRemainsTheOwnersBorrowedHandleForSubclasses() {
        Network previous = ContainerTestBase.sharedNetwork;
        try (Network owned = Network.newNetwork()) {
            NetworkConsumer.attach(owned);
            assertThat(NetworkConsumer.borrow()).isSameAs(owned);
        } finally {
            ContainerTestBase.sharedNetwork = previous;
        }
    }

    private static final class NetworkConsumer extends ContainerTestBase {
        static void attach(Network network) {
            sharedNetwork = network;
        }

        static Network borrow() {
            return sharedNetwork;
        }
    }

    @Test
    void acceptsAvailableDockerRuntime() {
        assertThatCode(() -> ContainerTestBase.verifyDockerAvailable(true))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsUnavailableDockerRuntime() {
        assertThatThrownBy(() -> ContainerTestBase.verifyDockerAvailable(false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Docker is required");
    }

    @Test
    void doesNotImplementPropertySkipHook() {
        assertThat(SkipExecutionHook.class.isAssignableFrom(ContainerTestBase.class))
                .isFalse();
    }
}
