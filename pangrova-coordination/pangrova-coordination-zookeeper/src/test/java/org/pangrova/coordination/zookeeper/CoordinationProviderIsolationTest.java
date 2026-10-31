package org.pangrova.coordination.zookeeper;

import org.junit.jupiter.api.Test;
import org.pangrova.coordination.api.CoordinationProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CoordinationProviderIsolationTest {
    @Test
    void providerLinksWithoutCoordinatorRuntime() {
        assertThat(CoordinationProvider.class.isAssignableFrom(ZooKeeperLeaseProvider.class)).isTrue();
        var loader = ZooKeeperLeaseProvider.class.getClassLoader();
        assertThatThrownBy(() -> loader.loadClass("org.pangrova.coordinator.bootstrap.CoordinatorBootstrap"))
                .isInstanceOf(ClassNotFoundException.class);
        assertThatThrownBy(() -> loader.loadClass("org.pangrova.coordinator.coordination.api.CoordinationProvider"))
                .isInstanceOf(ClassNotFoundException.class);
    }
}
