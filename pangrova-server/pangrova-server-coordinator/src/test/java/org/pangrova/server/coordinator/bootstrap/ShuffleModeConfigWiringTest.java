package org.pangrova.server.coordinator.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.config.enums.ShuffleMode;

/**
 * The external shuffle service, its Coordinator lifecycle and the Worker data
 * plane were all implemented, but no config key existed: bootstrap hardcoded
 * NATIVE with an empty endpoint list, so REMOTE was unreachable from YAML.
 */
class ShuffleModeConfigWiringTest {

    @Test
    void localModeRejectsSpeculationAndNeedsNoExternalService() {
        var installed = PangrovaCoordinatorMain.buildBootstrapConfig(PangrovaConfiguration.fromYaml(Map.of(
                "coordinator.shuffle.mode", "LOCAL", "coordinator.speculation.enabled", "false")));
        assertThat(installed.shuffleMode()).isEqualTo(ShuffleMode.LOCAL);
        assertThat(CoordinatorShuffleProviders.open(installed, ignored -> { throw new AssertionError(); })).isNull();
        assertThatThrownBy(() -> {
            var unsafe = PangrovaCoordinatorMain.buildBootstrapConfig(PangrovaConfiguration.fromYaml(Map.of(
                    "coordinator.shuffle.mode", "LOCAL", "coordinator.speculation.enabled", "true")));
            CoordinatorShuffleProviders.validate(unsafe);
        }).hasMessageContaining("speculative");
    }

    @Test
    void defaultsToNativeWithoutEndpoints() {
        var config = PangrovaCoordinatorMain.buildBootstrapConfig(
                PangrovaConfiguration.fromYaml(Map.<String, String>of()));
        assertThat(config.featureFlags().shuffleMode()).isEqualTo(ShuffleMode.NATIVE);
        assertThat(config.featureFlags().shuffleEndpoints()).isEmpty();
    }

    @Test
    void remoteModeAndEndpointsAreReachableFromYaml() {
        var config = PangrovaCoordinatorMain.buildBootstrapConfig(PangrovaConfiguration.fromYaml(Map.of(
                "coordinator.shuffle.mode", "REMOTE",
                "coordinator.shuffle.endpoints", "shuffle-a:9090, shuffle-b:9091")));
        assertThat(config.featureFlags().shuffleMode()).isEqualTo(ShuffleMode.REMOTE);
        assertThat(config.featureFlags().shuffleEndpoints())
                .containsExactly("shuffle-a:9090", "shuffle-b:9091");
    }

    @Test
    void remoteWithoutEndpointsFailsBeforeStartup() {
        // Silently falling back to NATIVE would lose the durability the operator
        // asked for, so this must fail closed.
        assertThatThrownBy(() -> PangrovaCoordinatorMain.buildBootstrapConfig(
                PangrovaConfiguration.fromYaml(Map.of("coordinator.shuffle.mode", "REMOTE"))))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("coordinator.shuffle.endpoints");
    }

    @Test
    void providersValidateScopedSettingsAndRejectConflictingSettings() {
        var config = PangrovaCoordinatorMain.buildBootstrapConfig(PangrovaConfiguration.fromYaml(Map.of(
                "coordinator.shuffle.mode", "REMOTE",
                "coordinator.shuffle.provider.pangrova.endpoints", "shuffle-a:9090",
                "coordinator.shuffle.provider.other.endpoints", "ignored")));
        assertThat(config.featureFlags().shuffleProviderSettings()).containsExactly(Map.entry("endpoints", "shuffle-a:9090"));
        assertThat(config.toString()).contains("configuredShuffleProperties=[endpoints]").doesNotContain("shuffle-a:9090");
        assertThatThrownBy(() -> PangrovaCoordinatorMain.buildBootstrapConfig(PangrovaConfiguration.fromYaml(Map.of(
                "coordinator.shuffle.mode", "REMOTE",
                "coordinator.shuffle.endpoints", "shuffle-a:9090",
                "coordinator.shuffle.provider.pangrova.endpoints", "shuffle-b:9091"))))
                .isInstanceOf(PangrovaException.class).hasMessageContaining("Duplicate");
    }

    @Test
    void rejectsUnknownOrIncapableConfiguredProviderBeforeStartup() {
        assertThatThrownBy(() -> PangrovaCoordinatorMain.buildBootstrapConfig(PangrovaConfiguration.fromYaml(Map.of(
                "coordinator.shuffle.mode", "REMOTE", "coordinator.shuffle.provider", "missing"))))
                .isInstanceOf(PangrovaException.class).hasMessageContaining("Unknown shuffle coordinator provider");
        assertThatThrownBy(() -> PangrovaCoordinatorMain.buildBootstrapConfig(PangrovaConfiguration.fromYaml(Map.of(
                "coordinator.shuffle.mode", "REMOTE", "coordinator.speculation.enabled", "true",
                "coordinator.shuffle.endpoints", "shuffle-a:9090"))))
                .isInstanceOf(PangrovaException.class).hasMessageContaining("SPECULATIVE_ATTEMPT_ISOLATION");
    }

    @Test
    void rejectsDuplicateRegisteredProviderIds() {
        var factory = java.util.ServiceLoader.load(org.pangrova.shuffle.api.coordinator.ShuffleCoordinatorFactory.class)
                .stream().map(java.util.ServiceLoader.Provider::get)
                .filter(provider -> provider.providerId().equals("pangrova")).findFirst().orElseThrow();
        assertThatThrownBy(() -> CoordinatorShuffleProviders.select("pangrova", java.util.Set.of(),
                java.util.List.of(factory, factory)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Duplicate shuffle provider: pangrova");
    }

    @Test
    void blankEndpointEntriesAreIgnored() {
        var config = PangrovaCoordinatorMain.buildBootstrapConfig(PangrovaConfiguration.fromYaml(Map.of(
                "coordinator.shuffle.mode", "REMOTE",
                "coordinator.shuffle.endpoints", " shuffle-a:9090 , , ")));
        assertThat(config.featureFlags().shuffleEndpoints()).containsExactly("shuffle-a:9090");
    }

    @Test
    void endpointsWithoutRemoteModeDoNotEnableRemote() {
        var config = PangrovaCoordinatorMain.buildBootstrapConfig(PangrovaConfiguration.fromYaml(
                Map.of("coordinator.shuffle.endpoints", "shuffle-a:9090")));
        assertThat(config.featureFlags().shuffleMode()).isEqualTo(ShuffleMode.NATIVE);
    }
}
