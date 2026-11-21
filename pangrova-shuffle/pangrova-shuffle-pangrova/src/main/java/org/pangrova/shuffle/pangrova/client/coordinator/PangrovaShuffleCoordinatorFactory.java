package org.pangrova.shuffle.pangrova.client.coordinator;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.pangrova.shuffle.api.ShuffleExecutionOwnerResolver;
import org.pangrova.shuffle.api.ShuffleServiceAddress;
import org.pangrova.shuffle.api.capability.ShuffleCapabilityDescriptor;
import org.pangrova.shuffle.api.coordinator.ShuffleCoordinatorFactory;
import org.pangrova.shuffle.api.coordinator.ShuffleCoordinatorSession;
import org.pangrova.shuffle.api.enums.ShuffleCapability;
import org.pangrova.shuffle.pangrova.client.RemoteShuffleClientPool.PemTlsConfig;

/** Creates the Pangrova service control session and owns its endpoint and PEM configuration grammar. */
public final class PangrovaShuffleCoordinatorFactory implements ShuffleCoordinatorFactory {
    static final ShuffleCapabilityDescriptor CAPABILITIES = ShuffleCapabilityDescriptor.of(
            ShuffleCapability.BLOCKING_READ, ShuffleCapability.PARTITION_READ);
    private static final Set<String> KEYS = Set.of("endpoints", "tls.server-identity", "tls.certificate-chain",
            "tls.private-key", "tls.root-ca");

    static final String PROVIDER_ID = "pangrova";

    @Override
    public String providerId() {
        return PROVIDER_ID;
    }

    @Override
    public int apiVersion() {
        return API_VERSION;
    }

    @Override
    public ShuffleCapabilityDescriptor capabilities() {
        return CAPABILITIES;
    }

    @Override
    public void validate(Map<String, String> settings) {
        configuration(settings);
    }

    @Override
    public ShuffleCoordinatorSession open(Map<String, String> settings, ShuffleExecutionOwnerResolver owners) {
        Objects.requireNonNull(owners, "durable execution ownership");
        var configuration = configuration(settings);
        return new PangrovaShuffleCoordinator(configuration.addresses(), owners, configuration.credentials());
    }

    private record Configuration(List<ShuffleServiceAddress> addresses, PemTlsConfig credentials) {}

    private static Configuration configuration(Map<String, String> settings) {
        if (!KEYS.containsAll(settings.keySet())) {
            throw new IllegalArgumentException("Unknown Pangrova shuffle provider setting");
        }
        String endpoints = settings.get("endpoints");
        if (endpoints == null || endpoints.isBlank()) {
            throw new IllegalArgumentException("Pangrova shuffle requires coordinator.shuffle.endpoints"
                    + " or coordinator.shuffle.provider.pangrova.endpoints");
        }
        boolean tls = settings.keySet().stream().anyMatch(key -> key.startsWith("tls."));
        String identity = tls ? required(settings, "tls.server-identity") : "";
        var addresses = Arrays.stream(endpoints.split(",", -1)).map(String::strip)
                .map(endpoint -> address(endpoint, identity)).toList();
        var credentials = tls ? new PemTlsConfig(Path.of(required(settings, "tls.certificate-chain")),
                Path.of(required(settings, "tls.private-key")), Path.of(required(settings, "tls.root-ca"))) : null;
        return new Configuration(addresses, credentials);
    }

    private static String required(Map<String, String> settings, String key) {
        String value = settings.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Pangrova shuffle requires " + key);
        }
        return value;
    }

    private static ShuffleServiceAddress address(String endpoint, String identity) {
        int separator = endpoint.lastIndexOf(':');
        if (separator <= 0 || separator == endpoint.length() - 1) {
            throw new IllegalArgumentException("Invalid shuffle endpoint (expected host:port): " + endpoint);
        }
        String host = endpoint.substring(0, separator);
        if (host.startsWith("[") && host.endsWith("]")) host = host.substring(1, host.length() - 1);
        return new ShuffleServiceAddress(host, Integer.parseInt(endpoint.substring(separator + 1)), identity);
    }
}
