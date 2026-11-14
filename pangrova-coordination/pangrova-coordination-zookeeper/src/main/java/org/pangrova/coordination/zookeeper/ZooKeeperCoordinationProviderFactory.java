package org.pangrova.coordination.zookeeper;

import org.pangrova.persistence.coordination.api.provider.CoordinationProvider;
import org.pangrova.persistence.coordination.api.provider.CoordinationProviderFactory;

import javax.sql.DataSource;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

public final class ZooKeeperCoordinationProviderFactory implements CoordinationProviderFactory {
    @Override
    public int apiVersion() {
        return API_VERSION;
    }

    @Override
    public String id() {
        return "zookeeper";
    }

    @Override
    public Set<String> aliases() {
        return Set.of("zk");
    }

    @Override
    public CoordinationProvider open(DataSource metadata, Function<String, String> setting) {
        String endpoint = setting.apply("connectString");
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalStateException("coordination lease backend=zookeeper requires "
                    + "pangrova.coordination.lease.zookeeper.connectString");
        }
        String path = Objects.requireNonNullElse(setting.apply("basePath"), "/pangrova/leases");
        return new ZooKeeperLeaseProvider(endpoint, path);
    }
}
