package org.pangrova.persistence.coordination.api.provider;

import org.pangrova.common.api.annotation.Experimental;

import javax.sql.DataSource;
import java.util.Set;
import java.util.function.Function;

/** Discovered on the Coordinator role classpath. A created provider owns its backend resources. */
@Experimental
public interface CoordinationProviderFactory {
    String id();

    default Set<String> aliases() {
        return Set.of();
    }

    int API_VERSION = 1;

    /** The contract version this implementation was compiled against. */
    int apiVersion();

    /**
     * Borrows the process metadata pool. The lookup exposes only settings under this provider's
     * canonical configuration prefix. An absent setting returns null. Failed opens release their resources.
     */
    CoordinationProvider open(DataSource metadata, Function<String, String> setting);
}
