package org.pangrova.shuffle.api.spool;

import org.pangrova.common.api.annotation.Experimental;

/**
 * {@code ServiceLoader} plugin point for persistent-plane shuffle storage backends (ADR-002 D6).
 *
 * <p>Deliberately named {@code SpoolStore} to distinguish the <em>cross-task committed shuffle
 * plane</em> ({@link org.pangrova.shuffle.api.enums.ShuffleDurability#LOCAL_SPILL LOCAL_SPILL} /
 * {@link org.pangrova.shuffle.api.enums.ShuffleDurability#DURABLE DURABLE}) from <em>operator
 * spill</em> (task-local memory relief, handled inside Velox). Implementations register via
 * {@code META-INF/services}. The Java-page exchange currently ships the {@code local-fs}
 * provider. The manager validates every installed factory version and name before opening
 * the selected store.
 */
@Experimental(reason = "ADR-002 D6 persistent-plane spool SPI; evolving alongside D4 providers")
public interface SpoolStoreFactory {

    int API_VERSION = 1;

    int apiVersion();

    /** Unique provider name used by the Java-page exchange configuration. */
    String name();

    /** Builds the store backend from its resolved {@code spool.*} configuration. */
    SpoolStore create(SpoolStoreContext context);
}
