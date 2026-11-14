package org.pangrova.shuffle.api.spool;

import org.pangrova.common.api.annotation.Experimental;

/**
 * {@code ServiceLoader} plugin point for persistent-plane shuffle storage backends (ADR-002 D6).
 *
 * <p>Deliberately named {@code SpoolStore} to distinguish the <em>cross-task committed shuffle
 * plane</em> ({@link org.pangrova.shuffle.api.ShuffleDurability#LOCAL_SPILL LOCAL_SPILL} /
 * {@link org.pangrova.shuffle.api.ShuffleDurability#DURABLE DURABLE}) from <em>operator
 * spill</em> (task-local memory relief, handled inside Velox). Implementations register via
 * {@code META-INF/services} and are selected by a stable name string ({@code "local-fs"},
 * {@code "s3"}, {@code "oss"}, {@code "hdfs"}), aligning the persistent shuffle plane with the
 * connector and index-engine plugin
 * points.
 */
@Experimental(reason = "ADR-002 D6 persistent-plane spool SPI; evolving alongside D4 providers")
public interface SpoolStoreFactory {

    /** Stable id used in configuration, e.g. {@code "local-fs"} / {@code "s3"} / {@code "hdfs"}. */
    String name();

    /** Builds the store backend from its resolved {@code spool.*} configuration. */
    SpoolStore create(SpoolStoreContext context);
}
