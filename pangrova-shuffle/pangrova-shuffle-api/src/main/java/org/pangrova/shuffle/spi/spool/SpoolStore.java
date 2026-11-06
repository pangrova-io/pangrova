package org.pangrova.shuffle.spi.spool;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.shuffle.api.ShuffleDurability;
import org.pangrova.shuffle.api.spool.SpoolReadSelection;

/**
 * Encoded-batch storage primitive, not an exchange publication or retry protocol.
 * Implementations must not substitute another attempt or treat a missing selected segment as empty.
 * No automatic cross-process, generation or retention guarantee is implied by this interface.
 */
@Experimental(reason = "Local spool primitive; generic session and publication binding remain pending")
public interface SpoolStore extends AutoCloseable {
    /** Opens an exclusive writer for one producer attempt; an existing completed segment is immutable. */
    SpoolWriter open(String queryId, int stageId, int partitionId, int mapId, long attemptId);

    /** Reads only completed, explicitly selected attempts; preserves each encoded batch boundary. */
    SpoolReader read(SpoolReadSelection selection);

    /** Removes query data only after its readers and writers are closed; fails while resources are active. */
    void delete(String queryId);

    /** Storage-medium description, not an exchange capability declaration. */
    ShuffleDurability durability();

    /** Closes owned readers/writers and storage resources; failures must be propagated. */
    @Override
    void close();
}
