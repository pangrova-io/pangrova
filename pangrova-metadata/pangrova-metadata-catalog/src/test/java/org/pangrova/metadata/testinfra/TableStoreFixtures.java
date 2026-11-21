package org.pangrova.metadata.testinfra;

import java.util.Optional;
import org.pangrova.persistence.table.api.provider.TableStores;
import org.pangrova.persistence.table.memory.provider.MemoryTableStoreFactory;

/** Fresh, fixture-owned table authority for metadata tests. */
public final class TableStoreFixtures {
    private TableStoreFixtures() {}

    public static TableStores memoryTableStores() {
        return new MemoryTableStoreFactory().create(Optional.empty());
    }
}
