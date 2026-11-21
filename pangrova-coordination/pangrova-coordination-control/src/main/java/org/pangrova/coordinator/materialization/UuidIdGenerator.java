package org.pangrova.coordinator.materialization;

import java.util.UUID;
import org.pangrova.materialization.api.port.IdGenerator;

/** Cryptographically random identifier source for attempts, generations, and incarnations. */
public final class UuidIdGenerator implements IdGenerator {

    @Override
    public UUID newId() {
        return UUID.randomUUID();
    }
}
