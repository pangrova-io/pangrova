package org.pangrova.connector.api.metadata;

import org.pangrova.common.api.annotation.Experimental;
import java.util.Optional;

/** Worker preparation evidence. Only the metadata catalog's atomic acceptance can establish COMMITTED. */
@Experimental
public record NativeWritePreparation(Outcome outcome, Optional<PreparedSourcePublication> publication) {
    @Experimental
    public enum Outcome { PREPARED, UNKNOWN }

    public NativeWritePreparation {
        java.util.Objects.requireNonNull(outcome);
        java.util.Objects.requireNonNull(publication);
        if ((outcome == Outcome.PREPARED) != publication.isPresent()) {
            throw new IllegalArgumentException("Only confirmed SDK preparation carries immutable manifest evidence");
        }
    }
}
