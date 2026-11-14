package org.pangrova.connector.api.metadata;

import org.pangrova.connector.api.enums.NativeWritePreparationOutcome;
import org.pangrova.common.api.annotation.Experimental;
import java.util.Optional;

/** Worker preparation evidence. Only the metadata catalog's atomic acceptance can establish COMMITTED. */
@Experimental
public record NativeWritePreparation(NativeWritePreparationOutcome outcome, Optional<PreparedSourcePublication> publication) {

    public NativeWritePreparation {
        java.util.Objects.requireNonNull(outcome);
        java.util.Objects.requireNonNull(publication);
        if ((outcome == NativeWritePreparationOutcome.PREPARED) != publication.isPresent()) {
            throw new IllegalArgumentException("Only confirmed SDK preparation carries immutable manifest evidence");
        }
    }
}
