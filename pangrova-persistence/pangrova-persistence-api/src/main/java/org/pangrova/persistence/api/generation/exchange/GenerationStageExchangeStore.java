package org.pangrova.persistence.api.generation.exchange;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.spi.generation.job.GenerationJobKey;
import org.pangrova.spi.generation.execution.GenerationExchangeFinalization;
import org.pangrova.spi.generation.execution.GenerationExchangeRegistration;
import org.pangrova.spi.generation.execution.GenerationExecutionOwner;

import java.util.List;
import java.util.Optional;

/** Immutable external exchange evidence guarded by current authority and the exact execution owner. */
@Internal
public interface GenerationStageExchangeStore {
    /** Persists the original real allocation without replacing an earlier registration. */
    void register(GenerationJobKey job, GenerationExecutionOwner owner, GenerationExchangeRegistration registration);

    /** Persists strictly validated external finalization only after exact producer winners exist. */
    void finalizeExchange(GenerationJobKey job, GenerationExecutionOwner owner,
            GenerationExchangeFinalization finalization);

    /** Restores immutable evidence; live access capabilities are supplied by the actual external service. */
    List<ExchangeEvidence> load(GenerationJobKey job, GenerationExecutionOwner owner);

    /** Immutable allocation and optional finalized-data evidence; never an access capability. */
    record ExchangeEvidence(GenerationExchangeRegistration registration,
            Optional<GenerationExchangeFinalization> finalization) {}
}
