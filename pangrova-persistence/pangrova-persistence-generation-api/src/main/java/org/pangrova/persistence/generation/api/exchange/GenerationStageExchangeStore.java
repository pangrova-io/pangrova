package org.pangrova.persistence.generation.api.exchange;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.generation.api.identity.GenerationJobKey;
import org.pangrova.generation.api.exchange.GenerationExchangeFinalization;
import org.pangrova.generation.api.exchange.GenerationExchangeRegistration;
import org.pangrova.generation.api.execution.GenerationExecutionOwner;

import java.util.List;
import java.util.Optional;

/** Immutable external exchange evidence guarded by current authority and the exact execution owner. */
@Internal
public interface GenerationStageExchangeStore {
    /** Returns the original durable allocation, preserving the first issuer when the current owner replays it. */
    GenerationExchangeRegistration register(
            GenerationJobKey job, GenerationExecutionOwner owner, GenerationExchangeRegistration registration);

    /** Resolves just the requested exchange and its registration under current authority. */
    Optional<GenerationExchangeRegistration> findRegistration(
            GenerationJobKey job, GenerationExecutionOwner owner, String exchangeId);

    /** Records the compact receipt and the exact provider message after verifying the producer winners. */
    void finalizeExchange(GenerationJobKey job, GenerationExecutionOwner owner,
            GenerationExchangeFinalization finalization, byte[] providerResponse);

    /** Restores immutable evidence; live access capabilities are supplied by the actual external service. */
    List<ExchangeEvidence> load(GenerationJobKey job, GenerationExecutionOwner owner);

    /** Immutable allocation and optional finalized-data evidence; never an access capability. */
    record ExchangeEvidence(GenerationExchangeRegistration registration,
            Optional<GenerationExchangeFinalization> finalization) {}
}
