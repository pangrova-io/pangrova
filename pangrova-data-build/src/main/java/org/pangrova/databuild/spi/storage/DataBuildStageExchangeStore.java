package org.pangrova.databuild.spi.storage;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.databuild.api.build.DataBuildJobKey;
import org.pangrova.databuild.api.execution.DataBuildExchangeFinalization;
import org.pangrova.databuild.api.execution.DataBuildExchangeRegistration;
import org.pangrova.databuild.api.execution.DataBuildExecutionOwner;

import java.util.List;
import java.util.Optional;

/** Immutable external exchange evidence guarded by current authority and the exact execution owner. */
@Internal
public interface DataBuildStageExchangeStore {
    /** Persists the original real allocation without replacing an earlier registration. */
    void register(DataBuildJobKey job, DataBuildExecutionOwner owner, DataBuildExchangeRegistration registration);

    /** Persists strictly validated external finalization only after exact producer winners exist. */
    void finalizeExchange(DataBuildJobKey job, DataBuildExecutionOwner owner,
            DataBuildExchangeFinalization finalization);

    /** Restores immutable evidence; live access capabilities are supplied by the actual external service. */
    List<ExchangeEvidence> load(DataBuildJobKey job, DataBuildExecutionOwner owner);

    /** Immutable allocation and optional finalized-data evidence; never an access capability. */
    record ExchangeEvidence(DataBuildExchangeRegistration registration,
            Optional<DataBuildExchangeFinalization> finalization) {}
}
