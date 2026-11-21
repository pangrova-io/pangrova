package org.pangrova.shuffle.api.coordinator;

import org.pangrova.shuffle.api.enums.ShuffleDataOperation;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.shuffle.api.ShuffleAccess;
import org.pangrova.shuffle.api.ShuffleServiceAddress;
import org.pangrova.shuffle.api.identity.ExchangeTaskAttempt;

/** Issues one least-privilege data grant to the exact control-bound Worker incarnation and attempt. */
@Experimental(reason = "Fenced task delegation for external exchanges")
public interface ShuffleTaskAccess {

    /** Returns an expiring grant for exactly one allocation, operation and task attempt. */
    ShuffleAccess grant(ShuffleServiceAddress destination, String allocationId, ExchangeTaskAttempt attempt,
            ShuffleDataOperation operation);
}
