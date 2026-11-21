package org.pangrova.scheduler.api.retry;

import org.pangrova.common.api.annotation.Experimental;

/** Stage-owned admission for replacing one failed task with an exact attempt identity. */
@Experimental
public interface TaskRetryAdmission {
    /** Reserves the replacement stage slot and retry budget and registers its terminal route before dispatch. */
    boolean reserve(String retryTaskId);

    /** Restores the failed slot and refunds a reservation whose dispatch failed. */
    void rollback(String retryTaskId);
}
