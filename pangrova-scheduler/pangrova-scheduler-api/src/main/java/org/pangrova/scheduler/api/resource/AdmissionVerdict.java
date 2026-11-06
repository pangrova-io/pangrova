package org.pangrova.scheduler.api.resource;


import org.pangrova.common.api.annotation.Experimental;

/**
 * Outcome of a {@link org.pangrova.scheduler.spi.admission.ClusterAdmissionController#admit} evaluation.
 */
@Experimental(reason = "G-RES-04: cluster admission control SPI")
public enum AdmissionVerdict {

    /** The query is admitted and may proceed to scheduling. */
    ADMIT,

    /** The query is queued because the resource group is at capacity. */
    QUEUE,

    /** The query is rejected because a hard quota limit has been reached. */
    REJECT
}
