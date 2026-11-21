package org.pangrova.worker.enums;

/** Admission decision returned to the producer of an output batch. */
public enum BackpressureAction {
    PROCEED,
    SLOW_DOWN,
    REJECT
}
