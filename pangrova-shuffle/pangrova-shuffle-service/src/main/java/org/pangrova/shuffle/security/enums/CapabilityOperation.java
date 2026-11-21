package org.pangrova.shuffle.security.enums;

/** Authorized remote-shuffle operation encoded into an opaque capability. */
public enum CapabilityOperation {
    READ,
    WRITE,
    FINALIZE,
    CLEANUP
}
