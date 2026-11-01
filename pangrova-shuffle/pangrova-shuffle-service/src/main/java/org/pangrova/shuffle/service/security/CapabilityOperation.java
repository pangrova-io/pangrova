package org.pangrova.shuffle.service.security;

/** Authorized remote-shuffle operation encoded into an opaque capability. */
public enum CapabilityOperation {
    READ,
    WRITE,
    FINALIZE,
    CLEANUP
}
