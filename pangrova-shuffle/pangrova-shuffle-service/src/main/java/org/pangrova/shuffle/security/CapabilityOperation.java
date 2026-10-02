package org.pangrova.shuffle.security;

/** Authorized remote-shuffle operation encoded into an opaque capability. */
public enum CapabilityOperation {
    READ,
    WRITE,
    FINALIZE,
    CLEANUP
}
