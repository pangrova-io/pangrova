package org.pangrova.connector.spi.metadata;

/** Authoritative SQL head is absent or has a different table incarnation. This alone is not an abort proof. */
public final class SourceTableUnavailableException extends IllegalStateException {
    public SourceTableUnavailableException(String message) { super(message); }
}
