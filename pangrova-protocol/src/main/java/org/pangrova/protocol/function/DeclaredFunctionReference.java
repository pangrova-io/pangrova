package org.pangrova.protocol.function;

import org.pangrova.protocol.v2.FunctionKind;

import java.util.Objects;

/** Canonical declaration identity; presence does not prove runtime availability. */
public record DeclaredFunctionReference(FunctionKind kind, String namespace, String name) {
    public DeclaredFunctionReference {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(name, "name");
        if (kind == FunctionKind.FUNCTION_KIND_UNSPECIFIED || kind == FunctionKind.UNRECOGNIZED
                || namespace.isBlank() || name.isBlank()) {
            throw new IllegalArgumentException("A function declaration needs an explicit kind and identity");
        }
    }
}
