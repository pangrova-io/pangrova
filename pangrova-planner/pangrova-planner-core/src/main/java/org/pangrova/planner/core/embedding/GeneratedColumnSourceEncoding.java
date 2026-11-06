package org.pangrova.planner.core.embedding;

/** Ordered text-source encoding shared by inline and durable generated expressions. */
public final class GeneratedColumnSourceEncoding {
    public static final String NAME = "ordered-unit-separator-v1";
    public static final String SEPARATOR = "\u001f";

    private GeneratedColumnSourceEncoding() {}
}
