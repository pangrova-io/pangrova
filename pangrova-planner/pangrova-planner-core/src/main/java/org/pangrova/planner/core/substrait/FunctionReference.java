package org.pangrova.planner.core.substrait;

/**
 * Substrait function reference with namespace and name.
 * Example: new FunctionReference("arithmetic", "add") → "arithmetic:add"
 */
public record FunctionReference(String namespace, String name) {

    public String compoundName() {
        return namespace + ":" + name;
    }
}
