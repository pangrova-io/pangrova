package org.pangrova.protocol.retrieval;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import io.substrait.proto.Plan;

/** Builds a validated function-anchor index for one Substrait plan. */
final class SubstraitFunctionNames {
    private SubstraitFunctionNames() {}

    static Map<Integer, String> index(Plan plan) {
        Map<Integer, String> names = new HashMap<>();
        for (var extension : plan.getExtensionsList()) {
            if (!extension.hasExtensionFunction()) {
                continue;
            }
            String raw = extension.getExtensionFunction().getName();
            int separator = raw.indexOf(':');
            String name = (separator < 0 ? raw : raw.substring(0, separator))
                    .toLowerCase(Locale.ROOT);
            if (names.put(extension.getExtensionFunction().getFunctionAnchor(), name) != null) {
                throw new IllegalArgumentException("Duplicate Substrait function anchor");
            }
        }
        return Map.copyOf(names);
    }
}
