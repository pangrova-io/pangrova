package org.pangrova.ai.worker.provider;

import org.pangrova.spi.ai.model.AiModelDescriptor;

/** Strict typed access to canonical model properties. */
final class AiModelProperties {

    private AiModelProperties() {}

    static int integer(
            AiModelDescriptor model, String property, int defaultValue) {
        String raw = model.properties().get(property);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException failure) {
            throw invalid(model, property, "must be an integer", failure);
        }
    }

    static double positiveDouble(
            AiModelDescriptor model, String property, double defaultValue) {
        String raw = model.properties().get(property);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            double value = Double.parseDouble(raw);
            if (!Double.isFinite(value) || value <= 0) {
                throw new NumberFormatException("not positive and finite");
            }
            return value;
        } catch (NumberFormatException failure) {
            throw invalid(model, property, "must be positive and finite", failure);
        }
    }

    private static IllegalArgumentException invalid(
            AiModelDescriptor model,
            String property,
            String requirement,
            NumberFormatException failure) {
        return new IllegalArgumentException(
                "AI model '" + model.name() + "' property '" + property
                        + "' " + requirement,
                failure);
    }
}
