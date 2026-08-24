package org.pangrova.observability.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import org.pangrova.spi.observability.MetricAttributes;

/**
 * Converts Pangrova's {@link MetricAttributes} to OpenTelemetry's {@link Attributes}.
 *
 * <p>Package-private utility used by OTel metric instrument wrappers.
 */
final class OtelAttributeConverter {

    private OtelAttributeConverter() {
    }

    /**
     * Converts the given metric attributes to OTel attributes.
     * Returns {@link Attributes#empty()} for null or empty input.
     */
    static Attributes toOtel(MetricAttributes attributes) {
        if (attributes == null || attributes.isEmpty()) {
            return Attributes.empty();
        }
        AttributesBuilder builder = Attributes.builder();
        attributes.asMap().forEach((key, value) ->
                builder.put(io.opentelemetry.api.common.AttributeKey.stringKey(key), value));
        return builder.build();
    }
}
