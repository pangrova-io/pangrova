package org.pangrova.streaming.core.cdc;

import org.pangrova.streaming.api.cdc.CdcRecordConverter;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Registry that maps format IDs to {@link CdcRecordConverter} instances.
 *
 * <p>The {@code CdcPipelineLoop} looks up the converter by the stream's
 * {@code sourceFormat} field. Constructed once at bootstrap with all
 * available converters; the internal map is immutable after construction.</p>
 */
public final class CdcRecordConverterRegistry {

    private final Map<String, CdcRecordConverter> converters;

    /**
     * Build an immutable registry from the given converter list.
     *
     * @param providers available converters; last-one-wins on duplicate format IDs
     */
    public CdcRecordConverterRegistry(List<CdcRecordConverter> providers) {
        Map<String, CdcRecordConverter> map = new LinkedHashMap<>();
        for (CdcRecordConverter converter : providers) {
            map.put(converter.formatId(), converter);
        }
        this.converters = Map.copyOf(map);
    }

    /**
     * Look up a converter by format identifier.
     *
     * @param formatId format identifier (e.g. "debezium-json")
     * @return the matching converter, never null
     * @throws IllegalArgumentException if no converter is registered for the given format
     */
    public CdcRecordConverter get(String formatId) {
        CdcRecordConverter converter = converters.get(formatId);
        if (converter == null) {
            throw new IllegalArgumentException(
                    "Unsupported source format: " + formatId
                            + ". Available: " + converters.keySet());
        }
        return converter;
    }

    /**
     * Return the set of registered format identifiers.
     *
     * @return unmodifiable set of format IDs
     */
    public Set<String> supportedFormats() {
        return converters.keySet();
    }
}
