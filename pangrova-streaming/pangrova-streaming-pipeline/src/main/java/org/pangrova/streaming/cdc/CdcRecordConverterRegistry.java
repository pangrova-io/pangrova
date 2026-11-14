package org.pangrova.streaming.cdc;

import org.pangrova.streaming.api.cdc.CdcRecordConverter;
import org.pangrova.streaming.api.format.CdcFormatFactory;

import java.util.LinkedHashMap;
import java.util.ServiceLoader;
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
     * @param providers available converters; each format ID must be nonblank and unique
     */
    public CdcRecordConverterRegistry(List<CdcRecordConverter> providers) {
        Map<String, CdcRecordConverter> map = new LinkedHashMap<>();
        for (CdcRecordConverter converter : providers) {
            String id = converter.formatId();
            if (id == null || id.isBlank() || map.putIfAbsent(id, converter) != null) {
                throw new IllegalArgumentException("Blank or duplicate CDC format: " + id);
            }
        }
        this.converters = Map.copyOf(map);
    }

    private CdcRecordConverterRegistry(Map<String, CdcRecordConverter> decoders) {
        this.converters = Map.copyOf(decoders);
    }

    /** Discovers only format factories visible to the server's plugin class loader. */
    public static CdcRecordConverterRegistry discover(ClassLoader loader) {
        return fromFactories(ServiceLoader.load(CdcFormatFactory.class, loader));
    }

    static CdcRecordConverterRegistry fromFactories(Iterable<CdcFormatFactory> factories) {
        var byId = new LinkedHashMap<String, CdcFormatFactory>();
        for (CdcFormatFactory factory : factories) {
            String id = factory.identifier();
            if (factory.apiVersion() != CdcFormatFactory.API_VERSION) {
                throw new IllegalArgumentException("Incompatible CDC format factory: " + id);
            }
            if (id == null || id.isBlank() || byId.putIfAbsent(id, factory) != null) {
                throw new IllegalArgumentException("Blank or duplicate CDC format: " + id);
            }
        }
        var decoders = new LinkedHashMap<String, CdcRecordConverter>();
        byId.forEach((id, factory) -> {
            var converter = factory.create();
            if (converter == null || !id.equals(converter.formatId())) {
                throw new IllegalStateException("CDC factory produced an invalid decoder for " + id);
            }
            decoders.put(id, converter);
        });
        return new CdcRecordConverterRegistry(decoders);
    }

    /**
     * Look up a converter by format identifier.
     *
     * @param formatId format identifier (e.g. "debezium-json")
     * @return the matching converter, never null
     * @throws IllegalArgumentException if no converter is registered for the given format
     */
    public CdcRecordConverter get(String formatId) {
        CdcRecordConverter converter = formatId == null ? null : converters.get(formatId);
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
