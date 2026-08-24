package org.pangrova.connector.iceberg.lance;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registry for file format models within the Iceberg connector.
 *
 * <p>Holds format declarations (name + extension) so the connector can
 * recognize non-standard file formats in Iceberg manifests and route
 * splits to the appropriate native reader.</p>
 *
 * <p>Concurrency strategy: ConcurrentHashMap for safe concurrent lookup
 * after startup registration.</p>
 */
@ThreadSafe
public final class FormatModelRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(FormatModelRegistry.class);

    private final Map<String, IcebergLanceFormatModel> models = new ConcurrentHashMap<>();

    /**
     * Registers a format model. Duplicate registrations for the same format
     * name are idempotent (last-write-wins with a warning).
     */
    public void register(IcebergLanceFormatModel model) {
        String name = model.formatName();
        IcebergLanceFormatModel previous = models.put(name, model);
        if (previous != null) {
            LOG.warn("event=format_model_replaced format={}", name);
        } else {
            LOG.info("event=format_model_registered format={} extension={}",
                    name, model.fileExtension());
        }
    }

    /**
     * Looks up a registered format model by name.
     */
    public Optional<IcebergLanceFormatModel> lookup(String formatName) {
        return Optional.ofNullable(models.get(formatName));
    }

    /**
     * Returns true if the given file extension belongs to a registered format.
     */
    public boolean isRegisteredExtension(String extension) {
        return models.values().stream()
                .anyMatch(m -> m.fileExtension().equals(extension));
    }
}
