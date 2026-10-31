package org.pangrova.streaming.api.cdc;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;
import java.util.Optional;

/**
 * SPI for persisting CDC stream registrations.
 *
 * <p>Implementations must be thread-safe. The default in-memory implementation
 * lives in {@code pangrova-streaming}; the JDBC implementation lives in
 * {@code pangrova-metadata}.</p>
 */
@Experimental
public interface CdcRegistrationStore {

    /**
     * Register or update a CDC stream registration (upsert by streamId).
     *
     * @param registration the registration to persist
     */
    void register(CdcRegistration registration);

    /**
     * Remove a CDC stream registration.
     *
     * @param streamId the stream to unregister
     * @return {@code true} if a registration was removed
     */
    boolean unregister(String streamId);

    /**
     * Retrieve a single registration by stream ID.
     *
     * @param streamId the stream identifier
     * @return the registration, or empty if not found
     */
    Optional<CdcRegistration> getRegistration(String streamId);

    /**
     * List all registrations currently stored.
     *
     * @return unmodifiable list of all registrations
     */
    List<CdcRegistration> listRegistrations();
}
