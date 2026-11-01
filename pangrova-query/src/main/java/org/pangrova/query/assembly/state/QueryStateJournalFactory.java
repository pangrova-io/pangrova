package org.pangrova.query.assembly.state;

import javax.sql.DataSource;

import org.pangrova.persistence.jdbc.query.state.JdbcQueryStateJournal;
import org.pangrova.persistence.memory.query.state.InMemoryQueryStateJournal;
import org.pangrova.persistence.api.query.state.QueryStateJournal;
import org.pangrova.spi.query.state.StateJournalConfig;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Creates the appropriate {@link QueryStateJournal} implementation
 * based on {@link StateJournalConfig#type()}.
 *
 * <ul>
 *   <li>{@code "NONE"} → {@link InMemoryQueryStateJournal} (no persistence)</li>
 *   <li>{@code "JDBC"} → {@link JdbcQueryStateJournal} (PostgreSQL-backed)</li>
 * </ul>
 */
public final class QueryStateJournalFactory {

    private QueryStateJournalFactory() {
        // utility class
    }

    /**
     * Creates a journal from the given config.
     *
     * @param config     journal configuration (required)
     * @param dataSource JDBC data source (required when type is "JDBC", may be null for "NONE")
     * @return the journal implementation matching the config type
     * @throws IllegalArgumentException if the type is unknown or JDBC is requested without a data source
     */
    public static QueryStateJournal create(StateJournalConfig config, DataSource dataSource) {
        checkNotNull(config, "config must not be null");
        return switch (config.type()) {
            case "NONE" -> new InMemoryQueryStateJournal(config.maxResidentEvents());
            case "JDBC" -> {
                if (dataSource == null) {
                    throw new IllegalArgumentException(
                            "DataSource is required for JDBC state journal");
                }
                yield new JdbcQueryStateJournal(dataSource, config);
            }
            default -> throw new IllegalArgumentException(
                    "Unknown state journal type: " + config.type());
        };
    }
}
