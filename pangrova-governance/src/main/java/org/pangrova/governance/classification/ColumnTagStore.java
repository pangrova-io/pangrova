package org.pangrova.governance.classification;

import java.util.List;
import java.util.Map;

/**
 * Persistence contract for column governance tags (e.g. pii_type=PII_EMAIL).
 * Implementations must be thread-safe.
 *
 * <p>Tags stored here drive the {@link TagBasedSecurityPolicy}: on coordinator
 * startup, all persisted tags are replayed through {@link ColumnTagService} so
 * that auto-masking rules are re-generated from the authoritative tag state.</p>
 */
public interface ColumnTagStore {

    /**
     * Persists a tag assignment for a column.
     *
     * @param catalog    catalog name
     * @param schema     schema name
     * @param table      table name
     * @param columnName column name
     * @param tagKey     tag key
     * @param tagValue   tag value (must not be null)
     */
    void saveTag(String catalog, String schema, String table,
                 String columnName, String tagKey, String tagValue);

    /**
     * Removes a specific tag from a column.
     *
     * @param catalog    catalog name
     * @param schema     schema name
     * @param table      table name
     * @param columnName column name
     * @param tagKey     tag key to remove
     */
    void deleteTag(String catalog, String schema, String table,
                   String columnName, String tagKey);

    /**
     * Returns all tags for a specific column.
     *
     * @return map of tagKey to tagValue (never null, may be empty)
     */
    Map<String, String> getTags(String catalog, String schema, String table, String columnName);

    /**
     * Returns all tag assignments in the store. Used at startup to replay
     * existing tags through the event system for auto-masking activation.
     *
     * @return list of all tag entries (never null, may be empty)
     */
    List<ColumnTagEntry> getAllTags();

    /**
     * A single column tag assignment.
     */
    record ColumnTagEntry(
            String catalog,
            String schema,
            String table,
            String columnName,
            String tagKey,
            String tagValue
    ) {}
}
