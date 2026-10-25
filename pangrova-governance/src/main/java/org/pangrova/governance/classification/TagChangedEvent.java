package org.pangrova.governance.classification;

/**
 * Event emitted when a tag is added, changed, or removed.
 */
public record TagChangedEvent(
    String catalog,
    String schema,
    String table,
    String columnName,
    String tagKey,
    String oldValue,
    String newValue,
    java.util.UUID tableId
) {}
