package org.pangrova.persistence.query.postgresql.journal;

import java.util.UUID;

/** Query and task events use distinct journal tables and UUID namespaces. */
public record JournalEventKey(boolean query, UUID eventId) {}
