package org.pangrova.query.store.jdbc.lineage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.pangrova.common.json.provider.jackson.JsonMapperFactory;
import org.pangrova.query.api.lineage.QueryLineageFact;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Exact typed reconciliation used by both the query journal transaction and the outbox producer. */
public final class QueryLineageFactSql {
    private static final ObjectReader READER = JsonMapperFactory.reader().forType(QueryLineageFact.class)
            .with(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .with(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES);
    private static final ObjectWriter WRITER = JsonMapperFactory.writer().forType(QueryLineageFact.class);

    private QueryLineageFactSql() { }

    /** Encodes a frozen lineage fact as the exact JSON payload stored in the outbox. */
    public static String encode(QueryLineageFact fact) throws SQLException {
        try {
            return WRITER.writeValueAsString(fact);
        } catch (JsonProcessingException failure) {
            throw new SQLException("Cannot encode frozen lineage fact", failure);
        }
    }

    /** Decodes the durable outbox payload, rejecting unknown or missing fields. */
    public static QueryLineageFact decode(String payload) throws SQLException {
        try {
            return READER.readValue(payload);
        } catch (JsonProcessingException failure) {
            throw new SQLException("Invalid durable lineage fact", failure);
        }
    }

    /** Caller owns the transaction, including rollback and final commit. */
    public static void append(Connection connection, QueryLineageFact fact) throws SQLException {
        String payload = encode(fact);
        try (PreparedStatement insert = connection.prepareStatement("""
                INSERT INTO query_lineage_outbox
                    (event_id, namespace, query_id, category, payload, created_at_ms)
                VALUES (?, ?, ?, ?, ?::jsonb, ?) ON CONFLICT (event_id) DO NOTHING
                """)) {
            insert.setObject(1, fact.eventId());
            insert.setString(2, fact.namespace());
            insert.setString(3, fact.queryId());
            insert.setString(4, fact.phase() == QueryLineageFact.Phase.START ? "START" : "TERMINAL");
            insert.setString(5, payload);
            insert.setLong(6, fact.timestampMs());
            insert.executeUpdate();
        }
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT payload::text FROM query_lineage_outbox WHERE event_id = ?")) {
            select.setObject(1, fact.eventId());
            try (ResultSet rows = select.executeQuery()) {
                if (!rows.next() || !fact.equals(decode(rows.getString(1))) || rows.next()) {
                    throw new PayloadConflict(fact.eventId());
                }
            }
        }
    }

    public static final class PayloadConflict extends SQLException {
        private static final long serialVersionUID = 1L;

        PayloadConflict(java.util.UUID identity) {
            super("Lineage identity has a conflicting immutable payload: " + identity, "23505");
        }
    }
}
