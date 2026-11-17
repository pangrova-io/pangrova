package org.pangrova.persistence.indexing.postgresql.metadata;

import java.sql.Connection;
import java.sql.SQLException;
import org.pangrova.indexing.api.enums.IndexState;

/**
 * Serializes artifact membership writes with publication on the descriptor row.
 * The caller owns the transaction and connection. This participant neither commits
 * nor closes them. Staging-row locks also serialize writes with failed-build fencing.
 */
final class JdbcIndexMappingWriteFence {
    private JdbcIndexMappingWriteFence() {}

    static void requireBuildable(Connection connection, String indexId, int generation) throws SQLException {
        IndexState state;
        int activeGeneration;
        try (var statement = connection.prepareStatement(
                "SELECT state,generation FROM index_descriptors WHERE index_id=? FOR UPDATE")) {
            statement.setString(1, indexId);
            try (var rows = statement.executeQuery()) {
                if (!rows.next()) {
                    throw new IllegalStateException("Index disappeared before artifact write: " + indexId);
                }
                state = IndexState.valueOf(rows.getString(1));
                activeGeneration = rows.getInt(2);
            }
        }
        requireUnpublished(connection, indexId, generation);
        if (state == IndexState.BUILDING && activeGeneration == generation) {
            return;
        }
        if ((state == IndexState.READY || state == IndexState.STALE)
                && activeGeneration == generation - 1
                && hasBuildingReservation(connection, indexId, generation, activeGeneration)) {
            return;
        }
        throw new IllegalStateException("Index generation is not buildable: " + indexId + "/" + generation);
    }

    static void requireCurrentAttempt(Connection connection, String indexId, int generation, String owner)
            throws SQLException {
        try (var statement = connection.prepareStatement(
                "SELECT 1 FROM index_build_claims WHERE index_id=? AND generation=? AND owner_id=? FOR UPDATE")) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            statement.setString(3, owner);
            try (var rows = statement.executeQuery()) {
                if (!rows.next()) {
                    throw new IllegalStateException("Index build attempt lost its claim: " + indexId + "/"
                            + generation);
                }
            }
        }
    }

    static void requirePublishedSource(Connection connection, String indexId, int generation) throws SQLException {
        try (var statement = connection.prepareStatement(
                "SELECT 1 FROM index_generation_manifests WHERE index_id=? AND generation=? AND state='ACTIVE'")) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            try (var rows = statement.executeQuery()) {
                if (!rows.next()) {
                    throw new IllegalStateException("Copy source generation is not published: " + indexId);
                }
            }
        }
    }

    private static void requireUnpublished(Connection connection, String indexId, int generation) throws SQLException {
        try (var statement = connection.prepareStatement(
                "SELECT 1 FROM index_generation_manifests WHERE index_id=? AND generation=?")) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            try (var rows = statement.executeQuery()) {
                if (rows.next()) {
                    throw new IllegalStateException("Published index generation rejects artifact writes: "
                            + indexId + "/" + generation);
                }
            }
        }
    }

    private static boolean hasBuildingReservation(
            Connection connection, String indexId, int generation, int baseGeneration) throws SQLException {
        try (var statement = connection.prepareStatement(
                "SELECT state FROM index_generation_builds "
                        + "WHERE index_id=? AND generation=? AND base_generation=? FOR UPDATE")) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            statement.setInt(3, baseGeneration);
            try (var rows = statement.executeQuery()) {
                return rows.next() && "BUILDING".equals(rows.getString(1));
            }
        }
    }
}
