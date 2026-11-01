/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.ai.store.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import static java.util.Objects.requireNonNull;

/** JDBC binding helpers for the AI governance stores. */
public final class JdbcAiSupport {

    private JdbcAiSupport() {}

    /** Binds canonical PostgreSQL JSON/JSONB text without implicit VARCHAR casts. */
    public static void setJson(PreparedStatement statement, int index, String json)
            throws SQLException {
        statement.setObject(index, json, Types.OTHER);
    }

    /**
     * Canonicalizes an instant to PostgreSQL {@code TIMESTAMPTZ(6)} precision.
     *
     * <p>PostgreSQL persists timestamps at microsecond precision. Canonicalizing before a write keeps
     * equality and compare-and-set values stable across the JDBC round trip.</p>
     */
    public static Instant canonicalInstant(Instant value) {
        return requireNonNull(value, "value is null").truncatedTo(ChronoUnit.MICROS);
    }

    /** Binds an absolute instant as an explicit UTC {@link OffsetDateTime}. */
    public static void setInstant(PreparedStatement statement, int index, Instant value)
            throws SQLException {
        statement.setObject(index, canonicalInstant(value).atOffset(ZoneOffset.UTC));
    }

    /** Reads a nullable time-zone-aware timestamp and returns its absolute instant. */
    public static Instant getInstant(ResultSet result, String column) throws SQLException {
        OffsetDateTime value = result.getObject(column, OffsetDateTime.class);
        return value == null ? null : canonicalInstant(value.toInstant());
    }

    /** Reads a nullable time-zone-aware timestamp and returns its absolute instant. */
    public static Instant getInstant(ResultSet result, int column) throws SQLException {
        OffsetDateTime value = result.getObject(column, OffsetDateTime.class);
        return value == null ? null : canonicalInstant(value.toInstant());
    }
}
