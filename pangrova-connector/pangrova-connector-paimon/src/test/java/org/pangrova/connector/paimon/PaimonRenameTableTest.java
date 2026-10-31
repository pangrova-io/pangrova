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
package org.pangrova.connector.paimon;

import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.base.ConnectorException;
import org.pangrova.connector.spi.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.spi.metadata.table.SchemaTableName;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for F-SPI-001: {@link PaimonMetadata#renameTable} must expose Paimon's catalog rename
 * capability (previously the SPI default threw {@code UnsupportedOperationException}). Uses a real
 * filesystem-backed Paimon catalog (no Docker).
 */
class PaimonRenameTableTest {

    @TempDir
    Path warehouse;

    private Catalog catalog;
    private PaimonMetadata metadata;

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override
        public java.util.Optional<org.pangrova.connector.spi.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of("test_catalog"); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    };

    @BeforeEach
    void setUp() throws Exception {
        Options options = new Options();
        options.set("warehouse", warehouse.toUri().toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase("db", false);
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("name", DataTypes.STRING())
                .build();
        catalog.createTable(Identifier.create("db", "orders"), schema, false);
        metadata = new PaimonMetadata(catalog, "test_catalog", new org.pangrova.connector.paimon.PaimonSplitRegistry());
    }

    @Test
    void renameMovesTableToNewIdentifier() throws Exception {
        ConnectorTableHandle handle = metadata.getTableHandle(
                SESSION, new SchemaTableName("db", "orders"));

        metadata.renameTable(SESSION, handle, "db", "orders_renamed");

        // New identifier resolves; the old identifier no longer exists.
        assertThat(catalog.getTable(Identifier.create("db", "orders_renamed"))).isNotNull();
        assertThatThrownBy(() -> catalog.getTable(Identifier.create("db", "orders")))
                .isInstanceOf(Catalog.TableNotExistException.class);
    }

    @Test
    void renameMissingSourceThrowsTableNotFound() throws Exception {
        ConnectorTableHandle handle = metadata.getTableHandle(
                SESSION, new SchemaTableName("db", "orders"));
        // Drop the source out from under the (now stale) handle so the rename hits a missing source.
        catalog.dropTable(Identifier.create("db", "orders"), false);

        assertThatThrownBy(() -> metadata.renameTable(SESSION, handle, "db", "orders_x"))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("Table not found for rename");
    }

    @Test
    void renameToExistingTargetThrowsAlreadyExists() throws Exception {
        catalog.createTable(Identifier.create("db", "orders2"),
                Schema.newBuilder().column("id", DataTypes.BIGINT()).build(), false);
        ConnectorTableHandle handle = metadata.getTableHandle(
                SESSION, new SchemaTableName("db", "orders"));

        assertThatThrownBy(() -> metadata.renameTable(SESSION, handle, "db", "orders2"))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("Target table already exists for rename");
    }
}
