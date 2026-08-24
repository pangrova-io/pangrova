/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.statement.TableResolver;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.parser.ast.statement.ShowMetadataStatement;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorSystemTable;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Redirects SHOW SNAPSHOTS/FILES/MANIFESTS/PARTITIONS to the corresponding
 * connector system table, returning the same result as a direct
 * {@code SELECT * FROM table$systemTable} query.
 */
public final class ShowMetadataRedirectTask implements MetadataTask<ShowMetadataStatement> {

    private static final Logger LOG = LoggerFactory.getLogger(ShowMetadataRedirectTask.class);

    private final CatalogRegistry catalogRegistry;

    public ShowMetadataRedirectTask(CatalogRegistry catalogRegistry) {
        this.catalogRegistry = catalogRegistry;
    }

    @Override
    public StatementResult execute(ShowMetadataStatement statement,
                                    StatementExecutionContext context) {
        String systemTableName = resolveSystemTableName(statement);
        QualifiedTableName tableName = QualifiedTableName.resolve(statement.tableName());

        ConnectorSession session = TableResolver.buildSession(tableName.catalog());
        ConnectorMetadata metadata = catalogRegistry.getCatalog(tableName.catalog())
                .getMetadata(session);

        ConnectorTableHandle tableHandle = TableResolver.requireTableHandle(
                metadata, session, tableName);

        ConnectorSystemTable systemTable = metadata.getSystemTable(
                session, tableHandle, systemTableName)
                .orElseThrow(() -> new PangrovaException(PangrovaErrorCode.METADATA_TABLE_NOT_FOUND,
                        "System table not found: " + tableName + "$" + systemTableName));

        List<String> columns = systemTable.columns();
        List<TypedRow> rows = systemTable.rows();

        LOG.info("event=show_metadata table={}${} rowCount={}",
                tableName, systemTableName, rows.size());

        return new StatementResult(rows, columns, null, StatementCategory.QUERY);
    }

    private static String resolveSystemTableName(ShowMetadataStatement stmt) {
        if (stmt instanceof ShowMetadataStatement.ShowSnapshots) {
            return "snapshots";
        } else if (stmt instanceof ShowMetadataStatement.ShowFiles) {
            return "files";
        } else if (stmt instanceof ShowMetadataStatement.ShowManifests) {
            return "manifests";
        } else if (stmt instanceof ShowMetadataStatement.ShowPartitions) {
            return "partitions";
        }
        throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                "Unknown SHOW metadata type: " + stmt.getClass().getSimpleName());
    }
}
