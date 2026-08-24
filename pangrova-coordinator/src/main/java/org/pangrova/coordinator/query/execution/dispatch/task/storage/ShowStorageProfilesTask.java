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
package org.pangrova.coordinator.query.execution.dispatch.task.storage;

import java.util.List;
import java.util.stream.Collectors;

import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.parser.ast.statement.ShowStorageProfilesStatement;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.storage.StorageProfile;
import org.pangrova.spi.storage.StorageProfileRegistry;

/**
 * Executes {@code SHOW STORAGE PROFILES} by listing all registered profiles.
 */
public final class ShowStorageProfilesTask implements MetadataTask<ShowStorageProfilesStatement> {

    private static final List<String> COLUMNS = List.of(
            "name", "type", "bucket", "region", "endpoint");

    private final StorageProfileRegistry registry;

    public ShowStorageProfilesTask(StorageProfileRegistry registry) {
        this.registry = registry;
    }

    @Override
    public StatementResult execute(ShowStorageProfilesStatement statement,
                                    StatementExecutionContext context) {
        List<StorageProfile> profiles = registry.list();
        List<TypedRow> rows = profiles.stream()
                .map(p -> {
                    TypedRow.Builder b = TypedRow.builder();
                    b.putRaw("name", p.name());
                    b.putRaw("type", p.type().name());
                    b.putRaw("bucket", p.bucket());
                    b.putRaw("region", p.region());
                    b.putRaw("endpoint", p.endpoint());
                    return b.build();
                })
                .collect(Collectors.toList());
        return new StatementResult(rows, COLUMNS, null, StatementCategory.QUERY);
    }
}
