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

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.parser.api.ast.statement.DropStorageProfileStatement;
import org.pangrova.persistence.file.api.profile.StorageProfileRegistry;

/**
 * Executes {@code DROP STORAGE PROFILE [IF EXISTS] name}.
 */
public final class DropStorageProfileTask implements MetadataTask<DropStorageProfileStatement> {

    private final StorageProfileRegistry registry;

    public DropStorageProfileTask(StorageProfileRegistry registry) {
        this.registry = registry;
    }

    @Override
    public StatementResult execute(DropStorageProfileStatement statement,
                                    StatementExecutionContext context) {
        String name = statement.profileName();
        boolean removed = registry.unregister(name);
        if (!removed && !statement.ifExists()) {
            throw new PangrovaException(PangrovaErrorCode.METADATA_TABLE_NOT_FOUND,
                    "Storage profile not found: " + name);
        }
        String message = removed ? "Storage profile dropped: " + name
                : "Storage profile does not exist: " + name;
        return new StatementResult(List.of(), List.of(), message, StatementCategory.DDL);
    }
}
