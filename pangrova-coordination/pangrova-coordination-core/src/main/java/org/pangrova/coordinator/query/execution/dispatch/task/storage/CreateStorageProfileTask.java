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
import java.util.Map;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.parser.ast.statement.CreateStorageProfileStatement;
import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.persistence.api.file.profile.StorageProfileRegistry;
import org.pangrova.common.storage.StorageType;

/**
 * Executes {@code CREATE STORAGE PROFILE} by registering a new profile
 * in the {@link StorageProfileRegistry}.
 */
public final class CreateStorageProfileTask implements MetadataTask<CreateStorageProfileStatement> {

    private final StorageProfileRegistry registry;

    public CreateStorageProfileTask(StorageProfileRegistry registry) {
        this.registry = registry;
    }

    @Override
    public StatementResult execute(CreateStorageProfileStatement statement,
                                    StatementExecutionContext context) {
        String name = statement.profileName();
        Map<String, String> props = statement.properties();
        String typeStr = props.getOrDefault("type", "S3");
        StorageType type;
        try {
            type = StorageType.valueOf(typeStr.toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Unknown storage type: " + typeStr
                            + ". Supported: S3, OSS, GCS, ADLS, LOCAL");
        }

        StorageProfile profile = new StorageProfile(
                name, type,
                props.getOrDefault("bucket", ""),
                props.getOrDefault("prefix", ""),
                props.getOrDefault("endpoint", ""),
                props.getOrDefault("region", ""),
                Map.of(), // credentials are not set via SQL for security
                filterOptions(props));
        if (!registry.createIfAbsent(profile)) {
            if (!statement.ifNotExists()) {
                throw new PangrovaException(PangrovaErrorCode.METADATA_CATALOG_ALREADY_EXISTS,
                        "Storage profile already exists: " + name);
            }
            return new StatementResult(List.of(), List.of(), "Storage profile already exists: " + name,
                    StatementCategory.DDL);
        }

        return new StatementResult(List.of(), List.of(), "Storage profile created: " + name,
                StatementCategory.DDL);
    }

    private static Map<String, String> filterOptions(Map<String, String> props) {
        var options = new java.util.LinkedHashMap<String, String>();
        for (var entry : props.entrySet()) {
            String key = entry.getKey();
            if ("path-style-access".equals(key)) {
                String value = entry.getValue();
                if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
                    throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                            "path-style-access must be true or false");
                }
                options.put(key, value.toLowerCase(java.util.Locale.ROOT));
            } else if ("project".equals(key) && "GCS".equalsIgnoreCase(props.getOrDefault("type", "S3"))) {
                options.put(key, entry.getValue());
            } else if (!"type".equals(key) && !"bucket".equals(key)
                    && !"prefix".equals(key) && !"endpoint".equals(key)
                    && !"region".equals(key)) {
                throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                        "Unsupported storage profile property: " + key);
            }
        }
        return options;
    }
}
