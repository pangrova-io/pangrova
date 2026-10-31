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
package org.pangrova.parser.ast.statement;

import java.util.Map;

import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents {@code CREATE STORAGE PROFILE [IF NOT EXISTS] name WITH (key=value, ...)}.
 */
public record CreateStorageProfileStatement(
        String profileName,
        boolean ifNotExists,
        Map<String, String> properties) implements Statement {

    public CreateStorageProfileStatement {
        properties = Map.copyOf(properties);
    }

    @Override
    public StatementType type() {
        return StatementType.CREATE_STORAGE_PROFILE;
    }
}
