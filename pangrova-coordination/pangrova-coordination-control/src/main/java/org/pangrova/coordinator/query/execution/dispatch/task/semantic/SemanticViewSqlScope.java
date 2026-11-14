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
package org.pangrova.coordinator.query.execution.dispatch.task.semantic;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.semantic.api.identity.SemanticViewId;

import java.util.List;

/** Resolves SQL names against the authenticated session before invoking the semantic domain. */
record SemanticViewSqlScope(UserIdentity user, String tenantId, String catalog, String schema) {

    static SemanticViewSqlScope from(QuerySession session) {
        if (session == null || session.userIdentity() == null || session.userIdentity().principal() == null
                || session.userIdentity().principal().isBlank() || session.tenantId() == null || session.tenantId().isBlank()) {
            throw new PangrovaException(PangrovaErrorCode.ACCESS_DENIED,
                    "Semantic View operations require an authenticated tenant context");
        }
        String catalog = session.sessionProperties().getOrDefault("catalog", "default");
        String schema = session.sessionProperties().getOrDefault("schema", "default");
        if (catalog.isBlank() || schema.isBlank()) {
            throw invalid("Session catalog and schema must not be blank");
        }
        return new SemanticViewSqlScope(session.userIdentity(), session.tenantId(), catalog, schema);
    }

    SemanticViewId resolveView(Identifier name) {
        List<String> parts = name.parts();
        return switch (parts.size()) {
            case 3 -> new SemanticViewId(tenantId, parts.get(0), parts.get(1), parts.get(2));
            case 2 -> new SemanticViewId(tenantId, catalog, parts.get(0), parts.get(1));
            case 1 -> new SemanticViewId(tenantId, catalog, schema, parts.get(0));
            default -> throw invalid("Semantic View name must have one to three parts");
        };
    }

    SemanticViewSqlScope resolveSchema(List<String> parts) {
        return switch (parts.size()) {
            case 2 -> new SemanticViewSqlScope(user, tenantId, parts.get(0), parts.get(1));
            case 1 -> new SemanticViewSqlScope(user, tenantId, catalog, parts.get(0));
            case 0 -> this;
            default -> throw invalid("SHOW SEMANTIC VIEWS scope must have zero to two parts");
        };
    }

    private static PangrovaException invalid(String message) {
        return new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message);
    }
}
