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
package org.pangrova.coordinator.query.execution;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.parser.syntax.TimeTravelSqlClauses.Analysis;
import org.pangrova.parser.syntax.TimeTravelSqlClauses.Reference;
import org.pangrova.planner.api.exception.PlannerException;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Expands view references in SQL before compilation. Replaces fully-qualified
 * view references with their definition SQL as subqueries. Supports nested views
 * with a fixed expansion limit.
 */
final class ViewExpander {

    private static final Logger LOG = LoggerFactory.getLogger(ViewExpander.class);
    private static final int MAX_EXPANSION_DEPTH = 10;

    private final CatalogRegistry catalogRegistry;

    ViewExpander(CatalogRegistry catalogRegistry) {
        this.catalogRegistry = catalogRegistry;
    }

    Analysis expand(Analysis input, QuerySession context) {
        if (catalogRegistry == null || input.references().isEmpty()) {
            return input;
        }
        Map<QualifiedTableName, Optional<String>> definitions =
                new HashMap<>();
        Analysis expanded = input;
        for (int depth = 0; depth < MAX_EXPANSION_DEPTH; depth++) {
            Analysis next = expanded.rewriteNames(reference -> expandReference(reference, context, definitions));
            if (next == expanded) {
                return expanded;
            }
            expanded = next;
        }
        return expanded;
    }

    private String expandReference(Reference reference, QuerySession context,
            Map<QualifiedTableName, Optional<String>> definitions) {
        var ref = reference.table();
        var parts = ref.name().parts();
        if (reference.cte() || parts.size() != 3 || ref.systemTableRef().isPresent()) {
            return null;
        }
        var name = new QualifiedTableName(
                parts.get(0), parts.get(1), parts.get(2));
        var definition = definitions.computeIfAbsent(name,
                key -> lookupView(key.catalog(), key.schema(), key.table(), context));
        if (definition.isEmpty()) {
            return null;
        }
        if (ref.timeTravelSpec().isPresent()) {
            throw new PlannerException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Time travel cannot target a logical view: " + name);
        }
        String alias = ref.alias().isPresent() ? "" : " AS \"" + name.table().replace("\"", "\"\"") + "\"";
        LOG.debug("event=view_expanded view={}", name);
        return "(" + definition.orElseThrow() + ")" + alias;
    }

    private Optional<String> lookupView(
            String catalog, String schema, String table, QuerySession context) {
        var session = SessionContextConverter.toConnectorSession(context, catalog, schema);
        return catalogRegistry.metadataForQuery(catalog, session)
                .getViewDefinition(session, new SchemaTableName(schema, table));
    }

}
