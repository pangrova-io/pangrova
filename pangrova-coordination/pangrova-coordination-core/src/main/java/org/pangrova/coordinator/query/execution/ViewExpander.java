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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.connector.spi.metadata.table.SchemaTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Expands view references in SQL before compilation. Replaces fully-qualified
 * view references with their definition SQL as subqueries. Supports nested views
 * via iterative expansion up to a configurable depth limit.
 */
final class ViewExpander {

    private static final Logger LOG = LoggerFactory.getLogger(ViewExpander.class);
    private static final int MAX_EXPANSION_DEPTH = 10;

    private final CatalogRegistry catalogRegistry;

    ViewExpander(CatalogRegistry catalogRegistry) {
        this.catalogRegistry = catalogRegistry;
    }

    String expand(String sql, SqlCompiler compiler, CoordinatorSessionContext context) {
        if (catalogRegistry == null) {
            return sql;
        }
        String expanded = sql;
        for (int depth = 0; depth < MAX_EXPANSION_DEPTH; depth++) {
            String before = expanded;
            expanded = expandOnce(expanded, compiler, context);
            if (expanded.equals(before)) {
                break;
            }
        }
        return expanded;
    }

    private String expandOnce(String sql, SqlCompiler compiler, CoordinatorSessionContext context) {
        org.pangrova.parser.ast.Statement ast;
        try {
            ast = compiler.parseToAst(sql);
        } catch (Exception e) {
            return sql;
        }
        if (ast == null) {
            return sql;
        }
        List<org.pangrova.parser.ast.TableRef> refs = collectAllNamedRefs(ast);
        if (refs.isEmpty()) {
            return sql;
        }
        String expanded = sql;
        for (org.pangrova.parser.ast.TableRef ref : refs) {
            List<String> parts = ref.name().parts();
            if (parts.size() < 3) {
                continue;
            }
            String catalog = parts.get(0);
            String schema = parts.get(1);
            String table = parts.get(2);
            Optional<String> viewDef = lookupView(catalog, schema, table, context);
            if (viewDef.isEmpty()) {
                continue;
            }
            String fqn = catalog + "." + schema + "." + table;
            String alias = ref.alias().orElse(table);
            expanded = expanded.replace(fqn, "(" + viewDef.get() + ") AS " + alias);
            LOG.info("event=view_expanded view={} alias={}", fqn, alias);
        }
        return expanded;
    }

    private Optional<String> lookupView(
            String catalog, String schema, String table, CoordinatorSessionContext context) {
        CatalogHandle handle = catalogRegistry.getCatalog(catalog);
        var session = SessionContextConverter.toConnectorSession(context, catalog, schema);
        return handle.getMetadata(session).getViewDefinition(session, new SchemaTableName(schema, table));
    }

    private static List<org.pangrova.parser.ast.TableRef> collectAllNamedRefs(
            org.pangrova.parser.ast.Statement ast) {
        List<org.pangrova.parser.ast.TableRef> result = new ArrayList<>();
        collectRefsFromStatement(ast, result);
        return result;
    }

    private static void collectRefsFromStatement(
            org.pangrova.parser.ast.Statement ast,
            List<org.pangrova.parser.ast.TableRef> result) {
        if (ast instanceof org.pangrova.parser.ast.Query query) {
            collectRefsFromQuery(query, result);
        } else if (ast instanceof org.pangrova.parser.ast.CteQuery cte) {
            if (cte.body() instanceof org.pangrova.parser.ast.Query query) {
                collectRefsFromQuery(query, result);
            }
        }
    }

    private static void collectRefsFromQuery(
            org.pangrova.parser.ast.Query query,
            List<org.pangrova.parser.ast.TableRef> result) {
        for (org.pangrova.parser.ast.TableRef ref : query.from()) {
            if (ref.kind() == org.pangrova.parser.ast.TableRefKind.NAMED) {
                result.add(ref);
            } else if (ref.subquery().isPresent()) {
                collectRefsFromStatement(ref.subquery().get(), result);
            }
        }
        for (org.pangrova.parser.ast.JoinClause join : query.joins()) {
            org.pangrova.parser.ast.TableRef right = join.right();
            if (right.kind() == org.pangrova.parser.ast.TableRefKind.NAMED) {
                result.add(right);
            } else if (right.subquery().isPresent()) {
                collectRefsFromStatement(right.subquery().get(), result);
            }
        }
    }
}
