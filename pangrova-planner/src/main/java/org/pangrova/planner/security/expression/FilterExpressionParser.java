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
package org.pangrova.planner.security.expression;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.prepare.CalciteCatalogReader;

import java.util.List;
import java.util.Properties;

/**
 * Parses SQL filter expressions (e.g., "department = 'sales'") into Calcite RexNode.
 * Used by row-level security filter application to convert filter strings
 * into executable filter conditions.
 *
 * <p>Strategy: wraps the filter expression as {@code SELECT * FROM _t WHERE <expr>},
 * compiles to a RelNode tree, and extracts the filter condition from the resulting
 * LogicalFilter node. This leverages Calcite's full SQL validation and type resolution
 * against the actual table schema.
 *
 * <p>Thread-safe: creates fresh parser/validator per parse call.
 */
public final class FilterExpressionParser {

    private static final String VIRTUAL_TABLE = "_rls_table";

    private FilterExpressionParser() {}

    /**
     * Parses a SQL filter expression string into a RexNode condition.
     *
     * @param filterExpr the SQL filter expression (e.g., "department = 'sales'")
     * @param scan the TableScan node providing schema context for column resolution
     * @return the parsed RexNode condition
     * @throws FilterParseException if the expression cannot be parsed or validated
     */
    public static RexNode parse(String filterExpr, TableScan scan) {
        return parse(filterExpr, scan.getRowType(), scan.getCluster());
    }

    /**
     * Parses a SQL filter expression string into a RexNode condition.
     *
     * @param filterExpr the SQL filter expression (e.g., "department = 'sales'")
     * @param rowType    the row type providing schema context for column resolution
     * @param cluster    the RelOptCluster for type factory and RexBuilder access
     * @return the parsed RexNode condition
     * @throws FilterParseException if the expression cannot be parsed or validated
     */
    public static RexNode parse(String filterExpr, RelDataType rowType, RelOptCluster cluster) {
        try {
            return compileFilterExpression(filterExpr, rowType, cluster);
        } catch (SqlParseException e) {
            throw new FilterParseException(
                    "Failed to parse filter expression: " + filterExpr, e);
        } catch (FilterParseException e) {
            throw e;
        } catch (Exception e) {
            throw new FilterParseException(
                    "Failed to convert filter expression to RexNode: " + filterExpr, e);
        }
    }

    private static RexNode compileFilterExpression(String filterExpr, RelDataType rowType,
                                                    RelOptCluster cluster) throws SqlParseException {
        String wrappedSql = "SELECT * FROM " + VIRTUAL_TABLE + " WHERE " + filterExpr;
        CalciteSchema rootSchema = buildSchemaWithVirtualTable(rowType, cluster.getTypeFactory());
        CalciteCatalogReader catalogReader = buildCatalogReader(rootSchema, cluster.getTypeFactory());
        SqlValidator validator = buildValidator(catalogReader, cluster.getTypeFactory());
        SqlParser.Config parserConfig = SqlParser.config().withCaseSensitive(false);
        SqlNode sqlNode = SqlParser.create(wrappedSql, parserConfig).parseQuery();
        SqlNode validated = validator.validate(sqlNode);
        SqlToRelConverter converter = new SqlToRelConverter(
                null, validator, catalogReader, cluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config());
        RelNode relNode = converter.convertQuery(validated, false, true).rel;
        return extractFilterCondition(relNode, filterExpr);
    }

    private static RexNode extractFilterCondition(RelNode relNode, String filterExpr) {
        if (relNode instanceof LogicalFilter filter) {
            return filter.getCondition();
        }
        for (RelNode input : relNode.getInputs()) {
            if (input instanceof LogicalFilter filter) {
                return filter.getCondition();
            }
        }
        throw new FilterParseException(
                "Filter expression did not produce a LogicalFilter: " + filterExpr, null);
    }

    private static CalciteSchema buildSchemaWithVirtualTable(RelDataType rowType,
                                                              RelDataTypeFactory typeFactory) {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        rootSchema.add(VIRTUAL_TABLE, new VirtualTable(rowType));
        return rootSchema;
    }

    private static CalciteCatalogReader buildCatalogReader(CalciteSchema rootSchema,
                                                           RelDataTypeFactory typeFactory) {
        Properties props = new Properties();
        props.setProperty("caseSensitive", "false");
        return new CalciteCatalogReader(rootSchema, List.of(""), typeFactory,
                new CalciteConnectionConfigImpl(props));
    }

    private static SqlValidator buildValidator(CalciteCatalogReader catalogReader,
                                               RelDataTypeFactory typeFactory) {
        return SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(), catalogReader, typeFactory,
                SqlValidator.Config.DEFAULT);
    }

    private static final class VirtualTable extends AbstractTable {
        private final RelDataType rowType;

        VirtualTable(RelDataType rowType) {
            this.rowType = rowType;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return rowType;
        }
    }

    /** Exception thrown when a filter expression cannot be parsed or converted. */
    public static final class FilterParseException extends RuntimeException {
        public FilterParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
