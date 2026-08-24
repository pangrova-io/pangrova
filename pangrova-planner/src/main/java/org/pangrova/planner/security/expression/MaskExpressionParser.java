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
import org.apache.calcite.rel.logical.LogicalProject;
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
 * Parses SQL mask expressions (e.g., "'***'", "0", "CAST(NULL AS INTEGER)")
 * into Calcite RexNode with correct type resolution.
 * Used by column mask application to convert column mask expression strings
 * into typed RexNode projections.
 *
 * <p>Strategy: wraps the mask expression as {@code SELECT <maskExpr> FROM _t},
 * compiles to a RelNode tree, and extracts the resulting RexNode from the
 * LogicalProject node. This leverages Calcite's full SQL validation and type
 * resolution against the actual table schema.
 *
 * <p>Thread-safe: creates fresh parser/validator per parse call.
 */
public final class MaskExpressionParser {

    private static final String VIRTUAL_TABLE = "_mask_table";

    private MaskExpressionParser() {}

    /**
     * Parses a SQL mask expression string into a RexNode.
     *
     * @param maskExpr the SQL mask expression (e.g., "'***'", "0", "CAST(NULL AS BIGINT)")
     * @param scan     the TableScan node providing schema context
     * @return the parsed RexNode with correct type
     * @throws MaskParseException if the expression cannot be parsed or validated
     */
    public static RexNode parse(String maskExpr, TableScan scan) {
        RelDataType rowType = scan.getRowType();
        try {
            return compileMaskExpression(maskExpr, rowType, scan.getCluster());
        } catch (SqlParseException e) {
            throw new MaskParseException(
                    "Failed to parse mask expression: " + maskExpr, e);
        } catch (MaskParseException e) {
            throw e;
        } catch (Exception e) {
            throw new MaskParseException(
                    "Failed to convert mask expression to RexNode: " + maskExpr, e);
        }
    }

    private static RexNode compileMaskExpression(String maskExpr, RelDataType rowType,
                                                  RelOptCluster cluster) throws SqlParseException {
        String wrappedSql = "SELECT " + maskExpr + " FROM " + VIRTUAL_TABLE;
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
        return extractProjectExpression(relNode, maskExpr);
    }

    private static RexNode extractProjectExpression(RelNode relNode, String maskExpr) {
        RexNode found = findFirstProjectExpr(relNode);
        if (found != null) {
            return found;
        }
        for (RelNode input : relNode.getInputs()) {
            found = findFirstProjectExpr(input);
            if (found != null) {
                return found;
            }
        }
        throw new MaskParseException(
                "Mask expression did not produce a LogicalProject: " + maskExpr, null);
    }

    private static RexNode findFirstProjectExpr(RelNode node) {
        if (node instanceof LogicalProject project && !project.getProjects().isEmpty()) {
            return project.getProjects().get(0);
        }
        return null;
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

    /** Exception thrown when a mask expression cannot be parsed or converted. */
    public static final class MaskParseException extends RuntimeException {
        public MaskParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
