package org.pangrova.planner.analysis;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.pangrova.parser.syntax.AntlrSqlParser;

import static org.assertj.core.api.Assertions.assertThat;

class EmptyArrayInsertPlannerTest {

    @Test
    void insertEmptyArrayUsesTargetColumnElementType() {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add("tags", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder()
                        .add("id", factory.createSqlType(SqlTypeName.BIGINT))
                        .add("labels", factory.createArrayType(
                                factory.createSqlType(SqlTypeName.VARCHAR), -1))
                        .build();
            }
        });
        String sql = "INSERT INTO tags (id, labels) VALUES (1, ARRAY[])";

        var result = new PangrovaPlanner(schema).planFromAst(new AntlrSqlParser().parse(sql));

        assertThat(result.plan()).isInstanceOf(TableModify.class);
        RelDataType labelsType = result.plan().getInput(0).getRowType().getFieldList().get(1).getType();
        assertThat(labelsType.getSqlTypeName()).isEqualTo(SqlTypeName.ARRAY);
        assertThat(labelsType.getComponentType().getSqlTypeName()).isEqualTo(SqlTypeName.VARCHAR);
    }
}
