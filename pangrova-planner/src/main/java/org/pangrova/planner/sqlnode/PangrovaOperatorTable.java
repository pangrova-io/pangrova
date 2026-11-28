package org.pangrova.planner.sqlnode;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * Custom SqlOperator instances for Pangrova SQL extensions
 * (COMPACT, OPTIMIZE, VACUUM, Catalog DDL, CALL, Table DDL).
 */
public final class PangrovaOperatorTable {

    public static final SqlFunction COMPACT = createOp("COMPACT");
    public static final SqlFunction OPTIMIZE = createOp("OPTIMIZE");
    public static final SqlFunction VACUUM = createOp("VACUUM");
    public static final SqlFunction CREATE_CATALOG = createOp("CREATE_CATALOG");
    public static final SqlFunction DROP_CATALOG = createOp("DROP_CATALOG");
    public static final SqlFunction SHOW_CATALOGS = createOp("SHOW_CATALOGS");
    public static final SqlFunction DESCRIBE_CATALOG = createOp("DESCRIBE_CATALOG");
    public static final SqlFunction ALTER_CATALOG = createOp("ALTER_CATALOG");
    public static final SqlFunction CALL = createOp("CALL");
    public static final SqlFunction CREATE_TABLE = createOp("CREATE_TABLE");
    public static final SqlFunction DROP_TABLE = createOp("DROP_TABLE");
    public static final SqlFunction INSERT_INTO = createOp("INSERT_INTO");
    public static final SqlFunction INSERT_OVERWRITE = createOp("INSERT_OVERWRITE");
    public static final SqlFunction DELETE_FROM = createOp("DELETE_FROM");
    public static final SqlFunction UPDATE_TABLE = createOp("UPDATE_TABLE");
    public static final SqlFunction MERGE_INTO = createOp("MERGE_INTO");
    public static final SqlFunction ALTER_TABLE = createOp("ALTER_TABLE");
    public static final SqlFunction CREATE_SCHEMA = createOp("CREATE_SCHEMA");
    public static final SqlFunction SHOW_SCHEMAS = createOp("SHOW_SCHEMAS");
    public static final SqlFunction SHOW_TABLES = createOp("SHOW_TABLES");
    public static final SqlFunction DESCRIBE_TABLE = createOp("DESCRIBE_TABLE");

    private PangrovaOperatorTable() {}

    private static SqlFunction createOp(String name) {
        return new SqlFunction(
                name,
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.VARCHAR_2000,
                null,
                OperandTypes.VARIADIC,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }
}
