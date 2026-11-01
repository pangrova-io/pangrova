package org.pangrova.connector.paimon.scan;

import org.pangrova.common.api.annotation.Internal;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.connector.base.ConnectorException;

import java.util.Set;

/** Rejects SDK policies that the canonical Native merge descriptor cannot represent, before dispatch. */
@Internal
public final class PaimonNativeMergeAdmission {
    private static final Set<String> FUNCTIONS = Set.of("SUM", "MAX", "MIN", "PRODUCT", "LAST_VALUE",
            "LAST_NON_NULL_VALUE", "FIRST_VALUE", "FIRST_NON_NULL_VALUE", "LISTAGG", "BOOL_AND", "BOOL_OR",
            "COLLECT", "MERGE_MAP", "NESTED_UPDATE");

    private PaimonNativeMergeAdmission() {}

    /** Rejects partial-update options that have no canonical Native representation. */
    public static void validatePartialUpdate(Table table) {
        for (var entry : table.options().entrySet()) {
            if (entry.getKey().endsWith(".sequence-group") || entry.getKey().endsWith(".aggregate-function")
                    || entry.getKey().equals("fields.default-aggregate-function")
                    || entry.getKey().startsWith("partial-update.remove-record-on-sequence-group")) {
                reject(entry.getKey());
            }
        }
    }

    /** Validates the exact SDK aggregator, type and options before dispatch. */
    public static void validateAggregation(String field, String function, DataType type, CoreOptions options) {
        if (!FUNCTIONS.contains(function)) reject("fields." + field + ".aggregate-function=" + function);
        if (options.fieldAggIgnoreRetract(field)) reject("fields." + field + ".ignore-retract");
        validateCollectionPolicy(field, function, options);
        validateNumericType(field, function, type);
    }

    private static void validateCollectionPolicy(String field, String function, CoreOptions options) {
        if (Set.of("COLLECT", "LISTAGG").contains(function) && options.fieldCollectAggDistinct(field)) {
            reject("fields." + field + ".distinct");
        }
        if (function.equals("NESTED_UPDATE") && (!options.fieldNestedUpdateAggNestedKey(field).isEmpty()
                || options.fieldNestedUpdateAggCountLimit(field) != Integer.MAX_VALUE)) {
            reject("fields." + field + ".nested-key/count-limit");
        }
        if (function.equals("LISTAGG") && !options.fieldListAggDelimiter(field).equals(",")) {
            reject("fields." + field + ".list-agg-delimiter");
        }
    }

    private static void validateNumericType(String field, String function, DataType type) {
        if (Set.of("SUM", "MAX", "MIN", "PRODUCT").contains(function)) {
            boolean numeric = Set.of(DataTypeRoot.TINYINT, DataTypeRoot.SMALLINT, DataTypeRoot.INTEGER,
                    DataTypeRoot.BIGINT, DataTypeRoot.FLOAT, DataTypeRoot.DOUBLE).contains(type.getTypeRoot());
            if (!numeric && !(type.getTypeRoot() == DataTypeRoot.DECIMAL && !function.equals("PRODUCT"))) {
                reject("fields." + field + ".aggregate-function=" + function + " on " + type);
            }
        }
    }

    private static void reject(String policy) {
        throw new ConnectorException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                "Paimon Native merge does not represent SDK policy: " + policy);
    }
}
