package org.pangrova.spi.execution.udf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.pangrova.spi.connector.data.type.Type;
import org.pangrova.spi.connector.data.type.TypeSignature;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link UserDefinedFunction} SPI contract.
 */
class UserDefinedFunctionTest {

    /** Simple stub Type for testing — represents BIGINT. */
    private static final Type BIGINT_TYPE = new Type() {
        @Override
        public String getDisplayName() {
            return "BIGINT";
        }

        @Override
        public TypeSignature getTypeSignature() {
            return new TypeSignature("BIGINT");
        }
    };

    /** Simple stub Type for testing — represents VARCHAR. */
    private static final Type VARCHAR_TYPE = new Type() {
        @Override
        public String getDisplayName() {
            return "VARCHAR";
        }

        @Override
        public TypeSignature getTypeSignature() {
            return new TypeSignature("VARCHAR");
        }
    };

    /**
     * Minimal in-engine Java UDF that doubles a BIGINT value.
     */
    private static final UserDefinedFunction DOUBLE_IT = new UserDefinedFunction() {
        @Override
        public String name() {
            return "double_it";
        }

        @Override
        public Type returnType(List<Type> argumentTypes) {
            if (argumentTypes.size() != 1) {
                throw new IllegalArgumentException("double_it requires exactly 1 argument");
            }
            return argumentTypes.get(0);
        }

        @Override
        public Object evaluate(Object... args) {
            if (args[0] == null) {
                return null;
            }
            return ((Number) args[0]).longValue() * 2;
        }
    };

    /**
     * SQL-defined UDF that concatenates two strings with a separator.
     */
    private static final UserDefinedFunction CONCAT_WITH_SEP = new UserDefinedFunction() {
        @Override
        public String name() {
            return "concat_sep";
        }

        @Override
        public Type returnType(List<Type> argumentTypes) {
            return VARCHAR_TYPE;
        }

        @Override
        public Object evaluate(Object... args) {
            return args[0] + "-" + args[1];
        }

        @Override
        public UdfExecutionMode executionMode() {
            return UdfExecutionMode.SCALAR_SQL;
        }

        @Override
        public String sqlBody() {
            return "$1 || '-' || $2";
        }

        @Override
        public String description() {
            return "Concatenates two strings with a dash separator";
        }
    };

    @Test
    void nameReturnsCorrectValue() {
        assertThat(DOUBLE_IT.name()).isEqualTo("double_it");
    }

    @Test
    void returnTypeResolvesFromArguments() {
        Type resolved = DOUBLE_IT.returnType(List.of(BIGINT_TYPE));
        assertThat(resolved).isSameAs(BIGINT_TYPE);
    }

    @Test
    void returnTypeRejectsInvalidArgCount() {
        assertThatThrownBy(() -> DOUBLE_IT.returnType(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void evaluateComputesCorrectResult() {
        Object result = DOUBLE_IT.evaluate(21L);
        assertThat(result).isEqualTo(42L);
    }

    @Test
    void evaluateHandlesNull() {
        Object result = DOUBLE_IT.evaluate((Object) null);
        assertThat(result).isNull();
    }

    @Test
    void defaultExecutionModeIsInEngineJava() {
        assertThat(DOUBLE_IT.executionMode()).isEqualTo(UdfExecutionMode.IN_ENGINE_JAVA);
    }

    @Test
    void defaultSqlBodyIsNull() {
        assertThat(DOUBLE_IT.sqlBody()).isNull();
    }

    @Test
    void defaultDescriptionIsEmpty() {
        assertThat(DOUBLE_IT.description()).isEmpty();
    }

    @Test
    void sqlUdfDeclaresCorrectMode() {
        assertThat(CONCAT_WITH_SEP.executionMode()).isEqualTo(UdfExecutionMode.SCALAR_SQL);
    }

    @Test
    void sqlUdfProvidesSqlBody() {
        assertThat(CONCAT_WITH_SEP.sqlBody()).isEqualTo("$1 || '-' || $2");
    }

    @Test
    void sqlUdfProvidesDescription() {
        assertThat(CONCAT_WITH_SEP.description())
                .isEqualTo("Concatenates two strings with a dash separator");
    }
}
