package org.pangrova.parser.parse;

import org.pangrova.parser.format.AstPrinter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;
import org.pangrova.parser.api.ast.type.DataTypeSpec;
import org.pangrova.parser.build.type.DataTypeSpecParser;
import org.pangrova.parser.api.enums.DistanceMetric;
import org.pangrova.types.enums.VectorElementType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VectorDeclarationPrecisionTest {
    @ParameterizedTest
    @EnumSource(VectorElementType.class)
    void neutralDeclarationRoundTripPreservesPrecisionAndExistingMetric(VectorElementType element) {
        for (DistanceMetric metric : DistanceMetric.values()) {
            var declared = new DataTypeSpec.VectorType(3, metric, element);
            assertThat(org.pangrova.parser.build.type.RawSqlTypeParser.parse(declared.toCanonicalString())).isEqualTo(declared);
            assertThat(DataTypeSpecParser.parse(declared.toCanonicalString())).isEqualTo(declared);
        }
    }

    @Test
    void storageCanonicalNamesAndLegacyCosineAliasHaveExplicitCoordinatePrecision() {
        assertThat(org.pangrova.parser.build.type.RawSqlTypeParser.parse("VECTOR(3,DOUBLE)"))
                .isEqualTo(new DataTypeSpec.VectorType(3, DistanceMetric.COSINE, VectorElementType.DOUBLE));
        assertThat(org.pangrova.parser.build.type.RawSqlTypeParser.parse("VECTOR(3)"))
                .isEqualTo(new DataTypeSpec.VectorType(3, DistanceMetric.COSINE, VectorElementType.FLOAT));
        assertThat(org.pangrova.parser.build.type.RawSqlTypeParser.parse("VECTOR(3, COSINE)"))
                .isEqualTo(new DataTypeSpec.VectorType(3, DistanceMetric.COSINE, VectorElementType.FLOAT));
    }

    @ParameterizedTest
    @EnumSource(VectorElementType.class)
    void actualSqlPrinterAndOnlyParserRetainVectorPrecision(VectorElementType element) {
        var parser = new AntlrSqlParser();
        var original = (CreateTableStatement) parser.parse(
                "CREATE TABLE t (v VECTOR(3," + element.name() + "))");
        var reparsed = (CreateTableStatement) parser.parse(new AstPrinter().print(original));
        assertThat(reparsed.columns().get(0).type()).isEqualTo(original.columns().get(0).type());
    }

    @Test
    void malformedPrecisionOrIncompleteDeclarationCannotBecomeDefaultFloat() {
        for (String type : new String[]{"VECTOR(3,)", "VECTOR(3,DOUBLE,)", "VECTOR(3,INT)",
                "VECTOR(3,COSINE,DOUBLE)", "VECTOR(65536)", "VECTOR(0)", "VECTOR(3,DOUBLE,COSINE,extra)"}) {
            assertThatThrownBy(() -> org.pangrova.parser.build.type.RawSqlTypeParser.parse(type)).isInstanceOf(IllegalArgumentException.class);
        }
    }
}
