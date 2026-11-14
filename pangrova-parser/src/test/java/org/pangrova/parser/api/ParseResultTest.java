package org.pangrova.parser.api;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.pangrova.parser.api.ast.statement.ShowCatalogsStatement;
import org.pangrova.parser.api.diagnostic.ParseError;
import org.pangrova.parser.api.parse.ParseResult;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ParseResultTest {
    @Test
    void recoveryFreezesDiagnosticsAndCannotClaimMissingAstAsSuccess() {
        var errors = new ArrayList<>(List.of(new ParseError(1, 7, "missing identifier")));
        var partial = new ParseResult(null, errors);
        errors.clear();
        assertThat(partial.hasErrors()).isTrue();
        assertThat(partial.errors()).hasSize(1);
        assertThat(partial.statement()).isNull();
        assertThatThrownBy(() -> partial.errors().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> new ParseResult(null, List.of())).isInstanceOf(NullPointerException.class);
        assertThat(new ParseResult(new ShowCatalogsStatement(), List.of()).isSuccess()).isTrue();
    }
}
