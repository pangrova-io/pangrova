package org.pangrova.client.cli;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TableFormatterTest {
    @Test
    void multilineModelTextCannotCreateExtraPhysicalTableRows() {
        var output = TableFormatter.formatPage(List.of("id", "response"),
                List.of(List.of("0", "Hello\nWorld\rAgain\tNow")));
        assertThat(output).contains("Hello\\nWorld\\rAgain\\tNow");
        assertThat(output.lines().filter(line -> line.startsWith("|"))).hasSize(2);
        assertAligned(output, 2);
    }

    @Test
    void cellDelimitersAndLiteralBackslashesRemainDistinguishable() {
        var output = TableFormatter.formatPage(List.of("name|label"),
                List.of(List.of("a|b\\n\\u007c")));
        assertThat(output).contains("name\\u007clabel", "a\\u007cb\\\\n\\\\u007c");
        assertAligned(output, 1);
    }

    @Test
    void terminalControlsAndUnicodeLineSeparatorsAreRenderedAsText() {
        var output = TableFormatter.formatPage(List.of("response\nheader"),
                List.of(List.of("\u001b[2J\u0000\u0085\u2028\u2029")));
        assertThat(output).contains("response\\nheader", "\\u001b[2J\\u0000\\u0085\\u2028\\u2029");
        assertThat(output).doesNotContain("\u001b", "\u0000", "\u0085", "\u2028", "\u2029");
        assertAligned(output, 1);
    }

    @Test
    void ordinaryUnicodeAndNullCellsKeepTheirExistingDisplay() {
        var output = TableFormatter.formatPage(List.of("name", "value"),
                List.of(Arrays.asList("你好😀", null)));
        assertThat(output).contains("你好😀", "NULL");
        assertAligned(output, 2);
    }

    @Test
    void styledTablesColorHeadersAndSeparatorsWithoutChangingPlainOutput() {
        var rows = List.of(List.of("Alice"));
        var plain = TableFormatter.formatPage(List.of("name"), rows);
        var styled = TableFormatter.formatPage(List.of("name"), rows, new TerminalStyle(true, 100));

        assertThat(plain).doesNotContain("\u001B[");
        assertThat(styled).contains("\u001B[1;38;5;141m", "\u001B[2m", "Alice");
    }

    private static void assertAligned(String output, int columns) {
        var physical = output.lines().filter(line -> line.startsWith("+") || line.startsWith("|")).toList();
        assertThat(physical).extracting(TableFormatter::displayWidth).containsOnly(
                TableFormatter.displayWidth(physical.get(0)));
        assertThat(physical.stream().filter(line -> line.startsWith("|")))
                .allSatisfy(line -> assertThat(line.chars().filter(value -> value == '|').count())
                        .isEqualTo(columns + 1L));
    }
}
