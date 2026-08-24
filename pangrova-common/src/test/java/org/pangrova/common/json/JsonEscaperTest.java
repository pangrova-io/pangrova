package org.pangrova.common.json;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link JsonEscaper} covering RFC 8259 §7 escaping rules.
 */
class JsonEscaperTest {

    @Test
    void normalStringPassesThrough() {
        assertThat(JsonEscaper.escape("hello world")).isEqualTo("hello world");
    }

    @Test
    void emptyStringPassesThrough() {
        assertThat(JsonEscaper.escape("")).isEqualTo("");
    }

    @Test
    void escapesDoubleQuote() {
        assertThat(JsonEscaper.escape("say \"hello\"")).isEqualTo("say \\\"hello\\\"");
    }

    @Test
    void escapesBackslash() {
        assertThat(JsonEscaper.escape("path\\to\\file")).isEqualTo("path\\\\to\\\\file");
    }

    @Test
    void escapesNewline() {
        assertThat(JsonEscaper.escape("line1\nline2")).isEqualTo("line1\\nline2");
    }

    @Test
    void escapesCarriageReturn() {
        assertThat(JsonEscaper.escape("line1\rline2")).isEqualTo("line1\\rline2");
    }

    @Test
    void escapesTab() {
        assertThat(JsonEscaper.escape("col1\tcol2")).isEqualTo("col1\\tcol2");
    }

    @Test
    void escapesBackspace() {
        assertThat(JsonEscaper.escape("a\bb")).isEqualTo("a\\bb");
    }

    @Test
    void escapesFormFeed() {
        assertThat(JsonEscaper.escape("a\fb")).isEqualTo("a\\fb");
    }

    @Test
    void escapesControlCharactersAsUnicodeSequence() {
        assertThat(JsonEscaper.escape("\u0000")).isEqualTo("\\u0000");
        assertThat(JsonEscaper.escape("\u0001")).isEqualTo("\\u0001");
        assertThat(JsonEscaper.escape("\u001f")).isEqualTo("\\u001f");
    }

    @Test
    void unicodeAboveControlRangePassesThrough() {
        assertThat(JsonEscaper.escape("日本語")).isEqualTo("日本語");
        assertThat(JsonEscaper.escape("émojis: 🎉")).isEqualTo("émojis: 🎉");
    }

    @Test
    void mixedContentEscapesCorrectly() {
        String input = "s3://bucket/path with \"quotes\" and \\backslash\nand newline";
        String expected = "s3://bucket/path with \\\"quotes\\\" and \\\\backslash\\nand newline";
        assertThat(JsonEscaper.escape(input)).isEqualTo(expected);
    }

    @Test
    void nullInputThrowsNullPointerException() {
        assertThatThrownBy(() -> JsonEscaper.escape(null))
                .isInstanceOf(NullPointerException.class);
    }
}
