package org.pangrova.common.property.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import org.pangrova.common.json.JsonEscaper;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link JsonEscaper} verifying RFC 8259 compliance
 * across arbitrary string inputs.
 *
 * <p><b>Validates: Requirements P0-3</b></p>
 */
class JsonEscaperPropertyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Property 1: For any string, wrapping the escaped output in quotes
     * produces valid JSON that Jackson can parse back to the original string.
     */
    @Property(tries = 200)
    void escapedOutputProducesValidJsonRoundTrip(@ForAll String input) throws JsonProcessingException {
        String escaped = JsonEscaper.escape(input);
        String json = "\"" + escaped + "\"";
        String parsed = MAPPER.readValue(json, String.class);
        assertThat(parsed).isEqualTo(input);
    }

    /**
     * Property 2: The escaped output never contains unescaped control characters
     * (U+0000–U+001F), unescaped double quotes, or unescaped backslashes.
     */
    @Property(tries = 200)
    void escapedOutputContainsNoRawSpecialCharacters(@ForAll String input) {
        String escaped = JsonEscaper.escape(input);
        for (int i = 0; i < escaped.length(); i++) {
            char ch = escaped.charAt(i);
            if (ch == '\\') {
                // Backslash must be followed by a valid escape character
                assertThat(i + 1).isLessThan(escaped.length());
                char next = escaped.charAt(i + 1);
                assertThat("\"\\bfnrtu").contains(String.valueOf(next));
                i++; // skip the escape sequence
                if (next == 'u') {
                    i += 4; // skip the 4 hex digits
                }
            } else {
                assertThat(ch).isNotEqualTo('"');
                assertThat(ch).isGreaterThanOrEqualTo((char) 0x20);
            }
        }
    }

    /**
     * Property 3: Escaping a string that contains no special characters
     * returns the same string instance (identity optimization).
     */
    @Property(tries = 200)
    void plainAsciiStringsReturnSameInstance(@ForAll char c) {
        if (c >= 0x20 && c != '"' && c != '\\') {
            String input = String.valueOf(c);
            assertThat(JsonEscaper.escape(input)).isSameAs(input);
        }
    }
}
