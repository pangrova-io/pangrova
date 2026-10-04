package org.pangrova.spi.connector.metadata.table;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Pure identity presentation properties; this key never grants a read or accepts a SQL name. */
class QualifiedTableCanonicalKeyTest {
    @Test
    void literalDotsAndQuotesCannotCollideWithQualification() {
        var first = new QualifiedTableName("cat", "a.b", "rows");
        var second = new QualifiedTableName("cat", "a", "b.rows");
        assertThat(first.toString()).isEqualTo(second.toString());
        assertThat(first.canonicalKey()).isNotEqualTo(second.canonicalKey());
        assertThat(decode(first.canonicalKey())).containsExactly("cat", "a.b", "rows");
        assertThat(decode(new QualifiedTableName("cat", "\"a:b\"", "😀.rows").canonicalKey()))
                .containsExactly("cat", "\"a:b\"", "😀.rows");
    }

    @Test
    void sourceSpellingEmptyComponentsAndLargeUnicodeArePreservedWithoutCaseFolding() {
        var value = new QualifiedTableName("", "Rows\0", "😀雪".repeat(65_536));
        assertThat(decode(value.canonicalKey())).containsExactly(value.catalog(), value.schema(), value.table());
        assertThat(value.canonicalKey()).startsWith("TABLE/");
        assertThat(new QualifiedTableName("Cat", "Db", "Rows").canonicalKey())
                .isNotEqualTo(new QualifiedTableName("cat", "db", "rows").canonicalKey());
        assertThat(new QualifiedTableName("cat", "db", "rows").toString()).isEqualTo("cat.db.rows");
        assertThatThrownBy(() -> new QualifiedTableName(null, "db", "rows").canonicalKey())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void twoThousandIndependentComponentTuplesRoundTripAndCannotShareDifferentKeys() {
        var random = new Random(41);
        var seen = new java.util.HashMap<String, List<String>>();
        for (int trial = 0; trial < 2_000; ++trial) {
            var components = List.of(component(random), component(random), component(random));
            var value = new QualifiedTableName(components.get(0), components.get(1), components.get(2));
            var key = value.canonicalKey();
            assertThat(decode(key)).isEqualTo(components);
            var previous = seen.putIfAbsent(key, components);
            if (previous != null) assertThat(previous).isEqualTo(components);
        }
    }

    private static String component(Random random) {
        var alphabet = List.of("a", ".", ":", "\"", "\0", "雪", "😀");
        var result = new StringBuilder();
        int length = random.nextInt(20);
        for (int i = 0; i < length; ++i) result.append(alphabet.get(random.nextInt(alphabet.size())));
        return result.toString();
    }

    private static List<String> decode(String key) {
        assertThat(key).startsWith("TABLE/");
        int cursor = "TABLE/".length();
        var components = new java.util.ArrayList<String>();
        for (int i = 0; i < 3; ++i) {
            int separator = key.indexOf(':', cursor);
            int length = Integer.parseInt(key.substring(cursor, separator));
            cursor = separator + 1;
            components.add(key.substring(cursor, cursor + length));
            cursor += length;
        }
        assertThat(cursor).isEqualTo(key.length());
        return components;
    }
}
