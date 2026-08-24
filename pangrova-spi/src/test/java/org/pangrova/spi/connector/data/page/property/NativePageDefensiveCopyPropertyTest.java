package org.pangrova.spi.connector.data.page.property;

import org.pangrova.spi.connector.data.page.BatchFormat;
import org.pangrova.spi.connector.data.page.NativePage;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based test for NativePage defensive copy immutability.
 *
 * <p>Verifies that after constructing a NativePage, mutating the original
 * byte array does NOT affect the content returned by {@code serializedBatch()}.</p>
 *
 * <p><b>Validates: Requirements 1.6</b></p>
 */
class NativePageDefensiveCopyPropertyTest {

    /**
     * Property 2: NativePage defensive copy immutability.
     *
     * <p>For any byte array, after constructing a NativePage, modifying the
     * original array should NOT affect {@code serializedBatch()} return content.
     * The returned bytes must match the original (pre-mutation) content.</p>
     *
     * <p><b>Validates: Requirements 1.6</b></p>
     */
    @Property(tries = 200)
    void mutatingOriginalArrayDoesNotAffectSerializedBatch(
            @ForAll("byteArrays") byte[] original,
            @ForAll("formats") BatchFormat format) {

        byte[] snapshot = original.clone();

        NativePage page = new NativePage(original, snapshot.length, 1, format);

        // Mutate every byte of the original array
        for (int i = 0; i < original.length; i++) {
            original[i] = (byte) ~original[i];
        }

        assertThat(page.serializedBatch())
                .as("serializedBatch() must return the original content, unaffected by external mutation")
                .isEqualTo(snapshot);
    }

    @Provide
    Arbitrary<byte[]> byteArrays() {
        return Arbitraries.bytes().array(byte[].class).ofMinSize(1).ofMaxSize(512);
    }

    @Provide
    Arbitrary<BatchFormat> formats() {
        return Arbitraries.of(BatchFormat.values());
    }
}
