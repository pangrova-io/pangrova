package org.pangrova.ai.api.invocation;

import org.pangrova.ai.api.embedding.BlobReference;
import org.pangrova.common.api.annotation.Experimental;

import java.util.Arrays;
import java.util.Objects;

/**
 * A single content part within a {@link MultimodalPrompt}.
 * Sealed hierarchy supporting text, file references, and inline binary content.
 */
@Experimental
public sealed interface ContentPart permits ContentPart.Text, ContentPart.FileRef, ContentPart.InlineImage {

    /**
     * A text content part.
     *
     * @param text the text content (must not be null)
     */
    record Text(String text) implements ContentPart {
        public Text {
            Objects.requireNonNull(text, "text must not be null");
        }
    }

    /**
     * A file reference content part pointing to an externally stored blob.
     * The provider will stream content from the URI or convert to base64/presigned URL.
     *
     * @param reference the blob reference (must not be null)
     */
    record FileRef(BlobReference reference) implements ContentPart {
        public FileRef {
            Objects.requireNonNull(reference, "reference must not be null");
        }
    }

    /**
     * An inline image content part with raw bytes already loaded.
     * Used when the image is small enough to embed directly (e.g., thumbnails).
     *
     * @param data     raw image bytes (must not be null or empty)
     * @param mimeType MIME type (e.g., "image/jpeg"; must not be null or blank)
     */
    record InlineImage(byte[] data, String mimeType) implements ContentPart {
        public InlineImage {
            if (data == null || data.length == 0) {
                throw new IllegalArgumentException("data must not be null or empty");
            }
            if (mimeType == null || mimeType.isBlank()) {
                throw new IllegalArgumentException("mimeType must not be null or blank");
            }
            data = Arrays.copyOf(data, data.length); // defensive copy
        }

        @Override
        public byte[] data() {
            return Arrays.copyOf(data, data.length); // defensive copy on access
        }
    }
}
