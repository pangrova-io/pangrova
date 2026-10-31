package org.pangrova.ai.worker.governance;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MimeTypeValidatorTest {
    @Test
    @ResourceLock("java-default-locale")
    void mimePolicyDoesNotDependOnTheProcessLocale() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            var validator = new MimeTypeValidator();
            var policy = policy("image/png");
            validator.validate("IMAGE/PNG", policy, "catalog.schema.files");
            assertThat(validator.isAllowed("IMAGE/PNG", policy)).isTrue();
            assertThat(validator.getAllowedTypes(policy("IMAGE/PNG"))).containsExactly("image/png");
        } finally {
            Locale.setDefault(previous);
        }
    }

    @Test
    void restrictedPolicyRejectsMissingMimeMetadata() {
        var validator = new MimeTypeValidator();
        for (String mime : new String[]{null, "", "   "}) {
            assertThatThrownBy(() -> validator.validate(mime, policy("image/*"), "catalog.schema.files"))
                    .isInstanceOf(MimeTypeValidator.MimeTypeNotAllowedException.class);
            assertThat(validator.isAllowed(mime, policy("image/*"))).isFalse();
        }
        assertThat(validator.isAllowed(null, policy("*/*"))).isTrue();
        validator.validate(null, policy("*/*"), "catalog.schema.files");
    }

    @Test
    void eachCapturedPolicyIsAppliedWithoutASecondCacheIdentity() {
        var validator = new MimeTypeValidator();
        validator.validate("image/png", policy("image/png"), "catalog.schema.files");
        assertThatThrownBy(() -> validator.validate("image/png", policy("application/pdf"),
                "catalog.schema.files")).isInstanceOf(MimeTypeValidator.MimeTypeNotAllowedException.class);
        assertThatThrownBy(() -> validator.validate("image/png", policy(" , , "),
                "catalog.schema.files")).isInstanceOf(MimeTypeValidator.MimeTypeNotAllowedException.class);
        assertThat(validator.isAllowed("application/pdf", policy("application/pdf"))).isTrue();
        assertThat(validator.getAllowedTypes(policy("image/*,application/pdf")))
                .containsExactlyInAnyOrder("image/*", "application/pdf");
    }

    private static Map<String, String> policy(String allowed) {
        return Map.of(MimeTypeValidator.ALLOWED_TYPES_PROPERTY, allowed);
    }
}
