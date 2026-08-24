package org.pangrova.spi.security.credential;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates G-META-03: VendedCredential SPI data model.
 */
class VendedCredentialTest {

    @Test
    void createsValidCredential() {
        Instant expiry = Instant.now().plus(1, ChronoUnit.HOURS);
        VendedCredential cred = new VendedCredential(
                "AKIA...", "secret", "token", expiry,
                "s3://bucket/warehouse/db/table/", Map.of("region", "us-east-1"));
        assertThat(cred.accessKeyId()).isEqualTo("AKIA...");
        assertThat(cred.secretAccessKey()).isEqualTo("secret");
        assertThat(cred.sessionToken()).isEqualTo("token");
        assertThat(cred.scope()).isEqualTo("s3://bucket/warehouse/db/table/");
        assertThat(cred.additionalProperties()).containsEntry("region", "us-east-1");
        assertThat(cred.isExpired()).isFalse();
        assertThat(cred.remainingSeconds()).isGreaterThan(3500);
    }

    @Test
    void detectsExpiredCredential() {
        Instant pastExpiry = Instant.now().minus(10, ChronoUnit.MINUTES);
        VendedCredential cred = new VendedCredential(
                "AKIA...", "secret", "", pastExpiry, "", Map.of());
        assertThat(cred.isExpired()).isTrue();
        assertThat(cred.remainingSeconds()).isNegative();
    }

    @Test
    void defaultsNullSessionTokenToEmpty() {
        VendedCredential cred = new VendedCredential(
                "AKIA...", "secret", null,
                Instant.now().plus(1, ChronoUnit.HOURS), null, null);
        assertThat(cred.sessionToken()).isEmpty();
        assertThat(cred.scope()).isEmpty();
        assertThat(cred.additionalProperties()).isEmpty();
    }

    @Test
    void rejectsNullAccessKeyId() {
        assertThatThrownBy(() -> new VendedCredential(
                null, "secret", "", Instant.now(), "", Map.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("accessKeyId");
    }

    @Test
    void rejectsNullExpiration() {
        assertThatThrownBy(() -> new VendedCredential(
                "AKIA", "secret", "", null, "", Map.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("expiration");
    }

    @Test
    void credentialRequestCreation() {
        CredentialRequest req = CredentialRequest.of(
                "prod-catalog", "arn:aws:iam::123:role/pangrova-read",
                "s3://bucket/db/table/", "tenant-1", "q-42");
        assertThat(req.catalogName()).isEqualTo("prod-catalog");
        assertThat(req.roleIdentity()).isEqualTo("arn:aws:iam::123:role/pangrova-read");
        assertThat(req.storagePrefix()).isEqualTo("s3://bucket/db/table/");
        assertThat(req.tenantId()).isEqualTo("tenant-1");
        assertThat(req.queryId()).isEqualTo("q-42");
        assertThat(req.sessionTags()).isEmpty();
    }
}
