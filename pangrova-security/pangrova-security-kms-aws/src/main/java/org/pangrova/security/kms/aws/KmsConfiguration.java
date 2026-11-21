package org.pangrova.security.kms.aws;

import java.util.Objects;

/**
 * Immutable configuration for AWS KMS envelope encryption.
 *
 * @param keyArn           the ARN of the KMS master key used for envelope encryption
 * @param region           the AWS region where the KMS key resides
 * @param endpointOverride optional endpoint override for testing (e.g., LocalStack); null for production
 */
public record KmsConfiguration(String keyArn, String region, String endpointOverride) {

    public KmsConfiguration {
        Objects.requireNonNull(keyArn, "keyArn must not be null");
        Objects.requireNonNull(region, "region must not be null");
        if (keyArn.isBlank()) {
            throw new IllegalArgumentException("keyArn must not be blank");
        }
        if (region.isBlank()) {
            throw new IllegalArgumentException("region must not be blank");
        }
    }

    /**
     * Creates a production configuration without endpoint override.
     */
    public KmsConfiguration(String keyArn, String region) {
        this(keyArn, region, null);
    }
}
