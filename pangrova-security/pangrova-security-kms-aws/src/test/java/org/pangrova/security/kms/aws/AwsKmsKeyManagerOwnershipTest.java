package org.pangrova.security.kms.aws;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kms.KmsClient;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class AwsKmsKeyManagerOwnershipTest {
    @Test
    void closesTheOwnedClientOnce() {
        var closes = new AtomicInteger();
        KmsClient client = new KmsClient() {
            @Override
            public String serviceName() {
                return "kms";
            }

            @Override
            public void close() {
                closes.incrementAndGet();
            }
        };
        var manager = new AwsKmsKeyManager(client, "alias/test");

        manager.close();
        manager.close();

        assertThat(closes.get()).isOne();
    }
}
