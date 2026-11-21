/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 */
package org.pangrova.testing.integration.smoke;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Bootstrap self-test for the SI-8 smoke harness (P-0 Task 1).
 *
 * <p>This proves the {@code pangrova-testing-integration-tests} module compiles, the {@code smoke} JUnit tag is
 * discoverable, and the dedicated CI gate runs green on an otherwise-empty suite. Real
 * per-capability activation assertions replace/extend this as each {@code ⚙️activation} task lands.
 */
@SmokeTest
class SmokeHarnessSelfTest {

    @Test
    @DisplayName("smoke harness module is wired and runnable")
    void harnessIsWired() {
        assertThat(SmokeTest.class.isAnnotationPresent(org.junit.jupiter.api.Tag.class)).isTrue();
    }
}
