/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 */
package org.pangrova.testing.integration.smoke;

import org.junit.jupiter.api.Tag;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a test as part of the SI-8 activation smoke suite.
 *
 * <p>A smoke test asserts that a subsystem has an <em>observable runtime effect</em> — not merely
 * that its classes load. Each {@code ⚙️activation} task in the architecture-remediation backlog
 * contributes one or more smoke assertions here (cache hit counter &gt; 0, spill bytes &gt; 0 under
 * pressure, trace_id non-zero across hops, RLS row dropped, ANN returns rows, CDC row committed,
 * failover recovers a query).
 *
 * <p>The suite runs as a dedicated CI gate via {@code -Dgroups=smoke}.
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Tag("smoke")
public @interface SmokeTest {
}
