/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.server.coordinator.bootstrap;

import java.util.ArrayList;
import java.util.List;

/**
 * Renders the coordinator's effective security posture as a high-visibility
 * startup banner.
 *
 * <p>Security defaults to OFF (a deliberate, config-driven choice for local/dev
 * ergonomics). The risk is that an operator ships to production without noticing
 * that authentication, row-level security, and column masking are silently
 * disabled. This banner surfaces the ACTUAL enforced state at boot and emits a
 * prominent warning when security is off, so the posture is never invisible.</p>
 *
 * <p>Pure and side-effect free so it can be unit-tested deterministically; the
 * caller decides the log level (WARN when insecure, INFO otherwise).</p>
 */
final class SecurityPostureSummary {

    private SecurityPostureSummary() {
    }

    /**
     * Formats the posture banner lines.
     *
     * @param securityEnabled whether {@code security.enabled=true}
     * @param authType        the configured auth type (only meaningful when enabled; may be null)
     * @param tracingEnabled  whether observability/tracing is enabled
     * @return the banner lines, in order
     */
    static List<String> format(boolean securityEnabled, String authType, boolean tracingEnabled) {
        List<String> lines = new ArrayList<>();
        lines.add("======== Pangrova Coordinator — Effective Security Posture ========");
        if (securityEnabled) {
            String type = (authType == null || authType.isBlank()) ? "UNSPECIFIED" : authType;
            lines.add("  Authentication            : ENABLED (type=" + type + ")");
            lines.add("  Row-level security (RLS)  : ENFORCED (applied per session after planning)");
            lines.add("  Column masking            : ENFORCED (applied per session after planning)");
        } else {
            lines.add("  Authentication            : DISABLED");
            lines.add("  Row-level security (RLS)  : NOT ENFORCED");
            lines.add("  Column masking            : NOT ENFORCED");
        }
        lines.add("  Distributed tracing (OTel): " + (tracingEnabled ? "ENABLED" : "DISABLED"));
        if (!securityEnabled) {
            lines.add("  !! SECURITY DISABLED — authentication, RLS, and column masking are NOT");
            lines.add("     enforced. This is safe for local/dev only. Set security.enabled=true and");
            lines.add("     security.auth.type (OAUTH2 or LDAP) before running in production.");
        }
        lines.add("=================================================================");
        return lines;
    }
}
