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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SecurityPostureSummary} (OBS startup posture banner).
 */
class SecurityPostureSummaryTest {

    private static String render(boolean security, String authType, boolean tracing) {
        return String.join("\n", SecurityPostureSummary.format(security, authType, tracing));
    }

    @Test
    @DisplayName("security enabled: shows auth type and enforced RLS/masking, no warning")
    void enabledPostureShowsEnforcement() {
        String out = render(true, "OAUTH2", true);
        assertThat(out).contains("Authentication            : ENABLED (type=OAUTH2)");
        assertThat(out).contains("Row-level security (RLS)  : ENFORCED");
        assertThat(out).contains("Column masking            : ENFORCED");
        assertThat(out).contains("Distributed tracing (OTel): ENABLED");
        assertThat(out).doesNotContain("SECURITY DISABLED");
    }

    @Test
    @DisplayName("security disabled: shows NOT ENFORCED and a prominent warning")
    void disabledPostureWarns() {
        String out = render(false, null, false);
        assertThat(out).contains("Authentication            : DISABLED");
        assertThat(out).contains("Row-level security (RLS)  : NOT ENFORCED");
        assertThat(out).contains("Column masking            : NOT ENFORCED");
        assertThat(out).contains("Distributed tracing (OTel): DISABLED");
        assertThat(out).contains("SECURITY DISABLED");
        assertThat(out).contains("security.enabled=true");
    }

    @Test
    @DisplayName("enabled with blank auth type renders UNSPECIFIED")
    void blankAuthTypeShowsUnspecified() {
        assertThat(render(true, "   ", true)).contains("ENABLED (type=UNSPECIFIED)");
    }

    @Test
    void explicitNoopReportsAnonymousAccessEvenWithSecurityEnabled() {
        assertThat(render(true, "noop", false)).contains("Authentication            : BYPASSED (type=noop)");
    }

    @Test
    @DisplayName("tracing flag is reflected independently of security")
    void tracingFlagReflected() {
        assertThat(render(true, "LDAP", false)).contains("Distributed tracing (OTel): DISABLED");
        assertThat(render(false, null, true)).contains("Distributed tracing (OTel): ENABLED");
    }

    @Test
    @DisplayName("banner is bounded and framed")
    void bannerIsFramed() {
        List<String> lines = SecurityPostureSummary.format(true, "OAUTH2", true);
        assertThat(lines.get(0)).contains("Effective Security Posture");
        assertThat(lines.get(lines.size() - 1)).startsWith("====");
    }
}
