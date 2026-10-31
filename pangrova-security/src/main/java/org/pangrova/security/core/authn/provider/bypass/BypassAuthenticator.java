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
package org.pangrova.security.core.authn.provider.bypass;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

import org.pangrova.security.api.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.pangrova.security.api.authn.AuthenticationRequest;
import org.pangrova.security.api.authn.AuthenticationResult;
import org.pangrova.security.api.authn.Authenticator;

/**
 * Authenticator that accepts all requests with a default anonymous identity.
 * Used when security is enabled but no real authentication backend is configured.
 */
public class BypassAuthenticator implements Authenticator {

    private static final Logger LOG = LoggerFactory.getLogger(BypassAuthenticator.class);

    private static final UserIdentity ANONYMOUS = new UserIdentity(
            "anonymous", Set.of("public"), Map.of(), Instant.EPOCH);

    @Override
    public String getName() {
        return "bypass";
    }

    @Override
    public void init(Map<String, String> config) {
        String enabled = config.getOrDefault("bypass.enabled", "false");
        if (!"true".equalsIgnoreCase(enabled)) {
            throw new IllegalStateException(
                    "BypassAuthenticator requires explicit opt-in: set bypass.enabled=true in security config. "
                    + "This authenticator grants unconditional access and must NEVER be used in production.");
        }
        LOG.warn("event=bypass_authenticator_enabled action=ALL_REQUESTS_WILL_BE_AUTHENTICATED_AS_ANONYMOUS");
    }

    @Override
    public AuthenticationResult authenticate(AuthenticationRequest request) {
        return AuthenticationResult.success(ANONYMOUS);
    }

    @Override
    public boolean supports(AuthenticationRequest request) {
        return true;
    }

    @Override
    public boolean isHealthy() {
        return true;
    }

    @Override
    public boolean bypassesAuthentication() {
        return true;
    }

    @Override
    public void close() {
        // no resources to release
    }
}
