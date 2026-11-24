package org.pangrova.security.api.authorization;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.security.api.policy.PrivilegeGrant;

import java.util.List;
import java.util.Set;

/** Resolves the roles and grants used by one foreground read authorization. */
@Internal
@FunctionalInterface
public interface ReadAuthorizationSubjectSource {
    Subject read(String principal, Set<String> groups);

    record Subject(Set<String> roles, List<PrivilegeGrant> grants) {
        public Subject {
            roles = Set.copyOf(roles);
            grants = List.copyOf(grants);
        }
    }
}
