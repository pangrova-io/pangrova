package org.pangrova.coordinator.query.resourcegroup;

import java.util.Optional;
import java.util.Set;

/**
 * Inbound-query attributes a {@link ResourceGroupSelector} matches rules against.
 *
 * <p>Mirrors the Trino selector inputs (user, source, client tags, query type, schema,
 * source IP). Optional fields are absent when the dispatcher cannot determine them; an absent
 * field never satisfies a rule criterion that constrains it.</p>
 *
 * @param user      the authenticated principal, if known
 * @param source    the client source identifier (e.g. application name), if known
 * @param clientTags free-form client tags supplied with the request; never null
 * @param queryType the statement type (e.g. {@code "SELECT"}, {@code "INSERT"}), if known
 * @param database  the default schema/database for the request, if known
 * @param sourceIp  the client source IP, if known
 */
public record ResourceGroupSelectionContext(
        Optional<String> user,
        Optional<String> source,
        Set<String> clientTags,
        Optional<String> queryType,
        Optional<String> database,
        Optional<String> sourceIp) {

    public ResourceGroupSelectionContext {
        user = user == null ? Optional.empty() : user;
        source = source == null ? Optional.empty() : source;
        clientTags = clientTags == null ? Set.of() : Set.copyOf(clientTags);
        queryType = queryType == null ? Optional.empty() : queryType;
        database = database == null ? Optional.empty() : database;
        sourceIp = sourceIp == null ? Optional.empty() : sourceIp;
    }

    /** Builder-style factory for a context carrying only a user. */
    public static ResourceGroupSelectionContext forUser(String user) {
        return new ResourceGroupSelectionContext(
                Optional.ofNullable(user), Optional.empty(), Set.of(),
                Optional.empty(), Optional.empty(), Optional.empty());
    }
}
