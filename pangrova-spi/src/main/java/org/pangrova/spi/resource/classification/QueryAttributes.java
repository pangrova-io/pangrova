package org.pangrova.spi.resource.classification;

import org.pangrova.spi.annotation.Experimental;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Inbound query attributes used by a {@link WorkloadClassifier} to resolve a
 * resource group.
 *
 * <p>Mirrors Trino's selector inputs: user, source, client tags, query type,
 * database, and source IP. Optional fields are absent when the dispatcher
 * cannot determine them; an absent field never satisfies a classifier rule
 * that constrains it.</p>
 *
 * @param user       the authenticated principal, if known
 * @param source     the client source identifier (e.g. application name), if known
 * @param clientTags free-form client tags supplied with the request; never null
 * @param queryType  the statement type (e.g. "SELECT", "INSERT"), if known
 * @param database   the default schema/database for the request, if known
 * @param sourceIp   the client source IP, if known
 */
@Experimental(reason = "G-RES-05: workload classification SPI")
public record QueryAttributes(
        Optional<String> user,
        Optional<String> source,
        Set<String> clientTags,
        Optional<String> queryType,
        Optional<String> database,
        Optional<String> sourceIp) {

    public QueryAttributes {
        user = user == null ? Optional.empty() : user;
        source = source == null ? Optional.empty() : source;
        clientTags = clientTags == null ? Set.of() : Set.copyOf(clientTags);
        queryType = queryType == null ? Optional.empty() : queryType;
        database = database == null ? Optional.empty() : database;
        sourceIp = sourceIp == null ? Optional.empty() : sourceIp;
    }

    /** Creates attributes carrying only a user. */
    public static QueryAttributes forUser(String user) {
        Objects.requireNonNull(user, "user must not be null");
        return new QueryAttributes(
                Optional.of(user), Optional.empty(), Set.of(),
                Optional.empty(), Optional.empty(), Optional.empty());
    }
}
