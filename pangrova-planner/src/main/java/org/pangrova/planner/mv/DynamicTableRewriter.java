package org.pangrova.planner.mv;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;

/**
 * Resolves Dynamic Table names to their published physical generations.
 *
 * <p>Only durable publications grant read authority. A name that has no
 * published generation is left untouched, and an unpublished or dropping
 * object fails closed through {@link DynamicTableNotReadyException} raised by
 * the resolver.</p>
 *
 * <p>Called from {@link org.pangrova.planner.PangrovaPlanner#plan(String)} before
 * Calcite table resolution to transparently redirect DT queries.</p>
 */
@org.pangrova.common.concurrent.ThreadSafe
public final class DynamicTableRewriter {

    /**
     * Reserved connector-session property binding kernel-authored materialization
     * execution. The coordinator always removes client copies before setting it
     * from its trusted execution context, so a client can never forge it.
     */
    public static final String INTERNAL_EXECUTION_PROPERTY =
            "pangrova.materialization.internal-execution";

    private final PublishedTargetResolver publishedTargetResolver;
    private final boolean allowDirectGenerationAccess;

    /** Resolves only durable, published physical generations for the current session. */
    public DynamicTableRewriter(PublishedTargetResolver publishedTargetResolver) {
        this(publishedTargetResolver, false);
    }

    /**
     * @param allowDirectGenerationAccess true only for kernel-authored
     *        materialization execution, which must read its pinned base
     *        generation; ordinary client SQL must never set this
     */
    public DynamicTableRewriter(PublishedTargetResolver publishedTargetResolver,
            boolean allowDirectGenerationAccess) {
        this.publishedTargetResolver = Objects.requireNonNull(publishedTargetResolver,
                "publishedTargetResolver");
        this.allowDirectGenerationAccess = allowDirectGenerationAccess;
    }

    /** Returns whether this session may name a reserved physical generation. */
    public boolean allowDirectGenerationAccess() {
        return allowDirectGenerationAccess;
    }

    /** Reads the reserved internal-execution binding from a converted session. */
    public static boolean isInternalExecution(org.pangrova.spi.connector.ConnectorSession session) {
        return "true".equals(session.getProperties().get(INTERNAL_EXECUTION_PROPERTY));
    }

    /** Component-preserving query-local authority for one published generation. */
    public record PublishedTarget(QualifiedTableName logical, QualifiedTableName physical) { }

    /** Resolves an AST identifier without flattening quoted components. */
    @FunctionalInterface
    public interface PublishedTargetResolver {
        Optional<PublishedTarget> resolve(List<String> nameParts);

        /** Qualifies an AST name without reading a publication, for query-local pin lookup. */
        default Optional<QualifiedTableName> canonicalLogical(List<String> nameParts) {
            return Optional.empty();
        }
    }

    /** Returns both identities for an AST reference in the durable publication mode. */
    public Optional<PublishedTarget> resolvePublishedTarget(List<String> nameParts) {
        return publishedTargetResolver.resolve(List.copyOf(nameParts));
    }

    /** Returns the durable logical identity before a publication read, when available. */
    public Optional<QualifiedTableName> canonicalPublishedLogical(List<String> nameParts) {
        return publishedTargetResolver.canonicalLogical(List.copyOf(nameParts));
    }
}
