package org.pangrova.parser.parse;

import org.pangrova.parser.api.parse.StatementParser;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * Registry of {@link StatementParser} extensions.
 *
 * <p>Extension parsers are discovered in two ways:
 * <ol>
 *   <li><b>ServiceLoader discovery</b> — at construction time, all
 *       {@code StatementParser} implementations declared in
 *       {@code META-INF/services/org.pangrova.parser.api.parse.StatementParser}
 *       are loaded and registered.</li>
 *   <li><b>Programmatic registration</b> — via {@link #register(StatementParser)}
 *       for testing or embedding scenarios.</li>
 * </ol>
 *
 * <p>When multiple parsers are registered, they are consulted in priority order
 * (lowest {@link StatementParser#priority()} first). The first parser whose
 * {@link StatementParser#canParse(String)} returns {@code true} wins.
 *
 * <p>Registration does not invalidate an owning parser's parse cache. Configure
 * extensions before serving queries, or explicitly invalidate that cache.
 *
 * <p>This class is thread-safe. Readers observe one immutable snapshot on the hot path; synchronized registration
 * publishes a fully sorted replacement once, without intermediate removals or gaps.
 */
@org.pangrova.common.concurrent.ThreadSafe
public final class StatementParserRegistry {

    private volatile List<StatementParser> parsers = List.of();

    /**
     * Creates an empty registry with no ServiceLoader discovery.
     * Use {@link #createWithServiceLoader()} for production usage.
     */
    public StatementParserRegistry() {
        // empty — programmatic registration only
    }

    /**
     * Creates a registry pre-populated with all {@link StatementParser} implementations
     * discovered via {@link ServiceLoader} on the current thread's context class loader.
     *
     * @return a new registry with ServiceLoader-discovered parsers registered
     */
    public static StatementParserRegistry createWithServiceLoader() {
        return createWithServiceLoader(Thread.currentThread().getContextClassLoader());
    }

    /**
     * Creates a registry pre-populated with all {@link StatementParser} implementations
     * discovered via {@link ServiceLoader} on the given class loader.
     *
     * @param classLoader the class loader to use for ServiceLoader discovery
     * @return a new registry with ServiceLoader-discovered parsers registered
     */
    public static StatementParserRegistry createWithServiceLoader(ClassLoader classLoader) {
        StatementParserRegistry registry = new StatementParserRegistry();
        ServiceLoader<StatementParser> loader = ServiceLoader.load(StatementParser.class, classLoader);
        for (StatementParser parser : loader) {
            registry.register(parser);
        }
        return registry;
    }

    /**
     * Registers a statement parser extension. If a parser with the same
     * {@link StatementParser#name()} is already registered, the new one
     * replaces it.
     *
     * @param parser the parser to register, must not be null
     * @throws IllegalArgumentException if parser is null or has a null/empty name
     */
    public synchronized void register(StatementParser parser) {
        if (parser == null) {
            throw new IllegalArgumentException("parser must not be null");
        }
        if (parser.name() == null || parser.name().isEmpty()) {
            throw new IllegalArgumentException("parser name must not be null or empty");
        }
        List<StatementParser> sorted = new ArrayList<>(parsers);
        sorted.removeIf(existing -> existing.name().equals(parser.name()));
        sorted.add(parser);
        sorted.sort(Comparator.comparingInt(StatementParser::priority));
        parsers = List.copyOf(sorted);
    }

    /**
     * Unregisters a statement parser by name.
     *
     * @param name the name of the parser to remove
     * @return {@code true} if a parser was removed
     */
    public synchronized boolean unregister(String name) {
        List<StatementParser> updated = new ArrayList<>(parsers);
        boolean removed = updated.removeIf(parser -> parser.name().equals(name));
        if (removed) parsers = List.copyOf(updated);
        return removed;
    }

    /**
     * Returns the first registered parser that can handle the given SQL,
     * consulting parsers in priority order (lowest priority value first).
     *
     * @param sql the SQL text to test
     * @return the matching parser, or empty if no extension can handle the SQL
     */
    public Optional<StatementParser> findParser(String sql) {
        for (StatementParser parser : parsers) {
            if (parser.canParse(sql)) {
                return Optional.of(parser);
            }
        }
        return Optional.empty();
    }

    /**
     * Returns an unmodifiable snapshot of all registered parsers, in priority order.
     */
    public List<StatementParser> registeredParsers() {
        return List.copyOf(parsers);
    }

    /**
     * Returns the number of registered extension parsers.
     */
    public int size() {
        return parsers.size();
    }
}
