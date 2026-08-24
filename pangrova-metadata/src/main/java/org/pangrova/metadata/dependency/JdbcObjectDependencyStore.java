package org.pangrova.metadata.dependency;

import org.pangrova.metadata.exception.MetastoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * JDBC-backed {@link ObjectDependencyStore} persisting edges to the
 * {@code pangrova_object_dependencies} table (see V9 migration).
 *
 * <p>The schema PK is {@code (parent_catalog, parent_schema, parent_object,
 * child_catalog, child_schema, child_object)} — kinds are NOT part of the
 * identity. This store treats inserts as idempotent on the natural key by
 * issuing a DELETE+INSERT inside the same statement when the edge already
 * exists; PostgreSQL supports ON CONFLICT but H2 compatibility varies —
 * to keep the implementation portable (PG production + H2 tests), we
 * delete-then-insert per row in the same
 * transaction. The cost is acceptable: dependency edges churn on DDL only,
 * which is rare relative to query traffic.
 *
 * <p>All multi-row operations run inside a single transaction so partial
 * failures roll back cleanly and the graph never observes a half-applied
 * replace.
 */
public final class JdbcObjectDependencyStore implements ObjectDependencyStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcObjectDependencyStore.class);

    private static final String INSERT_SQL =
            "INSERT INTO pangrova_object_dependencies "
                    + "(parent_catalog, parent_schema, parent_object, parent_kind, "
                    + "child_catalog, child_schema, child_object, child_kind) "
                    + "VALUES (?,?,?,?,?,?,?,?)";

    private static final String DELETE_EDGE_SQL =
            "DELETE FROM pangrova_object_dependencies "
                    + "WHERE parent_catalog=? AND parent_schema=? AND parent_object=? "
                    + "AND child_catalog=? AND child_schema=? AND child_object=?";

    private static final String DELETE_BY_CHILD_SQL =
            "DELETE FROM pangrova_object_dependencies "
                    + "WHERE child_catalog=? AND child_schema=? AND child_object=?";

    private static final String DELETE_BY_OBJECT_SQL =
            "DELETE FROM pangrova_object_dependencies "
                    + "WHERE (parent_catalog=? AND parent_schema=? AND parent_object=?) "
                    + "OR (child_catalog=? AND child_schema=? AND child_object=?)";

    private static final String SELECT_PARENTS_SQL =
            "SELECT parent_catalog, parent_schema, parent_object, parent_kind "
                    + "FROM pangrova_object_dependencies "
                    + "WHERE child_catalog=? AND child_schema=? AND child_object=?";

    private static final String SELECT_CHILDREN_SQL =
            "SELECT child_catalog, child_schema, child_object, child_kind "
                    + "FROM pangrova_object_dependencies "
                    + "WHERE parent_catalog=? AND parent_schema=? AND parent_object=?";

    private final DataSource dataSource;

    public JdbcObjectDependencyStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public void recordDependencies(ObjectRef child, Set<ObjectRef> parents) {
        Objects.requireNonNull(child, "child must not be null");
        Objects.requireNonNull(parents, "parents must not be null");
        if (parents.isEmpty()) {
            return;
        }
        try (Connection conn = dataSource.getConnection()) {
            boolean priorAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {
                upsertEdges(conn, child, parents);
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(priorAutoCommit);
            }
        } catch (SQLException e) {
            throw new MetastoreException(
                    "recordDependencies failed for child=" + child, e);
        }
    }

    @Override
    public void replaceDependenciesForChild(ObjectRef child, Set<ObjectRef> parents) {
        Objects.requireNonNull(child, "child must not be null");
        Objects.requireNonNull(parents, "parents must not be null");
        try (Connection conn = dataSource.getConnection()) {
            boolean priorAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try (PreparedStatement deleteStmt = conn.prepareStatement(DELETE_BY_CHILD_SQL)) {
                deleteStmt.setString(1, child.catalog());
                deleteStmt.setString(2, child.schema());
                deleteStmt.setString(3, child.name());
                deleteStmt.executeUpdate();
                if (!parents.isEmpty()) {
                    upsertEdges(conn, child, parents);
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(priorAutoCommit);
            }
        } catch (SQLException e) {
            throw new MetastoreException(
                    "replaceDependenciesForChild failed for child=" + child, e);
        }
    }

    @Override
    public int removeAllEdgesFor(ObjectRef obj) {
        Objects.requireNonNull(obj, "obj must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(DELETE_BY_OBJECT_SQL)) {
            ps.setString(1, obj.catalog());
            ps.setString(2, obj.schema());
            ps.setString(3, obj.name());
            ps.setString(4, obj.catalog());
            ps.setString(5, obj.schema());
            ps.setString(6, obj.name());
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new MetastoreException("removeAllEdgesFor failed for " + obj, e);
        }
    }

    @Override
    public List<ObjectRef> getParentsOf(ObjectRef child) {
        Objects.requireNonNull(child, "child must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_PARENTS_SQL)) {
            ps.setString(1, child.catalog());
            ps.setString(2, child.schema());
            ps.setString(3, child.name());
            try (ResultSet rs = ps.executeQuery()) {
                return readObjectRefs(rs);
            }
        } catch (SQLException e) {
            throw new MetastoreException("getParentsOf failed for " + child, e);
        }
    }

    @Override
    public List<ObjectRef> getDependentsOf(ObjectRef parent) {
        Objects.requireNonNull(parent, "parent must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_CHILDREN_SQL)) {
            ps.setString(1, parent.catalog());
            ps.setString(2, parent.schema());
            ps.setString(3, parent.name());
            try (ResultSet rs = ps.executeQuery()) {
                return readObjectRefs(rs);
            }
        } catch (SQLException e) {
            throw new MetastoreException("getDependentsOf failed for " + parent, e);
        }
    }

    @Override
    public List<ObjectRef> getTransitiveDependentsOf(ObjectRef parent) {
        Objects.requireNonNull(parent, "parent must not be null");
        // BFS over direct children. We collect in topological order: deepest first.
        // Cycle guard: visited set.
        Set<ObjectRef> visited = new HashSet<>();
        Deque<ObjectRef> frontier = new ArrayDeque<>();
        List<ObjectRef> order = new ArrayList<>();
        frontier.add(parent);
        visited.add(parent);
        while (!frontier.isEmpty()) {
            ObjectRef current = frontier.poll();
            for (ObjectRef child : getDependentsOf(current)) {
                if (visited.add(child)) {
                    order.add(child);
                    frontier.add(child);
                }
            }
        }
        // Reverse so deepest descendants come first (cascade-friendly).
        java.util.Collections.reverse(order);
        return order;
    }

    private static void upsertEdges(Connection conn, ObjectRef child, Set<ObjectRef> parents)
            throws SQLException {
        // Idempotent insert: delete the natural-key match if it exists, then insert.
        // Portable across PG and H2 (avoids ON CONFLICT syntax differences).
        try (PreparedStatement deleteEdge = conn.prepareStatement(DELETE_EDGE_SQL);
             PreparedStatement insert = conn.prepareStatement(INSERT_SQL)) {
            for (ObjectRef parent : parents) {
                if (parent.equals(child)) {
                    LOG.warn("event=dependency_self_edge_skipped obj={}", parent);
                    continue;
                }
                deleteEdge.setString(1, parent.catalog());
                deleteEdge.setString(2, parent.schema());
                deleteEdge.setString(3, parent.name());
                deleteEdge.setString(4, child.catalog());
                deleteEdge.setString(5, child.schema());
                deleteEdge.setString(6, child.name());
                deleteEdge.executeUpdate();

                insert.setString(1, parent.catalog());
                insert.setString(2, parent.schema());
                insert.setString(3, parent.name());
                insert.setString(4, parent.kind().name());
                insert.setString(5, child.catalog());
                insert.setString(6, child.schema());
                insert.setString(7, child.name());
                insert.setString(8, child.kind().name());
                insert.executeUpdate();
            }
        }
    }

    private static List<ObjectRef> readObjectRefs(ResultSet rs) throws SQLException {
        // Preserve discovery order for stable cascade ordering across calls.
        Set<ObjectRef> uniq = new LinkedHashSet<>();
        while (rs.next()) {
            String catalog = rs.getString(1);
            String schema = rs.getString(2);
            String name = rs.getString(3);
            ObjectKind kind = parseKind(rs.getString(4));
            uniq.add(new ObjectRef(catalog, schema, name, kind));
        }
        return List.copyOf(uniq);
    }

    private static ObjectKind parseKind(String raw) {
        if (raw == null) {
            return ObjectKind.EXTERNAL;
        }
        try {
            return ObjectKind.valueOf(raw);
        } catch (IllegalArgumentException e) {
            // Forward-compat: an unknown kind written by a newer node is treated
            // as EXTERNAL rather than crashing the lookup.
            LOG.warn("event=unknown_object_kind raw={} fallback=EXTERNAL", raw);
            return ObjectKind.EXTERNAL;
        }
    }
}
