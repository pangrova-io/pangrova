package org.pangrova.security.store;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/** PostgreSQL transaction lock shared by publication and every authority mutation. */
public final class AuthorizationTransactionGate {
    private static final String SHARED_LOCK = "SELECT pg_advisory_xact_lock_shared(207578817, 1)";
    private static final String EXCLUSIVE_LOCK = "SELECT pg_advisory_xact_lock(207578817, 1)";
    private static final List<String> AUTHORITY_TABLES = List.of(
            "pangrova_roles", "pangrova_user_roles", "pangrova_group_roles", "security_grants",
            "row_policy", "column_mask", "tenants", "tenant_user_memberships",
            "pangrova_embedding_models", "storage_profile_definitions");

    private AuthorizationTransactionGate() {}

    /** Acquires the gate before any job, descriptor or publication row lock. */
    public static void lockPublication(Connection connection) throws SQLException {
        if (connection.getAutoCommit()) {
            throw new IllegalStateException("Publication authority requires an explicit transaction");
        }
        execute(connection, SHARED_LOCK);
    }

    /** Acquires the mutation gate before schema or authority row locks. Ordinary query reads do not use it. */
    public static void lockMutation(Connection connection) throws SQLException {
        if (connection.getAutoCommit()) {
            throw new IllegalStateException("Authority mutation requires an explicit transaction");
        }
        execute(connection, EXCLUSIVE_LOCK);
    }

    /** Installs statement-level mutation gates atomically, including direct SQL provisioning. */
    public static void initialize(Connection connection) throws SQLException {
        if (connection.getAutoCommit()) {
            throw new IllegalStateException("Authority gate initialization requires an explicit transaction");
        }
        lockMutation(connection);
        execute(connection, """
                CREATE OR REPLACE FUNCTION pangrova_authority_mutation_gate()
                RETURNS trigger LANGUAGE plpgsql AS $gate$
                BEGIN
                    IF TG_OP='TRUNCATE' THEN
                        RAISE EXCEPTION 'Authority tables cannot be truncated; use gated DELETE';
                    END IF;
                    PERFORM pg_advisory_xact_lock(207578817, 1);
                    RETURN NULL;
                END
                $gate$
                """);
        for (String table : AUTHORITY_TABLES) {
            installTrigger(connection, table);
        }
        installRoleCleanup(connection);
        installStorageProfileGrantGuard(connection);
    }

    /** Exact data/metadata read grants bind to an existing profile; direct SQL follows the same rule. */
    private static void installStorageProfileGrantGuard(Connection connection) throws SQLException {
        if (!tableExists(connection, "security_grants")) {
            return;
        }
        execute(connection, """
                CREATE OR REPLACE FUNCTION pangrova_storage_profile_grant_guard()
                RETURNS trigger LANGUAGE plpgsql AS $guard$
                BEGIN
                    IF upper(NEW.object_type) = 'STORAGE_PROFILE'
                            AND upper(NEW.privilege) IN (
                                'READ_STORAGE_PROFILE', 'VIEW_STORAGE_PROFILE', 'ALL', 'OWNER')
                            AND NEW.catalog_name IS DISTINCT FROM '*' THEN
                        IF NEW.catalog_name IS NULL OR NEW.schema_name IS NOT NULL
                                OR NEW.table_name IS NOT NULL OR NEW.column_name IS NOT NULL
                                OR to_regclass('storage_profile_definitions') IS NULL THEN
                            RAISE EXCEPTION 'Storage profile read grant requires an existing exact profile';
                        END IF;
                        PERFORM 1 FROM storage_profile_definitions
                        WHERE name = NEW.catalog_name FOR KEY SHARE;
                        IF NOT FOUND THEN
                            RAISE EXCEPTION 'Storage profile read grant requires an existing exact profile';
                        END IF;
                    END IF;
                    RETURN NEW;
                END
                $guard$
                """);
        execute(connection, "CREATE OR REPLACE TRIGGER pangrova_storage_profile_grant_guard "
                + "BEFORE INSERT OR UPDATE ON security_grants FOR EACH ROW "
                + "EXECUTE FUNCTION pangrova_storage_profile_grant_guard()");
    }

    private static void installRoleCleanup(Connection connection) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT to_regclass('pangrova_roles')" );
             var result = statement.executeQuery()) {
            result.next();
            if (result.getString(1) == null) {
                return;
            }
        }
        execute(connection, """
                CREATE OR REPLACE FUNCTION pangrova_role_drop_cleanup()
                RETURNS trigger LANGUAGE plpgsql AS $cleanup$
                BEGIN
                    DELETE FROM security_grants WHERE role_name=OLD.role_name;
                    DELETE FROM pangrova_user_roles WHERE role_name=OLD.role_name;
                    DELETE FROM pangrova_group_roles WHERE role_name=OLD.role_name;
                    IF to_regclass('pangrova_embedding_models') IS NOT NULL THEN
                        UPDATE pangrova_embedding_models
                        SET admin_role=CASE WHEN admin_role=OLD.role_name THEN NULL ELSE admin_role END,
                            usage_role=CASE WHEN usage_role=OLD.role_name THEN NULL ELSE usage_role END
                        WHERE admin_role=OLD.role_name OR usage_role=OLD.role_name;
                    END IF;
                    RETURN OLD;
                END
                $cleanup$
                """);
        execute(connection, "CREATE OR REPLACE TRIGGER pangrova_role_drop_cleanup "
                + "AFTER DELETE ON pangrova_roles FOR EACH ROW EXECUTE FUNCTION pangrova_role_drop_cleanup()");
    }

    private static void installTrigger(Connection connection, String table) throws SQLException {
        if (!AUTHORITY_TABLES.contains(table)) {
            throw new IllegalArgumentException("Authority trigger requires a fixed system table");
        }
        if (!tableExists(connection, table)) {
            return;
        }
        execute(connection, "CREATE OR REPLACE TRIGGER pangrova_authority_gate "
                + "BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON " + table
                + " FOR EACH STATEMENT EXECUTE FUNCTION pangrova_authority_mutation_gate()");
    }

    private static boolean tableExists(Connection connection, String table) throws SQLException {
        try (var query = connection.prepareStatement("SELECT to_regclass(?)")) {
            query.setString(1, table);
            try (var result = query.executeQuery()) {
                result.next();
                return result.getString(1) != null;
            }
        }
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
