-- Role removal revokes the model policy itself, not only the current memberships.
-- The statement-level role mutation gate precedes role/model rows; publication
-- retains the shared gate until its transaction commits.
SELECT pg_advisory_xact_lock(207578817, 1);

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
$cleanup$;

-- Security bootstrap owns creation of its policy tables. Install on existing
-- authorities now; its atomic initialization installs the same trigger on new ones.
DO $install$
BEGIN
    IF to_regclass('pangrova_roles') IS NOT NULL THEN
        CREATE OR REPLACE TRIGGER pangrova_role_drop_cleanup
        AFTER DELETE ON pangrova_roles FOR EACH ROW
        EXECUTE FUNCTION pangrova_role_drop_cleanup();
    END IF;
END
$install$;
