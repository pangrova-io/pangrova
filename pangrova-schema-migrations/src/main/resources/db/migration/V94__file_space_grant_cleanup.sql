-- Volume and Stage grants follow the space name. Dropping a space revokes exact
-- grants in the same transaction; renaming moves them with the object.
-- Wildcard grants deliberately authorize all current and future names.

CREATE OR REPLACE FUNCTION revoke_dropped_file_space_grants()
RETURNS trigger LANGUAGE plpgsql AS $cleanup$
BEGIN
    DELETE FROM security_grants
    WHERE upper(object_type) = OLD.space_type
      AND catalog_name = OLD.catalog_name
      AND schema_name = OLD.schema_name
      AND table_name = OLD.space_name
      AND column_name IS NULL;
    RETURN OLD;
END;
$cleanup$;

CREATE OR REPLACE FUNCTION move_renamed_file_space_grants()
RETURNS trigger LANGUAGE plpgsql AS $cleanup$
BEGIN
    IF NEW.space_name IS DISTINCT FROM OLD.space_name THEN
        UPDATE security_grants
        SET table_name = NEW.space_name
        WHERE upper(object_type) = NEW.space_type
          AND catalog_name = NEW.catalog_name
          AND schema_name = NEW.schema_name
          AND table_name = OLD.space_name
          AND column_name IS NULL;
    END IF;
    RETURN NEW;
END;
$cleanup$;

DO $install$
BEGIN
    IF to_regclass('security_grants') IS NOT NULL THEN
        EXECUTE 'CREATE OR REPLACE TRIGGER file_space_drop_revoke_grants '
             || 'AFTER DELETE ON file_space_registry FOR EACH ROW '
             || 'EXECUTE FUNCTION revoke_dropped_file_space_grants()';
        EXECUTE 'CREATE OR REPLACE TRIGGER file_space_rename_move_grants '
             || 'AFTER UPDATE ON file_space_registry FOR EACH ROW '
             || 'EXECUTE FUNCTION move_renamed_file_space_grants()';
    END IF;
END
$install$;

-- Security bootstrap installs the gate function; migrations may run first.
DO $install$
BEGIN
    IF to_regprocedure('pangrova_authority_mutation_gate()') IS NOT NULL THEN
        EXECUTE 'CREATE OR REPLACE TRIGGER pangrova_authority_gate '
             || 'BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON file_space_registry '
             || 'FOR EACH STATEMENT EXECUTE FUNCTION pangrova_authority_mutation_gate()';
    END IF;
END
$install$;
