-- Store the exact Worker fragments and canonical write binding as binary data.
CREATE FUNCTION decode_write_redo_fragments(encoded text)
RETURNS bytea[] LANGUAGE sql IMMUTABLE AS $$
    SELECT ARRAY(
        SELECT decode(part, 'base64')
        FROM unnest(string_to_array(encoded, ',')) WITH ORDINALITY AS fragments(part, ordinal)
        ORDER BY ordinal
    );
$$;

ALTER TABLE write_commit_journal
    ALTER COLUMN fragments TYPE bytea[] USING decode_write_redo_fragments(fragments),
    ALTER COLUMN write_output_binding TYPE bytea USING decode(write_output_binding, 'base64');

ALTER TABLE write_commit_journal
    ALTER COLUMN fragments SET DEFAULT ARRAY[]::bytea[],
    ALTER COLUMN fragments SET NOT NULL;

DROP FUNCTION decode_write_redo_fragments(text);
