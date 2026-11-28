-- A durable exchange keeps the exact provider message apart from the replay receipt.
ALTER TABLE artifact_exchange_finalizations
    ADD COLUMN provider_response BYTEA NOT NULL,
    ADD CONSTRAINT artifact_exchange_provider_response_size
        CHECK (octet_length(provider_response) BETWEEN 1 AND 16777216),
    ADD CONSTRAINT artifact_exchange_provider_response_digest
        CHECK (encode(sha256(provider_response), 'hex') IS NOT DISTINCT FROM
            (convert_from(finalization_bytes, 'UTF8')::jsonb ->> 'responseSha256'));
