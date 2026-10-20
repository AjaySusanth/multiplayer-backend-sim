ALTER TABLE match_results
DROP CONSTRAINT IF EXISTS uq_match_results_match_player;

ALTER TABLE idempotency_keys
DROP CONSTRAINT IF EXISTS idempotency_keys_pkey;

ALTER TABLE idempotency_keys
DROP COLUMN IF EXISTS scope_id;

ALTER TABLE idempotency_keys
ADD PRIMARY KEY (idempotency_key, operation);
