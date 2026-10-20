-- 1. Ensure a player cannot have more than one result in the same match
ALTER TABLE match_results
ADD CONSTRAINT uq_match_results_match_player UNIQUE (match_id, player_id);

-- 2. Scope idempotency keys by entity/player to prevent cross-player key collisions
ALTER TABLE idempotency_keys
ADD COLUMN scope_id VARCHAR(255) NOT NULL DEFAULT '';

ALTER TABLE idempotency_keys
DROP CONSTRAINT idempotency_keys_pkey;

ALTER TABLE idempotency_keys
ADD PRIMARY KEY (idempotency_key, operation, scope_id);
