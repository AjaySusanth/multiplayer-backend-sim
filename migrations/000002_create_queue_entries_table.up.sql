CREATE TABLE IF NOT EXISTS queue_entries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    player_id UUID NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    skill_rating INT NOT NULL,
    region VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'QUEUED',
    joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    matched_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ NOT NULL
);

-- Enforce that a player can only have one active queue entry at a time.
CREATE UNIQUE INDEX idx_active_queue_entry ON queue_entries (player_id) WHERE status = 'QUEUED';