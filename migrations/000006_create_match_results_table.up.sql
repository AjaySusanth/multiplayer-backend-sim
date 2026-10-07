CREATE TABLE IF NOT EXISTS match_results (
    id UUID PRIMARY KEY,
    match_id UUID NOT NULL,
    player_id UUID NOT NULL,
    score INT NOT NULL,
    result VARCHAR(50) NOT NULL,
    submitted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    idempotency_key VARCHAR(255),
    CONSTRAINT fk_match FOREIGN KEY (match_id) REFERENCES matches(id) ON DELETE CASCADE,
    CONSTRAINT fk_player FOREIGN KEY (player_id) REFERENCES players(id) ON DELETE CASCADE
);
