CREATE TABLE IF NOT EXISTS audit_logs (
    id UUID PRIMARY KEY,
    actor_id VARCHAR(100) NOT NULL,
    action VARCHAR(100) NOT NULL,          -- e.g., 'CONFIG_CREATED', 'CONFIG_ACTIVATED', 'CONFIG_ROLLED_BACK'
    resource_type VARCHAR(100) NOT NULL,   -- e.g., 'LIVEOPS_CONFIG'
    resource_id VARCHAR(100) NOT NULL,     -- e.g., version number or config UUID
    metadata_json JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Index created_at for fast time-ordered retrieval of logs
CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs (created_at DESC);