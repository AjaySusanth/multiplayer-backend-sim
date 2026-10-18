CREATE TABLE IF NOT EXISTS liveops_configurations (
    id UUID PRIMARY KEY,
    version INT NOT NULL UNIQUE,
    configuration_json JSONB NOT NULL,
    status VARCHAR(50) NOT NULL, -- 'DRAFT', 'ACTIVE', 'ARCHIVED'
    created_by VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    activated_at TIMESTAMP WITH TIME ZONE
);

-- Ensure there is at most one ACTIVE configuration at any time.
-- A partial unique index guarantees this at the database engine level.
CREATE UNIQUE INDEX IF NOT EXISTS idx_liveops_single_active 
ON liveops_configurations (status) 
WHERE status = 'ACTIVE';