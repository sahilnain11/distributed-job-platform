-- Flyway creates this schema before applying versioned migrations.
-- Job tables belong to Phase 2, not the foundation milestone.
COMMENT ON SCHEMA job_platform IS 'Distributed job platform application data';