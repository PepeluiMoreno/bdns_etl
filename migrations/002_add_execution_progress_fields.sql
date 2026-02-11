-- Migration: Add progress tracking fields to etl_execution
-- Author: Claude Code
-- Date: 2026-02-09
-- Updated: 2026-02-10 (schema rename etl_admin -> etl_control)

-- Add new columns for real-time progress tracking
ALTER TABLE etl_control.etl_execution
ADD COLUMN IF NOT EXISTS entrypoint VARCHAR(255),
ADD COLUMN IF NOT EXISTS current_phase VARCHAR(50),
ADD COLUMN IF NOT EXISTS current_operation TEXT,
ADD COLUMN IF NOT EXISTS progress_percentage INTEGER DEFAULT 0;

-- Add comments
COMMENT ON COLUMN etl_control.etl_execution.entrypoint IS 'Script/entrypoint being executed';
COMMENT ON COLUMN etl_control.etl_execution.current_phase IS 'Current ETL phase: extracting, transforming, loading, validating';
COMMENT ON COLUMN etl_control.etl_execution.current_operation IS 'Specific operation currently in progress';
COMMENT ON COLUMN etl_control.etl_execution.progress_percentage IS 'Overall progress percentage (0-100)';

-- Create index on current_phase for filtering
CREATE INDEX IF NOT EXISTS ix_etl_execution_phase ON etl_control.etl_execution(current_phase);
