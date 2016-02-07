CREATE SCHEMA IF NOT EXISTS vicmap;

-- Create types
DO $$
BEGIN
	--vicmap.state
    IF NOT EXISTS (SELECT 1 FROM pg_type t LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace WHERE nspname='vicmap' AND typname = 'state' ) THEN
        CREATE TYPE vicmap.state AS ENUM ('VIC','NSW','SA','QLD','ACT','WA','TAS','NT');
    END IF;
END$$;

