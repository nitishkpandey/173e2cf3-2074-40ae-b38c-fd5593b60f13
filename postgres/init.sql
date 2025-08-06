-- Create user if not exists
DO
$$
BEGIN
   IF NOT EXISTS (
      SELECT
      FROM   pg_catalog.pg_roles
      WHERE  rolname = 'energy_user') THEN

      CREATE ROLE energy_user WITH LOGIN PASSWORD 'energy_pass';
   END IF;
END
$$;

-- Create database if not exists and grant access
CREATE DATABASE energy
    WITH
    OWNER = energy_user
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1;

GRANT ALL PRIVILEGES ON DATABASE energy TO energy_user;
