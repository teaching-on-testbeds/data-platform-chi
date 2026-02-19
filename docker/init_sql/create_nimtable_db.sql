CREATE USER nimtable_user WITH PASSWORD 'gourmetgram_nimtable';
ALTER USER nimtable_user CREATEDB;
CREATE DATABASE nimtable OWNER nimtable_user;
GRANT ALL PRIVILEGES ON DATABASE nimtable TO nimtable_user;

\c nimtable
GRANT ALL ON SCHEMA public TO nimtable_user;
ALTER SCHEMA public OWNER TO nimtable_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO nimtable_user;
