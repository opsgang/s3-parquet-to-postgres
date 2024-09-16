#!/bin/bash
set -e

# pg types - postgres_types constant
# DOUBLE        FLOAT8
# BIGINT        INT8
# INT           INT4
# NUMERIC(x,y)  NUMERIC
# REAL          FLOAT4
# SMALLINT      INT2
# VARCHAR       VARCHAR

# projct .env files may be shared with psql containers that want to connect
# from outside the db container over tcp.
# In this case we want to connect via local socket (the default psql setting)
# so we need to remove the conflicting env vars that force tcp connection:
unset PGHOST # want to connect via local socket
unset PGPORT # want to connect via local socket
unset PGDATABASE # connect to default db
DB_TO_CREATE="testing"
psql -v ON_ERROR_STOP=1 <<-EOSQL
    DROP DATABASE IF EXISTS $DB_TO_CREATE;
    CREATE DATABASE $DB_TO_CREATE;
    GRANT ALL PRIVILEGES ON DATABASE $DB_TO_CREATE TO $PGUSER;
EOSQL
