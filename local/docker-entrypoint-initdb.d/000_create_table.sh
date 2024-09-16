#!/bin/bash
set -e

# projct .env files may be shared with psql containers that want to connect
# from outside the db container over tcp.
# In this case we want to connect via local socket (the default psql setting)
# so we need to remove the conflicting env vars that force tcp connection:
unset PGHOST # want to connect via local socket
unset PGPORT # want to connect via local socket
unset PGDATABASE # we have to create it
DB_TO_CREATE="warehouse"
psql -v ON_ERROR_STOP=1 <<-EOSQL
    DROP DATABASE IF EXISTS $DB_TO_CREATE;
    CREATE DATABASE $DB_TO_CREATE;
    GRANT ALL PRIVILEGES ON DATABASE $DB_TO_CREATE TO $PGUSER;
    \c $DB_TO_CREATE
    CREATE TABLE customer_orders (
        id BIGINT,
        description VARCHAR (255),
        some_unsigned_float DOUBLE PRECISION,
        some_positive_int BIGINT
    );
EOSQL
