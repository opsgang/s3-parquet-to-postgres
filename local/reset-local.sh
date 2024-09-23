#!/usr/bin/env bash
# vim: et sr sw=4 ts=4 smartindent syntax=bash:
set -e

BINARY="s3-parquet-to-postgres"
RELEASE_BUILD=""
BUILD_MODE="debug"
if [[ "$1" =~ ^(-r|-?-rel(ease)?) ]]; then
    RELEASE_BUILD="-r"
    BUILD_MODE="release"
fi

# get path to dir (local/) containing script
LOCAL_DIR="$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)"

cd "$LOCAL_DIR"
mkdir -p "$LOCAL_DIR"/{out,work}
rm -f "$LOCAL_DIR"/out/*.parquet
rm -f "$LOCAL_DIR"/work/*
cp "$LOCAL_DIR/todo" "$LOCAL_DIR/work/todo"

# Running tests will restart the docker stack, but that doesn't happen in release mode.
if [[ -n "$RELEASE_BUILD" ]]; then
    docker compose down --volumes --remove-orphans
    docker compose up -d --wait
fi

cd .. # should be repo root dir
cargo build ${RELEASE_BUILD} # do not quote the var, as cargo picks up the zero length string!

# env vars for s3 download from localstack
unset AWS_SESSION_TOKEN
export AWS_ACCESS_KEY_ID=foo
export AWS_SECRET_ACCESS_KEY=bar
export AWS_DEFAULT_REGION=eu-west-1
export AWS_ENDPOINT_URL=http://127.0.0.1:4566

[[ -n  "$RELEASE_BUILD" ]] || cargo test

# log only info for everything apart from our specific modules.
# Note this doesn't cause other crates used in those mods to log at debug.
export RUST_LOG=info,s3_parquet_to_postgres=debug,parquet_ops=debug,s3_download=debug,work_lists=debug,db=debug
time "target/$BUILD_MODE/$BINARY" "$LOCAL_DIR/config.yml"
