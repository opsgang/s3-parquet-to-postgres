# s3-pq-to-pg

[1]: https://arrow.apache.org/rust/parquet/record/enum.Field.html "parquet to rust type mapping"
[2]: https://docs.rs/postgres-types/latest/postgres_types/trait.ToSql.html#types "rust to postgres type mapping"

>
> Concurrently downloads parquet files from s3 in batches from a todo list.
> Serially reads specified fields from each parquet file into a postgres db table
> All configuration via a yaml file.
>

Got data in parquet files you need to get into a postgres db?
Are the parquet files stored in S3?

If you say yes to both, we're probably working for the same company and you don't
need to reinvent the wheel. Here you go.

## RUN

See BINARY.md for examples

## CAVEAT

The columns in your destination db table are expected to have data types compatible with the
corresponding parquet file's column types _as resolved by the parquet crate_.

So if you have a parquet DOUBLE value (stored as rust f64), and you try to stick that in a postgres
SMALLINT, expect a stack trace, even if the values ought to fit.

Check [the Parquet crate docs][1] for `Field` `enum`
for the internal rust types that correspond to the parquet types.

Check [here][2] for the mapping of rust to postgres data types.

## PERFORMANCE

Honestly, the downloading from s3 will be the biggest time suck.

Optimising the rust code is left as an exercise for those with more patience.
I clone `Strings` like a mo'fo' as it doesn't hurt my use-case.
Really this'll only hurt if your parquet has many, many columns you wish
to push to the db. If you're pulling a few dozen columns, this isn't going to matter.

In practical terms, consider disabling any indexes on the db table before running.
This'll greatly improve the COPY INTO that this does under the hood.
However if your indexes are there to prevent duplicate keys etc, don't do that
unless you're sure about the incoming parquet data.

## LOCAL

```bash
# activate the pre-commit hooks in repo's root dir:
pre-commit install

# set logging for further development
export RUST_LOG=info,s3_parquet_to_postgres=debug,work_lists=debug,s3_download=debug

# or is you want to just run the binary
export RUST_LOG=info

## local/reset-local.sh
# The reset-local.sh script requires permissions to read from our S3 bucket
# defined in the local/config.yml

# build debug binary and run the tests
./local/reset-local.sh # requires docker / docker-compose

# build for release
./local/reset-local.sh -r

```
