use anyhow::{anyhow, Result};
use parquet::basic::ConvertedType;
use parquet::record::Field;
use parquet::schema::types::Type;
use tokio_postgres::types::{ToSql, Type as PgType};

pub fn build<'a>(
    pq_type_data: &'a Vec<(parquet::basic::Type, ConvertedType)>,
    db_col_types: &'a Vec<PgType>,
) -> Result<Vec<&'a dyn Fn(Field) -> Box<dyn ToSql + Sync>>> {
    Ok(Vec::new())
}
