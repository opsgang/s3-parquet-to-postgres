use anyhow::{anyhow, Result};
use chrono::NaiveDate;
use parquet::basic::{ConvertedType, Type as PqType};
use parquet::record::Field;
use parquet::schema::types::Type;
use tokio_postgres::types::{to_sql_checked, IsNull, ToSql, Type as PgType};

static NAIVE_EPOCH: NaiveDate = NaiveDate::from_ymd(1970, 1, 1);

fn parquet_date_to_naive_date(parquet_date: i32) -> NaiveDate {
    // The Unix epoch date is 1970-01-01
    // Add the number of days to the epoch date
    NAIVE_EPOCH + chrono::Duration::days(parquet_date as i64)
}

#[derive(Debug)]
struct NullVal;

impl ToSql for NullVal {
    fn to_sql(
        &self,
        _ty: &tokio_postgres::types::Type,
        _buf: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        Ok(IsNull::Yes) // Represents NULL in PostgreSQL
    }

    fn accepts(_ty: &tokio_postgres::types::Type) -> bool {
        true // Accept any type
    }

    to_sql_checked!();
}

pub fn build<'a>(
    pq_type_data: &'a [(PqType, ConvertedType)],
    db_col_types: &'a [PgType],
    converters: &'a mut Vec<&'a dyn Fn(Field) -> Box<dyn ToSql + Sync>>,
) -> Result<()> {
    for (i, (physical, converted)) in pq_type_data.iter().enumerate() {
        let db_col_type = db_col_types[i].clone();
        println!(
            "{}: P:{:?}, C:{:?}, pg:{:?}",
            i, physical, converted, db_col_types[i]
        );
        let converter_fn: &dyn Fn(Field) -> Box<dyn ToSql + Sync> = match physical {
            // TODO: add arms for physical -> converted -> db_col_type
            PqType::INT32 => {
                println!("Found a parquet physical INT32");
                &|_f: Field| -> Box<dyn ToSql + Sync> {
                    Box::new(&NullVal) as Box<dyn ToSql + Sync>
                }
            }
            _ => &|_f: Field| -> Box<dyn ToSql + Sync> {
                Box::new(&NullVal) as Box<dyn ToSql + Sync>
            },
        };
        converters.push(converter_fn);
    }
    Ok(())
}
