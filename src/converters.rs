use anyhow::{anyhow, bail, Result};
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

/*
https://arrow.apache.org/rust/parquet/basic/enum.Type.html
https://arrow.apache.org/rust/parquet/basic/enum.ConvertedType.html
https://github.com/apache/arrow-rs/blob/master/parquet/src/record/api.rs # tests

Also see https://github.com/adriangb/pgpq?tab=readme-ov-file#data-type-support

PG Types: https://github.com/sfackler/rust-postgres/blob/master/postgres-types/src/lib.rs#L461-L512

PHYSICAL    CONVERTED   PQField::   Allowed PG types
BOOL        BOOL        Bool        *BOOL, VARCHAR|TEXT|CHAR, SMALLINT (1 or 0)
BOOL        NONE        Bool        *BOOL, VARCHAR|TEXT|CHAR, SMALLINT (1 or 0)
INT32       INT_8       Byte        *INT2|SMALLINT(i16), INT|INT4(i32), BIGINT|INT8(i64)
INT32       INT_16      Short       *INT2|SMALLINT(i16), INT|INT4(i32), BIGINT|INT8(i64)
INT32       DATE        Date        *DATE, INT|INT4(i32), BIGINT|INT8(i64), VARCHAR|TEXT|CHAR(>10) assumes YYYY-mm-dd
INT32       INT_32      Int
INT32       NONE        Int         *INT|INT4(i32), BIGINT|INT8(i64)
BYTE_ARRAY  UTF8        Str         *VARCHAR|TEXT|CHAR(>0)
*/

fn p_int32<'a>(
    converted: &'a ConvertedType,
    db_col_type: &PgType,
) -> &'a dyn Fn(&Field) -> Box<dyn ToSql + Sync> {
    println!("Found a parquet physical INT32");
    match *converted {
        ConvertedType::DATE => p_int32_c_date(converted, db_col_type), // parquet date
        ConvertedType::INT_16 => p_int32_c_int_16(converted, db_col_type), // parquet smallint/short
        ConvertedType::NONE => {
            println!("NO CONVERTED TYPE = must be INT32 compatible");
            &|_f: &Field| -> Box<dyn ToSql + Sync> { Box::new(NullVal) as Box<dyn ToSql + Sync> }
        }
        _ => {
            println!("UNKNOWN CONVERTED TYPE {}", converted);
            &|_f: &Field| -> Box<dyn ToSql + Sync> { Box::new(NullVal) as Box<dyn ToSql + Sync> }
        }
    }
}

fn p_int32_c_int_16<'a>(
    _converted: &'a ConvertedType,
    db_col_type: &PgType,
) -> &'a dyn Fn(&Field) -> Box<dyn ToSql + Sync> {
    println!("Found a converted INT16 (Short)");
    match *db_col_type {
        PgType::INT2 => &|f: &Field| -> Box<dyn ToSql + Sync> {
            match f {
                Field::Short(v) => Box::new((*v as i16)) as Box<dyn ToSql + Sync>,
                _ => Box::new(NullVal) as Box<dyn ToSql + Sync>,
            }
        },
        PgType::INT4 => &|f: &Field| -> Box<dyn ToSql + Sync> {
            match f {
                Field::Short(v) => Box::new(*v as i32) as Box<dyn ToSql + Sync>,
                _ => Box::new(NullVal) as Box<dyn ToSql + Sync>,
            }
        },
        PgType::INT8 => &|f: &Field| -> Box<dyn ToSql + Sync> {
            match f {
                Field::Short(v) => Box::new(*v as i64) as Box<dyn ToSql + Sync>,
                _ => Box::new(NullVal) as Box<dyn ToSql + Sync>,
            }
        },
        _ => {
            todo!()
        }
    }
}

fn p_int32_c_date<'a>(
    _converted: &'a ConvertedType,
    db_col_type: &PgType,
) -> &'a dyn Fn(&Field) -> Box<dyn ToSql + Sync> {
    println!("Found a converted DATE");
    match *db_col_type {
        PgType::DATE => &|f: &Field| -> Box<dyn ToSql + Sync> {
            match f {
                Field::Date(v) => Box::new(parquet_date_to_naive_date(*v)) as Box<dyn ToSql + Sync>,
                _ => Box::new(NullVal) as Box<dyn ToSql + Sync>,
            }
        },
        PgType::VARCHAR | PgType::TEXT | PgType::CHAR => &|f: &Field| -> Box<dyn ToSql + Sync> {
            match f {
                Field::Date(v) => {
                    let date_fmt = "%Y-%m-%d";
                    let chrono_date = parquet_date_to_naive_date(*v);
                    Box::new(chrono_date.format(date_fmt).to_string()) as Box<dyn ToSql + Sync>
                }
                _ => Box::new(NullVal) as Box<dyn ToSql + Sync>,
            }
        },
        _ => {
            todo!()
        }
    }
}

pub fn build<'a>(
    pq_type_data: &'a [(PqType, ConvertedType)],
    db_col_types: &'a [PgType],
) -> Result<Vec<&'a dyn Fn(&Field) -> Box<dyn ToSql + Sync>>> {
    let mut converters: Vec<&dyn Fn(&Field) -> Box<dyn ToSql + Sync>> =
        Vec::with_capacity(db_col_types.len());
    for (i, (physical, converted)) in pq_type_data.iter().enumerate() {
        let db_col_type = db_col_types[i].clone();
        println!(
            "{}: P:{:?}, C:{:?}, pg:{:?}",
            i, physical, converted, db_col_types[i]
        );
        let converter_fn: &dyn Fn(&Field) -> Box<dyn ToSql + Sync> = match physical {
            // TODO: add arms for physical -> converted -> db_col_type
            PqType::INT32 => p_int32(&converted, &db_col_type),
            _ => {
                // Just return v as Box, for all those mappings between parquet->rust->pg
                // that I don't need to implement right now.
                println!("UNKNOWN PHYSICAL TYPE {}", physical);
                &|f: &Field| -> Box<dyn ToSql + Sync> {
                    match *f {
                        Field::Null => Box::new(NullVal) as Box<dyn ToSql + Sync>, // Use NullMarker for NULL values
                        Field::Bool(v) => Box::new(v) as Box<dyn ToSql + Sync>,
                        Field::Byte(v) => Box::new(v) as Box<dyn ToSql + Sync>,
                        Field::Short(v) => Box::new(v) as Box<dyn ToSql + Sync>,
                        Field::Int(v) => Box::new(v) as Box<dyn ToSql + Sync>,
                        Field::Long(v) => Box::new(v) as Box<dyn ToSql + Sync>,
                        Field::UInt(v) => Box::new(v) as Box<dyn ToSql + Sync>,
                        Field::Float(v) => Box::new(v) as Box<dyn ToSql + Sync>,
                        Field::Double(v) => Box::new(v) as Box<dyn ToSql + Sync>,
                        Field::Str(ref v) => Box::new(v.clone()) as Box<dyn ToSql + Sync>,
                        _ => {
                            println!("NOT IMPLEMENTED - will return Null");
                            Box::new(NullVal) as Box<dyn ToSql + Sync>
                        }
                    }
                }
            }
        };
        converters.push(converter_fn);
    }
    Ok(converters)
}
