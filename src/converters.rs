use anyhow::Result;
use chrono::NaiveDate;
use log::{debug, error};
use parquet::basic::{ConvertedType, Type as PqType};
use parquet::record::Field;
use tokio_postgres::types::{to_sql_checked, IsNull, ToSql, Type as PgType};

const NAIVE_EPOCH: NaiveDate = match NaiveDate::from_ymd_opt(1970, 1, 1) {
    Some(naive_epoch) => naive_epoch,
    None => panic!("Invalid date for epoch"),
};

type ConverterFn = dyn Fn(&Field) -> Box<dyn ToSql + Sync>;
type Converters<'a> = Vec<&'a ConverterFn>;

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
BOOL        BOOL        Bool        *BOOL, VARCHAR|TEXT|BPCHAR, SMALLINT (1 or 0)
BOOL        NONE        Bool        *BOOL, VARCHAR|TEXT|BPCHAR, SMALLINT (1 or 0)
INT32       INT_8       Byte        *INT2|SMALLINT(i16), INT|INT4(i32), BIGINT|INT8(i64)
INT32       INT_16      Short       *INT2|SMALLINT(i16), INT|INT4(i32), BIGINT|INT8(i64)
INT32       DATE        Date        *DATE, INT|INT4(i32), BIGINT|INT8(i64), VARCHAR|TEXT|BPCHAR assumes YYYY-mm-dd
INT32       INT_32      Int
INT32       NONE        Int         *INT|INT4(i32), BIGINT|INT8(i64)
BYTE_ARRAY  UTF8        Str         *VARCHAR|TEXT|CHAR(>0)
*/

// INT32: https://github.com/apache/arrow-rs/blob/master/parquet/src/record/api.rs#L1025-L1060
fn p_int32<'a>(converted: &'a ConvertedType, db_col_type: &PgType) -> &'a ConverterFn {
    println!("Found a parquet physical INT32");
    match *converted {
        ConvertedType::DATE => field_is_date(converted, db_col_type), // parquet date
        ConvertedType::INT_16 => field_is_short(converted, db_col_type), // parquet smallint/short
        ConvertedType::NONE | ConvertedType::INT_32 => field_is_int(converted, db_col_type),

        _ => {
            println!("UNKNOWN CONVERTED TYPE {}", converted);
            &|_f: &Field| -> Box<dyn ToSql + Sync> { Box::new(NullVal) as Box<dyn ToSql + Sync> }
        }
    }
}

// BYTE_ARRAY: https://github.com/apache/arrow-rs/blob/master/parquet/src/record/api.rs#L725-L737
fn p_byte_array<'a>(converted: &'a ConvertedType, db_col_type: &PgType) -> &'a ConverterFn {
    println!("Found a physical BYTE_ARRAY");
    match *converted {
        ConvertedType::UTF8 | ConvertedType::ENUM | ConvertedType::JSON => {
            field_is_str(converted, db_col_type)
        }
        ConvertedType::NONE | ConvertedType::BSON => field_is_bytes(converted, db_col_type),
        ConvertedType::DECIMAL => field_is_decimal(converted, db_col_type),
        _ => {
            println!("UNHANDLED CONVERTED TYPE {}, will use NULL", converted);
            &|_f: &Field| -> Box<dyn ToSql + Sync> { Box::new(NullVal) as Box<dyn ToSql + Sync> }
        }
    }
}

fn field_is_bytes<'a>(_converted: &'a ConvertedType, db_col_type: &PgType) -> &'a ConverterFn {
    println!("Found an unconverted BYTE_ARRAY or converted BSON (BYTE_ARRAY)");
    match *db_col_type {
        _ => {
            todo!()
        }
    }
}

fn field_is_decimal<'a>(_converted: &'a ConvertedType, db_col_type: &PgType) -> &'a ConverterFn {
    println!("Found a converted DECIMAL");
    match *db_col_type {
        PgType::FLOAT4 => &|f: &Field| -> Box<dyn ToSql + Sync> {
            match f {
                Field::Decimal(v) => Box::new(NullVal) as Box<dyn ToSql + Sync>,
                _ => Box::new(NullVal) as Box<dyn ToSql + Sync>,
            }
        },
        _ => {
            todo!()
        }
    }
}

// TODO: we don't actually know that postgres crate will do the conversions from each of these types
// to an acceptable type for writing to the db ...
// Need to create test with every single type with incoming string data to check
fn pgtype_accepts_str(pgtype: &PgType) -> bool {
    matches!(
        *pgtype,
        PgType::BPCHAR
            | PgType::CHAR
            | PgType::TEXT
            | PgType::DATE
            | PgType::TIMESTAMP
            | PgType::TIMESTAMPTZ
            | PgType::VARCHAR
            | PgType::UNKNOWN
            | PgType::INET
            | PgType::CIDR
    )
}

fn field_is_str<'a>(_converted: &'a ConvertedType, db_col_type: &PgType) -> &'a ConverterFn {
    println!("Found a UTF8 (Str)");
    match db_col_type {
        _ if pgtype_accepts_str(db_col_type) => &|f: &Field| -> Box<dyn ToSql + Sync> {
            match f {
                Field::Str(ref v) => Box::new(v.clone()) as Box<dyn ToSql + Sync>,
                _ => Box::new(NullVal) as Box<dyn ToSql + Sync>,
            }
        },
        _ => {
            todo!()
        }
    }
}

fn field_is_int<'a>(_converted: &'a ConvertedType, db_col_type: &PgType) -> &'a ConverterFn {
    println!("Found an INT32 (Short)");
    match *db_col_type {
        PgType::INT4 => &|f: &Field| -> Box<dyn ToSql + Sync> {
            match f {
                Field::Int(v) => Box::new(*v) as Box<dyn ToSql + Sync>,
                _ => Box::new(NullVal) as Box<dyn ToSql + Sync>,
            }
        },
        PgType::INT8 => &|f: &Field| -> Box<dyn ToSql + Sync> {
            match f {
                Field::Int(v) => Box::new(*v as i64) as Box<dyn ToSql + Sync>,
                _ => Box::new(NullVal) as Box<dyn ToSql + Sync>,
            }
        },
        _ => {
            todo!()
        }
    }
}

fn field_is_short<'a>(_converted: &'a ConvertedType, db_col_type: &PgType) -> &'a ConverterFn {
    println!("Found a converted INT16 (Int)");
    match *db_col_type {
        PgType::INT2 => &|f: &Field| -> Box<dyn ToSql + Sync> {
            match f {
                Field::Short(v) => Box::new(*v) as Box<dyn ToSql + Sync>,
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

fn field_is_date<'a>(_converted: &'a ConvertedType, db_col_type: &PgType) -> &'a ConverterFn {
    println!("Found a converted DATE");
    match *db_col_type {
        PgType::DATE => &|f: &Field| -> Box<dyn ToSql + Sync> {
            match f {
                Field::Date(v) => Box::new(parquet_date_to_naive_date(*v)) as Box<dyn ToSql + Sync>,
                _ => Box::new(NullVal) as Box<dyn ToSql + Sync>,
            }
        },
        PgType::VARCHAR | PgType::TEXT | PgType::BPCHAR => &|f: &Field| -> Box<dyn ToSql + Sync> {
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
            error!("NOT YET IMPLEMENTED for PG type {:?}", db_col_type);
            todo!()
        }
    }
}

pub fn build<'a>(
    pq_type_data: &'a [(PqType, ConvertedType)],
    db_col_types: &'a [PgType],
) -> Result<Vec<&'a ConverterFn>> {
    let mut converters: Converters = Vec::with_capacity(db_col_types.len());

    for (i, (physical, converted)) in pq_type_data.iter().enumerate() {
        let db_col_type = db_col_types[i].clone();

        println!(
            "{}: P:{:?}, C:{:?}, pg:{:?}",
            i, physical, converted, db_col_types[i]
        );

        let converter_fn: &ConverterFn = match physical {
            // TODO: add arms for physical -> converted -> db_col_type
            PqType::INT32 => p_int32(converted, &db_col_type),
            PqType::BYTE_ARRAY => p_byte_array(converted, &db_col_type),
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
