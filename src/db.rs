use anyhow::{bail, Result}; // don't need to return Result<T,E>
use log::{debug, error};
use parquet::record::{Field, Row};
use pin_utils::pin_mut;
use std::collections::HashMap;
use tokio_postgres::binary_copy::BinaryCopyInWriter; // let's us pg COPY from STDIN
use tokio_postgres::types::{ToSql, Type as PgType};
use tokio_postgres::Client; // used so data may be verified according to the pg data type

async fn db_col_to_type(client: &Client, table_name: &str) -> Result<HashMap<String, PgType>> {
    // The SQL query to get column names and type OIDs
    let query = format!(
        "SELECT a.attname as column_name, a.atttypid as type_oid
         FROM pg_attribute a
         JOIN pg_class c ON a.attrelid = c.oid
         WHERE c.relname = '{}' AND a.attnum > 0 AND NOT a.attisdropped",
        table_name
    );

    // Execute the query
    let rows = client.query(&query, &[]).await?;

    if rows.is_empty() {
        let msg = format!("Table {} does not exist in connected db.", table_name);
        error!("{}", msg);
        bail!("{}", msg);
    }

    // Create a HashMap to store the column names and their corresponding tokio_postgres::types::Type
    let mut db_col_to_type: HashMap<String, PgType> = HashMap::new();

    // Iterate through the rows
    for row in rows {
        let column_name: String = row.get("column_name");
        let type_oid: u32 = row.get("type_oid");

        // Convert OID to tokio_postgres::types::Type
        if let Some(data_type) = PgType::from_oid(type_oid) {
            db_col_to_type.insert(column_name, data_type);
        } else {
            let msg = format!("Unknown type OID: {}", type_oid);
            error!("{}", msg);
            bail!("{}", msg);
        }
    }

    Ok(db_col_to_type)
}

#[derive(Debug)]
pub struct Db {
    pub client: Client,
    pub db_cols: Vec<String>,
    pub db_col_types: Vec<PgType>,
    pub table_name: String,
}

impl Db {
    pub async fn connect(
        conn_str: &str,
        table_name: &str,
        parquet_fields: Vec<String>,
        parquet_to_db: Option<HashMap<String, Option<String>>>,
    ) -> Result<Self> {
        use tokio_postgres::{connect, NoTls};

        let (client, connection) = connect(conn_str, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                let msg = format!("db connection error: {}", e);
                error!("{}", msg);
                bail!("{}", msg);
            }
            Ok(())
        });

        // query db table to get types for each column
        let db_col_to_type: HashMap<String, PgType> = db_col_to_type(&client, table_name).await?;
        debug!("db_col_to_type: {:?}", db_col_to_type);

        // parquet_to_db: HashMap of parquet field name to the destination db col.
        // It's useful when the db col name differs from the parquet field name.
        // e.g. when parquet field name has characters not allowed in a db column name.
        // Default assumes db col has same name as parquet field.

        // For each desired parquet field use alias if defined, or else use parquet field name
        let db_cols: Vec<String> = match parquet_to_db {
            None => parquet_fields.clone(),
            Some(field_aliases) => parquet_fields
                .clone()
                .iter()
                .filter_map(|f| match field_aliases.contains_key(f) {
                    false => Some(String::from(f)),
                    true => match field_aliases.get(f.as_str()) {
                        None => None,
                        Some(alias) => {
                            if alias.is_none() {
                                Some(String::from(f))
                            } else {
                                alias.clone()
                            }
                        }
                    },
                })
                .collect::<Vec<String>>(),
        };

        // Check each user specified db col exists or error.
        // For each that exists, collect its postgres column data type.
        let mut db_col_types: Vec<PgType> = Vec::with_capacity(db_cols.len());
        for col in &db_cols {
            if let Some(col_type) = db_col_to_type.get(col.as_str()) {
                db_col_types.push(col_type.clone());
            } else {
                let msg = format!("Table {} does not have column {}", table_name, col);
                error!("{}", msg);
                bail!("{}", msg);
            }
        }

        Ok(Db {
            client,
            db_cols,
            db_col_types,
            table_name: table_name.to_string(),
        })
    }

    // Because of the need for pin_mut!, we have to create the following in the same scope:
    // * sink (filehandle) for copy in
    // * writer object
    // * pin_mut'ed writer (fixed mem address for its lifetime, but rust will still allow mutability)
    // You have to also call writer.as_mut().write() in the same scope
    // as any abstraction involves borrowing the writer, which is complicated by the Pin
    pub async fn write_rows(
        &self,
        iter: parquet::record::reader::RowIter<'_>,
        parquet_col_nums: &[usize],
    ) -> Result<u64> {
        let copy_in_sql = format!(
            "COPY {} ({}) FROM STDIN BINARY",
            self.table_name.clone(),
            self.db_cols.join(","),
        );
        let pg_types = &self.db_col_types;

        let sink = self.client.copy_in(copy_in_sql.as_str()).await?;
        let writer = BinaryCopyInWriter::new(sink, pg_types);
        pin_mut!(writer);

        for row_result in iter {
            let row: Row = row_result?;
            let all_fields = row.into_columns();
            let desired_fields: Vec<_> = parquet_col_nums
                .iter()
                .map(|index| all_fields[*index].1.clone())
                .collect();

            let mut row_data: Vec<_> = desired_fields
                .iter()
                .filter_map(|f| match f {
                    Field::Null => None,
                    Field::Bool(v) => Some(v as &(dyn ToSql + Sync)),
                    Field::Byte(v) => Some(v as &(dyn ToSql + Sync)),
                    Field::Short(v) => Some(v as &(dyn ToSql + Sync)),
                    Field::Int(v) => Some(v as &(dyn ToSql + Sync)),
                    Field::Long(v) => Some(v as &(dyn ToSql + Sync)),
                    Field::UInt(v) => Some(v as &(dyn ToSql + Sync)),
                    Field::Float(v) => Some(v as &(dyn ToSql + Sync)),
                    Field::Double(v) => Some(v as &(dyn ToSql + Sync)),
                    Field::Str(v) => Some(v as &(dyn ToSql + Sync)),
                    Field::Date(v) => Some(v as &(dyn ToSql + Sync)),
                    Field::TimestampMillis(v) => Some(v as &(dyn ToSql + Sync)),
                    Field::TimestampMicros(v) => Some(v as &(dyn ToSql + Sync)),
                    _ => {
                        error!("ToSQL not implemented for {}", f);
                        None
                    }
                })
                .collect();

            writer.as_mut().write(&row_data).await?;
            row_data.clear();
        }

        let num_rows_added = writer.finish().await?;

        Ok(num_rows_added)
    }
}

// These tests only make sense if there is a real running postgres.
// So probably better to just have integration tests for this.
#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{bail, Result};
    use assert_fs::{fixture::TempDir, prelude::*};
    use const_format::formatcp;
    use csv::WriterBuilder;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::any::type_name;
    use std::fs::File;
    use std::io::Cursor;
    use std::path::Path;
    use tokio_postgres::types::Type as PgType;
    use tokio_postgres::Client; // used so data may be verified according to the pg data type

    macro_rules! vec_stringify {
        ($($x:expr),*) => (vec![$($x.to_string()),*]);
    }

    static PARQUET_SRC_DIR: &str = formatcp!(
        "{}/{}",
        env!("CARGO_MANIFEST_DIR"),
        "tests/testdata/unit-tests/parquet_ops"
    );

    const GOOD_DB_CONN_STR: &str = "host=127.0.0.1 user=postgres password=postgres dbname=testing";

    #[allow(dead_code)]
    fn type_of<T>(_: T) -> &'static str {
        type_name::<T>()
    }

    async fn default_db_struct_for_cars_table(table_name: &str) -> Result<Db> {
        // TODO: create something like:
        let client = create_table_return_client(table_name.to_string()).await?;
        Ok(Db {
            client, // do connection as simply as possible.
            db_cols: vec_stringify!["model", "num_of_cyl", "miles_per_gallon", "gear"],
            db_col_types: vec![PgType::VARCHAR, PgType::INT4, PgType::FLOAT8, PgType::INT4],
            table_name: table_name.to_string(),
        })
        // TODO2: let it take params
    }

    async fn create_table_return_client(table_name: String) -> Result<Client> {
        let (client, connection) =
            tokio_postgres::connect(GOOD_DB_CONN_STR, tokio_postgres::NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                let msg = format!("db connection error: {}", e);
                bail!("{}", msg);
            }
            Ok(())
        });
        client
            .batch_execute(
                format!(
                    "
            DROP TABLE IF EXISTS {};
            CREATE TABLE {} (
                model VARCHAR (255),
                miles_per_gallon FLOAT8,
                num_of_cyl INT,
                disp FLOAT8,
                hp INT,
                drat FLOAT8,
                wt FLOAT8,
                qsec FLOAT8,
                vs SMALLINT,
                am SMALLINT,
                gear INT,
                carb SMALLINT
            );",
                    table_name.clone(),
                    table_name.clone()
                )
                .as_str(),
            )
            .await
            .unwrap();
        Ok(client)
    }

    async fn parquet_cars_reader() -> Result<(TempDir, SerializedFileReader<File>)> {
        let tmp_dir = TempDir::new().unwrap();
        tmp_dir
            .copy_from(PARQUET_SRC_DIR, &["cars.parquet"])
            .unwrap();
        let parquet_file = format!("{}/cars.parquet", tmp_dir.path().display());
        let f = File::open(Path::new(parquet_file.as_str())).unwrap();
        let reader = SerializedFileReader::new(f).unwrap();

        Ok((tmp_dir, reader))
    }

    async fn get_rows_as_csv_string(client: &Client, sql: &str) -> Result<String> {
        let rows = client.query(sql, &[]).await.unwrap();

        let cols = rows
            .first()
            .map(|row| {
                row.columns()
                    .iter()
                    .map(|col| col.name().to_string())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let mut wtr = WriterBuilder::new()
            .has_headers(true)
            .from_writer(Cursor::new(Vec::new()));

        wtr.write_record(&cols)?;
        for row in rows {
            let values: Vec<String> = (0..row.len())
                .map(|i| get_value_as_string(&row, i))
                .collect::<Result<Vec<String>, _>>()?;
            wtr.write_record(values)?;
        }

        let csv_string = String::from_utf8(wtr.into_inner()?.into_inner())?;

        Ok(csv_string)
    }

    // we use full type spec for row or compiler will think this is a parquet record row
    fn get_value_as_string(row: &tokio_postgres::Row, idx: usize) -> Result<String> {
        let column_type = row.columns()[idx].type_();
        let value_str = match *column_type {
            // Handle integers
            tokio_postgres::types::Type::INT2 => row
                .get::<_, Option<i16>>(idx)
                .map_or("".to_string(), |v| v.to_string()),
            tokio_postgres::types::Type::INT4 => row
                .get::<_, Option<i32>>(idx)
                .map_or("".to_string(), |v| v.to_string()),
            tokio_postgres::types::Type::INT8 => row
                .get::<_, Option<i64>>(idx)
                .map_or("".to_string(), |v| v.to_string()),

            // Handle floating point numbers
            tokio_postgres::types::Type::FLOAT4 => row
                .get::<_, Option<f32>>(idx)
                .map_or("".to_string(), |v| v.to_string()),
            tokio_postgres::types::Type::FLOAT8 => row
                .get::<_, Option<f64>>(idx)
                .map_or("".to_string(), |v| v.to_string()),

            // Handle string types
            tokio_postgres::types::Type::TEXT | tokio_postgres::types::Type::VARCHAR => row
                .get::<_, Option<String>>(idx)
                .map_or("".to_string(), |v| v),

            // Handle other types as needed
            tokio_postgres::types::Type::BOOL => row
                .get::<_, Option<bool>>(idx)
                .map_or("".to_string(), |v| v.to_string()),

            // Fallback for other types
            _ => "".to_string(),
        };

        Ok(value_str)
    }

    #[tokio::test]
    async fn test_connect_success() -> Result<()> {
        let table_name = "test_connect_success";
        let _ = create_table_return_client(table_name.to_string()).await;

        // Attempt to connect.
        let db = Db::connect(
            GOOD_DB_CONN_STR,
            table_name,
            vec_stringify!["model", "gear"],
            None,
        )
        .await;

        // Ensure the connection was successful by asserting it is Ok.
        assert!(
            db.is_ok(),
            "Connection should succeed with a valid connection string"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_connect_failure() -> Result<()> {
        let table_name = "test_connect_failure";
        let _ = create_table_return_client(table_name.to_string()).await; // create table
                                                                          // Set up an invalid connection string.
        let invalid_conn_str = "apples and pears";

        // Attempt to connect.
        let db = Db::connect(
            invalid_conn_str,
            table_name,
            vec_stringify!["model", "gear"],
            None,
        )
        .await;

        // Ensure the connection fails by asserting it is an Err.
        assert!(
            db.is_err(),
            "Connection should fail with an invalid connection string"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_connect_invalid_tablename() -> Result<()> {
        let table_name = "test_connect_invalid_tablename";
        let _ = create_table_return_client(table_name.to_string()).await; // create table

        // Attempt to connect.
        let db = Db::connect(
            GOOD_DB_CONN_STR,
            "not_a_real_table",
            vec_stringify!["model", "gear"],
            None,
        )
        .await;

        // Ensure the connection was successful by asserting it is Ok.
        assert!(db.is_err(), "Should fail as table not in connected db");

        Ok(())
    }
    #[tokio::test]
    async fn test_connect_col_name_not_exist() -> Result<()> {
        let table_name = "test_connect_col_name_not_exist";
        let _ = create_table_return_client(table_name.to_string()).await; // create table

        // Attempt to connect.
        let db = Db::connect(
            GOOD_DB_CONN_STR,
            table_name,
            vec_stringify!["model", "gear", "not_a_col"],
            None,
        )
        .await;

        // Ensure the connection was successful by asserting it is Ok.
        assert!(db.is_err(), "Should fail as col not in table");

        Ok(())
    }

    #[tokio::test]
    async fn test_connect_col_name_has_alias() -> Result<()> {
        let table_name = "test_connect_col_name_has_alias";
        let _ = create_table_return_client(table_name.to_string()).await; // create table

        let aliases: HashMap<String, Option<String>> = HashMap::from([
            ("i.model".to_string(), Some("model".to_string())),
            ("num_of_gears".to_string(), Some("gear".to_string())),
        ]);

        let db = Db::connect(
            GOOD_DB_CONN_STR,
            table_name,
            vec_stringify!["i.model", "num_of_gears"], // desired cols from parquet
            Some(aliases),                             // map of parquet col names to db table cols
        )
        .await;

        let res = db.as_ref().unwrap();

        assert!(db.is_ok(), "Should pass as aliases are all cols in db");

        let exp_db_col_names_for_desired_fields = vec_stringify!["model", "gear"];
        assert_eq!(res.db_cols, exp_db_col_names_for_desired_fields);

        let exp_db_types = [PgType::VARCHAR, PgType::INT4];
        assert_eq!(res.db_col_types, exp_db_types);

        Ok(())
    }

    #[tokio::test]
    async fn test_connect_db_col_name_has_none_alias_but_parquet_name_is_same() -> Result<()> {
        let table_name = "test_connect_db_col_name_has_none_alias_but_parquet_name_is_same";
        let _ = create_table_return_client(table_name.to_string()).await; // create table

        let aliases: HashMap<String, Option<String>> = HashMap::from([
            ("model".to_string(), None), // should use delivery_id
            ("num_of_gears".to_string(), Some("gear".to_string())), // will use alias even if same
        ]);

        let db = Db::connect(
            GOOD_DB_CONN_STR,
            table_name,
            vec_stringify!["model", "num_of_gears"], // desired cols from parquet
            Some(aliases),                           // map of parquet col names to db table cols
        )
        .await;

        let res = db.as_ref().unwrap();

        assert!(db.is_ok(), "Should pass as aliases are all cols in db");

        let exp_db_col_names_for_desired_fields = vec_stringify!["model", "gear"];
        assert_eq!(res.db_cols, exp_db_col_names_for_desired_fields);

        let exp_db_types = [PgType::VARCHAR, PgType::INT4];
        assert_eq!(res.db_col_types, exp_db_types);

        Ok(())
    }

    #[tokio::test]
    async fn test_connect_db_col_name_has_none_alias_but_parquet_name_is_not_same_as_db_col(
    ) -> Result<()> {
        let table_name =
            "test_connect_db_col_name_has_none_alias_but_parquet_name_is_not_same_as_db_col";
        let _ = create_table_return_client(table_name.to_string()).await; // create table

        let aliases: HashMap<String, Option<String>> = HashMap::from([
            ("model".to_string(), None),
            ("num_of_gears".to_string(), None), // the db col is actually gear, so should have had alias
        ]);

        let db = Db::connect(
            GOOD_DB_CONN_STR,
            table_name,
            vec_stringify!["model", "num_of_gears"], // desired cols from parquet
            Some(aliases),                           // map of parquet col names to db table cols
        )
        .await;

        assert!(
            db.is_err(),
            "Should fail as no db col alias given for parquet field d_id, and no col with same name as parquet field"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_connect_given_db_col_alias_does_not_exist() -> Result<()> {
        let table_name = "test_connect_given_db_col_alias_does_not_exist";
        let _ = create_table_return_client(table_name.to_string()).await; // create table

        let aliases: HashMap<String, Option<String>> = HashMap::from([
            ("model".to_string(), Some("not_a_col".to_string())), // not_a_col doesn't exist in db
            ("num_of_gears".to_string(), Some("gear".to_string())),
        ]);

        let db = Db::connect(
            GOOD_DB_CONN_STR,
            table_name,
            vec_stringify!["model", "num_of_gears"],
            Some(aliases),
        )
        .await;

        assert!(
            db.is_err(),
            "Should fail as alias 'not_a_col' is not a col in the db"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_write_rows_happy_path() -> Result<()> {
        let table_name = "test_write_rows_happy_path";
        let db = default_db_struct_for_cars_table(table_name).await.unwrap();
        let (tmp_dir, reader) = parquet_cars_reader().await.unwrap();
        let row_iter: parquet::record::reader::RowIter = reader.get_row_iter(None).unwrap();

        let col_nums = vec![0, 2, 1, 10];
        let num_rows_added = db.write_rows(row_iter, &col_nums).await?;
        tmp_dir.close().unwrap(); // can be deleted as read what we need

        assert_eq!(num_rows_added, 32);
        let sql = format!("SELECT * from {} ORDER by model DESC LIMIT 2", table_name);
        let exp_string = "\
            model,miles_per_gallon,num_of_cyl,disp,hp,drat,wt,qsec,vs,am,gear,carb\n\
            Volvo 142E,21.4,4,,,,,,,,4,\n\
            Valiant,18.1,6,,,,,,,,3,\n\
        ";
        let csv_string = get_rows_as_csv_string(&db.client, sql.as_str())
            .await
            .unwrap();
        assert_eq!(csv_string, exp_string.to_string());

        Ok(())
    }
}
