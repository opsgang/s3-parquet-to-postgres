use anyhow::{anyhow, Result};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::Type;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

pub struct Parquet {
    pub filename: String,
    pub desired_fields: Vec<String>,
}

impl Parquet {
    pub fn new(filename: String, desired_fields: Vec<String>) -> Result<Self> {
        // TODO - error if file does not exist, error if desired_fields empty
        Ok(Parquet {
            filename,
            desired_fields,
        })
    }

    pub fn file_reader(&self) -> Result<SerializedFileReader<File>> {
        let file = File::open(Path::new(&self.filename))?;
        let reader = SerializedFileReader::new(file)?;
        Ok(reader)
    }

    pub fn get_desired_cols(&mut self, reader: &SerializedFileReader<File>) -> Result<Vec<usize>> {
        let schema: &Type = reader.metadata().file_metadata().schema();

        let mut desired_cols: Vec<usize> = Vec::with_capacity(self.desired_fields.len());

        let field_map: &mut HashMap<String, usize> = &mut HashMap::new();
        Self::map_fields_to_cols(field_map, schema, 0, 0);
        for field in self.desired_fields.clone() {
            let col_num = field_map
                .get(&field)
                .ok_or_else(|| anyhow!("Field '{}' not found in field_map", field))?;

            desired_cols.push(*col_num);
        }

        Ok(desired_cols)
    }

    fn map_fields_to_cols(
        field_map: &mut HashMap<String, usize>,
        schema: &Type,
        _depth: usize,
        col_num: usize,
    ) {
        let name = schema.name();

        match schema {
            Type::PrimitiveType { .. } => field_map.insert(String::from(name), col_num),
            Type::GroupType { .. } => None,
        };

        if schema.is_group() {
            for (column_num, column) in schema.get_fields().iter().enumerate() {
                Self::map_fields_to_cols(field_map, column, _depth + 1, column_num);
            }
        }
    }
}

// SEE PARQUET SPECS / META AT BOTTOM OF THIS FILE
#[cfg(test)]
mod tests {
    use super::*;
    use assert_fs::{fixture::TempDir, prelude::*};
    use const_format::formatcp;
    use parquet::file::reader::SerializedFileReader;
    use std::any::type_name;
    use std::env;
    use std::fs::File;

    use crate::test_setup::tests::LOCALSTACK_PARQUET_DIR_IRIS;

    static TESTDATA_DIR: &str = formatcp!(
        "{}/{}",
        env!("CARGO_MANIFEST_DIR"),
        "tests/testdata/unit-tests/parquet_ops"
    );

    macro_rules! vec_stringify {
        ($($x:expr),*) => (vec![$($x.to_string()),*]);
    }

    fn type_of<T>(_: T) -> &'static str {
        type_name::<T>()
    }

    fn test_reader_iris_file(
        desired_fields: Vec<String>,
    ) -> Result<(TempDir, Parquet, SerializedFileReader<File>)> {
        let tmp_dir = TempDir::new().unwrap();
        tmp_dir
            .copy_from(LOCALSTACK_PARQUET_DIR_IRIS, &["iris.parquet"])
            .unwrap();
        let parquet_file = format!("{}/iris.parquet", tmp_dir.path().display());

        let pq = Parquet {
            filename: parquet_file,
            desired_fields,
        };
        let reader = pq.file_reader().unwrap();

        Ok((tmp_dir, pq, reader))
    }

    #[test]
    fn test_new_returns_result() {
        /*
            We don't really need to unit-test New as it does nothing but return the type
            except it does wrap the user passed fields in a Result, in case we add some
            validation later. Don't want to go around adding ? and unwrap to callers later.
        */

        let result = Parquet::new("/some/file".to_string(), vec_stringify!["field_A"]);
        assert!(result.is_ok(), "Should return a valid Result");
        let pq = result.unwrap();
        assert_eq!("s3_parquet_to_postgres::parquet_ops::Parquet", type_of(pq));
    }

    #[test]
    fn test_file_reader_no_such_file() {
        let pq = Parquet {
            filename: "/no/such/file".to_string(),
            desired_fields: vec_stringify!["field_A"],
        };
        let result = pq.file_reader();
        assert!(result.is_err(), "Should fail as file does not exist");
    }

    #[test]
    fn test_file_reader_valid_parquet_file() {
        let tmp_dir = TempDir::new().unwrap();
        tmp_dir
            .copy_from(LOCALSTACK_PARQUET_DIR_IRIS, &["iris.parquet"])
            .unwrap();

        let parquet_file = format!("{}/iris.parquet", tmp_dir.path().display());

        let pq = Parquet {
            filename: parquet_file.clone(),
            desired_fields: vec_stringify!["field_A"],
        };

        {
            let result = pq.file_reader();
            assert!(result.is_ok(), "Should return fine as file exists");
        }
        tmp_dir.close().unwrap(); // can close file now
    }

    #[test]
    fn test_file_reader_empty_file_not_parquet() {
        let tmp_dir = TempDir::new().unwrap();
        tmp_dir
            .copy_from(TESTDATA_DIR, &["empty_file.not_parquet"])
            .unwrap();

        let parquet_file = format!("{}/empty_file.not_parquet", tmp_dir.path().display());

        let pq = Parquet {
            filename: parquet_file.clone(),
            desired_fields: vec_stringify!["field_A"],
        };

        {
            let result = pq.file_reader();
            assert!(result.is_err(), "Should fail as not a parquet file");
        }
        tmp_dir.close().unwrap(); // can close file now
    }

    #[test]
    fn test_file_reader_text_file_not_parquet() {
        let tmp_dir = TempDir::new().unwrap();
        tmp_dir
            .copy_from(TESTDATA_DIR, &["just_contains.txt"])
            .unwrap();

        let parquet_file = format!("{}/just_contains.txt", tmp_dir.path().display());

        let pq = Parquet {
            filename: parquet_file.clone(),
            desired_fields: vec_stringify!["field_A"],
        };

        {
            let result = pq.file_reader();
            assert!(result.is_err(), "Should fail as not a parquet file");
        }
        tmp_dir.close().unwrap(); // can close file now
    }

    #[test]
    fn test_get_desired_cols_valid_fields() {
        let desired_fields = vec_stringify!["variety", "sepal.length"];
        let (tmp_dir, mut pq, reader) = test_reader_iris_file(desired_fields).unwrap();

        // test method
        let result = pq.get_desired_cols(&reader);
        tmp_dir.close().unwrap(); // can be deleted as read what we need

        assert!(result.is_ok(), "should find variety field in iris.parquet");
        assert_eq!(result.unwrap(), vec![4, 0]); // can see col order in PARQUET META at end of file
    }

    #[test]
    fn test_get_desired_cols_missing_field() {
        let desired_fields = vec_stringify!["variety", "sepal.length", "does.not.exist"];
        let (tmp_dir, mut pq, reader) = test_reader_iris_file(desired_fields).unwrap();

        // test method
        let result = pq.get_desired_cols(&reader);
        tmp_dir.close().unwrap(); // can be deleted as read what we need

        assert!(
            result.is_err(),
            "field 'does.not.exist' should cause and error"
        );
    }

    #[test]
    fn test_get_desired_cols_same_field_duplicated_is_fine() {
        let desired_fields = vec_stringify!["variety", "sepal.length", "variety"];
        let (tmp_dir, mut pq, reader) = test_reader_iris_file(desired_fields).unwrap();

        // test method
        let result = pq.get_desired_cols(&reader);
        tmp_dir.close().unwrap(); // can be deleted as read what we need

        assert!(result.is_ok(), "should find variety field in iris.parquet");
        assert_eq!(result.unwrap(), vec![4, 0, 4]); // can see col order in PARQUET META at end of file
    }
}
/*
PARQUET META:

IRIS
=====
parquet meta local/localstack/bucket_data/iris-parquet/iris.parquet

File path:  local/localstack/bucket_data/iris-parquet/iris.parquet
Created by: DuckDB
Properties: (none)
Schema:
message duckdb_schema {
  optional double sepal.length;
  optional double sepal.width;
  optional double petal.length;
  optional double petal.width;
  optional binary variety (STRING);
}


Row group 0:  count: 150  12.57 B records  start: 4  total(compressed): 1.841 kB total(uncompressed):97.250 kB
--------------------------------------------------------------------------------
              type      encodings count     avg size   nulls   min / max
sepal.length  DOUBLE    S RR_     150       3.09 B     0       "4.3" / "7.9"
sepal.width   DOUBLE    S RR_     150       3.03 B     0       "2.0" / "4.4"
petal.length  DOUBLE    S RR_     150       3.11 B     0       "1.0" / "6.9"
petal.width   DOUBLE    S RR_     150       2.77 B     0       "0.1" / "2.5"
variety       BINARY    S RRR_    150       0.57 B     0       "Setosa" / "Virginica"

CARS
====
parquet meta local/localstack/bucket_data/cars-parquet/cars.parquet

File path:  local/localstack/bucket_data/cars-parquet/cars.parquet
Created by: DuckDB
Properties: (none)
Schema:
message duckdb_schema {
  optional binary model (STRING);
  optional double mpg;
  optional int32 cyl (INTEGER(32,true));
  optional double disp;
  optional int32 hp (INTEGER(32,true));
  optional double drat;
  optional double wt;
  optional double qsec;
  optional int32 vs (INTEGER(32,true));
  optional int32 am (INTEGER(32,true));
  optional int32 gear (INTEGER(32,true));
  optional int32 carb (INTEGER(32,true));
}


Row group 0:  count: 32  60.75 B records  start: 4  total(compressed): 1.898 kB total(uncompressed):163.192 kB
--------------------------------------------------------------------------------
       type      encodings count     avg size   nulls   min / max
model  BINARY    S RR_     32        14.19 B    0       "AMC Javelin" / "Volvo 142E"
mpg    DOUBLE    S RR_     32        5.63 B     0       "10.4" / "33.9"
cyl    INT32     S RR_     32        2.44 B     0       "4" / "8"
disp   DOUBLE    S RR_     32        5.81 B     0       "71.1" / "472.0"
hp     INT32     S RR_     32        4.16 B     0       "52" / "335"
drat   DOUBLE    S RR_     32        6.00 B     0       "2.76" / "4.93"
wt     DOUBLE    S RR_     32        6.84 B     0       "1.513" / "5.424"
qsec   DOUBLE    S RR_     32        6.69 B     0       "14.5" / "22.9"
vs     INT32     S RR_     32        2.25 B     0       "0" / "1"
am     INT32     S RR_     32        1.50 B     0       "0" / "1"
gear   INT32     S RR_     32        2.25 B     0       "3" / "5"
carb   INT32     S RR_     32        3.00 B     0       "1" / "8"
*/
