/*
Scan all tables and return total num of rows, and distinct count for each col in the table

table1: {rows: 5, cols:[1,5,6,4]}

Takes about ~60sec using --release
 */

use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error;
use std::fs;
use std::path::Path;
use std::time::Instant;

use datafusion::arrow::array::as_string_array;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TableStats {
    pub rows: u64,
    pub cols: HashMap<String, u64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut stats = BTreeMap::new();

    let ctx = SessionContext::new();
    let table_dir = "C:\\Users\\G\\Desktop\\jobdata\\imdb";
    let mut tables = fs::read_dir(table_dir)?;
    let start = Instant::now();

    while let Some(Ok(entry)) = tables.next() {
        let path = entry.path();
        let filename = entry.file_name();
        let filename = Path::new(filename.to_str().expect("")).with_extension("");
        let filename = filename.to_str().unwrap().strip_prefix("job_").unwrap();
        ctx.register_parquet(
            filename,
            path.as_os_str().to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?;

        let df = ctx
            .sql(format!("SELECT * FROM {filename}").as_str())
            .await?;
        let records = df.collect().await?;

        let mut rows: u64 = 0;
        let mut sets: Vec<HashSet<String>> =
            vec![HashSet::new(); records.first().unwrap().num_columns()];
        let mut names = vec![];
        for record in records {
            rows += record.num_rows() as u64;
            names = record
                .schema()
                .fields
                .iter()
                .map(|x| x.name().to_string())
                .collect();
            for i in 0..record.num_columns() {
                let arr = cast(record.column(i), &DataType::Utf8).unwrap();
                let string_array = as_string_array(&arr);
                for v in string_array {
                    if v.is_none() || sets.get(i).unwrap().contains(v.unwrap()) {
                        continue;
                    }
                    sets.get_mut(i).unwrap().insert(v.unwrap().to_string());
                }
            }
        }

        let mut cols: HashMap<String, u64> = HashMap::new();
        sets.iter().enumerate().for_each(|e| {
            let name = names.get(e.0).unwrap().to_string();
            let val = e.1.len() as u64;
            cols.insert(name, val);
        });

        let stat = TableStats { rows, cols };

        stats.insert(filename.to_string(), stat);
    }

    let time = (Instant::now() - start).as_secs();
    println!("Collecting all stats took: {time} sec");

    let json = serde_json::to_string(&stats)?;
    fs::write(Path::new("./data/stats.json"), json).expect("unable to write json");
    Ok(())
}
