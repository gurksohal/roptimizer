use datafusion::arrow::array::as_string_array;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::Path;
use std::time::Instant;

use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};

// Takes ~14 mins
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut res = BTreeMap::new();
    let mut exec_time = BTreeMap::new();
    let mut config = SessionConfig::default();
    config.options_mut().execution.collect_statistics = true;

    let ctx = SessionContext::new_with_config(config);
    let table_dir = "C:\\Users\\G\\Desktop\\jobdata\\imdb";
    let mut tables = fs::read_dir(table_dir)?;
    let start = Instant::now();
    while let Some(Ok(entry)) = tables.next() {
        let path = entry.path();
        let filename = entry.file_name();
        let filename = Path::new(filename.to_str().expect(""));
        let filename = filename.with_extension("");
        let filename = filename.to_str().unwrap().strip_prefix("job_").unwrap();
        ctx.register_parquet(
            filename,
            path.as_os_str().to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?;
    }

    let mut total_time = (Instant::now() - start).as_millis();
    exec_time.insert(String::from("load_time"), total_time);

    let queries_path = "C:/Users/G/Desktop/jobdata/query";
    let mut files = fs::read_dir(queries_path)?;
    while let Some(Ok(entry)) = files.next() {
        let sql_path = entry.path();
        let filename = entry.file_name();
        let filename = filename.to_str().expect("error with name");
        let query = fs::read_to_string(sql_path)?;

        let local_start = Instant::now();
        let df = ctx.sql(query.as_str()).await?;
        let records = df.collect().await?;
        let time = (Instant::now() - local_start).as_millis();

        total_time += time;
        exec_time.insert(filename.to_string(), time);

        let mut r = vec![];
        for record in records {
            for i in 0..record.num_columns() {
                let arr = cast(record.column(i), &DataType::Utf8).unwrap();
                let string_array = as_string_array(&arr);
                for v in string_array {
                    if v.is_none() {
                        continue;
                    }
                    r.push(v.unwrap().to_owned());
                }
            }
        }

        res.insert(filename.to_string(), r);
    }

    exec_time.insert(String::from("total_time"), total_time);

    let exec_time_json = serde_json::to_string(&exec_time)?;
    let res_json = serde_json::to_string(&res)?;
    fs::write(Path::new("./data/df_exec_time.json"), exec_time_json).expect("unable to write json");
    fs::write(Path::new("./data/df_res.json"), res_json).expect("unable to write json");

    return Ok(());
}
