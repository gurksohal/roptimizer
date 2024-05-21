use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::Path;
use std::time::Instant;

use datafusion::arrow::array::as_string_array;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{DataFrame, ParquetReadOptions, SessionConfig, SessionContext};

use roptimizer::{optimize};

mod join_order;
#[tokio::main]
async fn main() {
    let mut config = SessionConfig::default();
    config.options_mut().optimizer.max_passes = 3;
    
    //let ctx = SessionContext::new_with_config(config);
    //load_job_data(&ctx).await;
    //let plan = get_df_plan(&ctx, "15a").await;
    //let plan = optimize(&plan);
    //let df = DataFrame::new(ctx.state(), plan);
    //df.collect().await.expect("TODO: panic message");
    //println!("{}", df.into_optimized_plan().unwrap().display_graphviz());
    run_and_test_all().await;
}

async fn run_and_test_all() {
    let mut res = BTreeMap::new();
    let mut exec_time = BTreeMap::new();
    let mut config = SessionConfig::default();
    config.options_mut().optimizer.max_passes = 3;
    config.options_mut().execution.collect_statistics = true;

    let ctx = SessionContext::new_with_config(config);
    load_job_data(&ctx).await;

    let queries_path = "C:/Users/G/Desktop/jobdata/query";
    let mut files = fs::read_dir(queries_path).unwrap();
    let mut total_time = 0;

    while let Some(Ok(entry)) = files.next() {
        let filename = entry.file_name();
        let filename = filename.to_str().unwrap().strip_suffix(".sql").unwrap();
        let plan = get_df_plan(&ctx, filename).await;
        let local_start = Instant::now();
        let plan = optimize(&plan);
        println!("join time: {}", (Instant::now()-local_start).as_millis());
        let local_start = Instant::now();
        let df = DataFrame::new(ctx.state(), plan);
        let records = df.collect().await.unwrap();
        let time = (Instant::now() - local_start).as_millis();

        println!("{filename}: took {time}ms");
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
    let exec_time_json = serde_json::to_string(&exec_time).unwrap();
    let res_json = serde_json::to_string(&res).unwrap();

    fs::write(Path::new("./data/df_opt_exec_time.json"), exec_time_json).expect("unable to write json");

    let old_res = fs::read_to_string("./data/df_res.json").unwrap();
    assert_eq!(old_res, res_json, "Result from the queries changed");
}

async fn load_job_data(ctx: &SessionContext) {
    let table_dir = "C:\\Users\\G\\Desktop\\jobdata\\imdb";
    let mut tables = fs::read_dir(table_dir).unwrap();
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
        .await
        .unwrap();
    }
}

// Get plan for a given JOB query
async fn get_df_plan(ctx: &SessionContext, query: &str) -> LogicalPlan {
    let queries_path = format!("C:/Users/G/Desktop/jobdata/query/{}.sql", query);
    let query_str = fs::read_to_string(queries_path).unwrap();
    let df = ctx.sql(query_str.as_str()).await.unwrap();
    df.logical_plan().to_owned()
}