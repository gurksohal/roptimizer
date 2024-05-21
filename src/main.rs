use std::fs;
use std::path::Path;

use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{DataFrame, ParquetReadOptions, SessionConfig, SessionContext};

use roptimizer::optimize;

mod join_order;
#[tokio::main]
async fn main() {
    let mut config = SessionConfig::default();
    config.options_mut().optimizer.max_passes = 0;

    let ctx = SessionContext::new_with_config(config);
    load_job_data(&ctx).await;
    let plan = get_df_plan(&ctx, "1a").await;
    let plan = optimize(&plan);
    let df = DataFrame::new(ctx.state(), plan);
    //df.collect().await.expect("TODO: panic message");
    println!("{}", df.into_unoptimized_plan().display_graphviz());
    //run_and_test_all().await;
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
