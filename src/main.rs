use std::fs;
use std::path::Path;

use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{ParquetReadOptions, SessionContext};

use crate::join_order::query_graph::build_query_graph;

mod catalog;
mod join_order;

#[tokio::main]
async fn main() {
    let ctx = SessionContext::new();
    load_job_data(&ctx).await;
    let plan = get_df_plan(&ctx, "1a").await;
    if !verify_plan(&plan) { panic!("Unexpected plan") };

    build_query_graph(&plan);
}

async fn load_job_data(ctx: &SessionContext) {
    let table_dir = "C:\\Users\\G\\Desktop\\jobdata\\imdb";
    let mut tables = fs::read_dir(table_dir).unwrap();
    while let Some(Ok(entry)) =  tables.next() {
        let path = entry.path();
        let filename = entry.file_name();
        let filename = Path::new(filename.to_str().expect("")).with_extension("");
        let filename = filename.to_str().unwrap().strip_prefix("job_").unwrap();
        ctx.register_parquet(filename, path.as_os_str().to_str().unwrap(), ParquetReadOptions::default()).await.unwrap();
    }
}

// Get plan for a given JOB query
async fn get_df_plan(ctx: &SessionContext, query: &str) -> LogicalPlan {
    let queries_path = format!("C:/Users/G/Desktop/jobdata/query/{}.sql", query);
    let query_str = fs::read_to_string(queries_path).unwrap();
    let df = ctx.sql(query_str.as_str()).await.unwrap();
    df.logical_plan().to_owned()
}

// Make sure plan only has expected/supported nodes
fn verify_plan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(_) => {}
        LogicalPlan::Filter(_) => {}
        LogicalPlan::Aggregate(_) => {}
        LogicalPlan::CrossJoin(_) => {}
        LogicalPlan::TableScan(_) => {}
        LogicalPlan::SubqueryAlias(_) => {}
        LogicalPlan::Limit(_) => {}
        _ => return false
    };
    
    plan.inputs().iter().all(|p| verify_plan(p))
}