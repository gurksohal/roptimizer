use std::ops::Deref;
use std::sync::Arc;

use datafusion::common::JoinConstraint;
use datafusion::common::JoinType::Inner;
use datafusion::logical_expr::{Aggregate, BinaryExpr, Expr, Filter, Join, Limit, LogicalPlan, Projection};

use crate::join_order::dp::{JoinNode, JoinOrderOpt, JoinTree};
use crate::join_order::query_graph::{leaf_binary_expr, QueryGraph};

mod join_order;
pub fn optimize(plan: &LogicalPlan) -> LogicalPlan {
    if !verify_plan(plan) {
        panic!("Unexpected plan")
    };
    
    let graph = QueryGraph::build_query_graph(plan);
    let mut opt = JoinOrderOpt::build(&graph);
    let optimized_join_tree = opt.join_order();

    let new_df_plan = create_df_plan(optimized_join_tree, plan);
    copy_and_merge_plan(plan, &new_df_plan).unwrap()
}

fn create_df_plan(join_tree: JoinTree, df_plan: &LogicalPlan) -> LogicalPlan {
    if join_tree.size == 1 {
        let join_node = join_tree.left.expect("left is empty");
        assert!(join_tree.right.is_none(), "right should be none");
        let table_name = if let JoinNode::Single(s) = join_node.deref() {
            s
        } else {
            panic!("left should be of single join node type");
        };

        return get_logical_scan_node(df_plan, table_name)
            .unwrap()
            .to_owned();
    }

    let left_tree = JoinTree::from_join_node(&join_tree.left.expect("left was empty"));
    let right_tree = JoinTree::from_join_node(&join_tree.right.expect("right was empty"));

    let left_plan = create_df_plan(left_tree.to_owned(), df_plan);
    let right_plan = create_df_plan(right_tree.to_owned(), df_plan);

    let join = Join {
        left: Arc::new(left_plan.to_owned()),
        right: Arc::new(right_plan.to_owned()),
        on: get_join_expr(df_plan, &left_tree, &right_tree),
        filter: None,
        join_type: Inner,
        join_constraint: JoinConstraint::On,
        schema: Arc::new(
            left_plan
                .schema()
                .join(right_plan.schema())
                .expect("error in joining schemas"),
        ),
        null_equals_null: false,
    };

    LogicalPlan::Join(join)
}

fn get_join_expr(
    df_plan: &LogicalPlan,
    left_tree: &JoinTree,
    right_tree: &JoinTree,
) -> Vec<(Expr, Expr)> {
    let left_set = left_tree.to_set();
    let right_set = right_tree.to_set();
    let join_predicates = get_join_predicates(df_plan);
    let mut res = vec![];

    for join_pred in join_predicates {
        match (&*join_pred.left, &*join_pred.right) {
            (Expr::Column(c1), Expr::Column(c2)) => {
                let table1 = c1.relation.as_ref().unwrap().table().to_owned();
                let table2 = c2.relation.as_ref().unwrap().table().to_owned();

                if left_set.contains(&table1) && right_set.contains(&table2)
                    || left_set.contains(&table2) && right_set.contains(&table1)
                {
                    res.push((*join_pred.left.to_owned(), *join_pred.right.to_owned()));
                }
            }
            _ => {
                panic!("not a col expr")
            }
        }
    }

    res
}

fn get_join_predicates(plan: &LogicalPlan) -> Vec<&BinaryExpr> {
    leaf_binary_expr(plan)
        .iter()
        .filter(|e| matches!(*e.left, Expr::Column(_)) && matches!(*e.right, Expr::Column(_)))
        .copied()
        .collect()
}

fn get_logical_scan_node<'a>(
    df_plan: &'a LogicalPlan,
    table_name: &str,
) -> Option<&'a LogicalPlan> {
    match df_plan {
        LogicalPlan::TableScan(table) => {
            if table.table_name.table() == table_name {
                Some(df_plan)
            } else {
                None
            }
        }
        LogicalPlan::SubqueryAlias(table) => {
            if table.alias.table() == table_name {
                Some(df_plan)
            } else {
                None
            }
        }
        _ => {
            let plans: Vec<&LogicalPlan> = df_plan
                .inputs()
                .iter()
                .filter_map(|plan| get_logical_scan_node(plan, table_name))
                .collect();
            if plans.is_empty() {
                return None;
            }
            assert_eq!(plans.len(), 1, "size of plans should be one");
            return Some(plans.first().unwrap());
        }
    }
}

fn copy_and_merge_plan(plan: &LogicalPlan, new_plan: &LogicalPlan) -> Option<LogicalPlan> {
    match plan {
        LogicalPlan::Projection(p) => {
            let children: Vec<LogicalPlan> = plan
                .inputs()
                .iter()
                .filter_map(|node| copy_and_merge_plan(node, new_plan))
                .collect();
            let input = Arc::new(children.first().unwrap().to_owned());
            Some(LogicalPlan::Projection(
                Projection::try_new(p.expr.to_owned(), input).unwrap(),
            ))
        }
        LogicalPlan::Aggregate(p) => {
            let children: Vec<LogicalPlan> = plan
                .inputs()
                .iter()
                .filter_map(|node| copy_and_merge_plan(node, new_plan))
                .collect();
            let input = Arc::new(children.first().unwrap().to_owned());
            Some(LogicalPlan::Aggregate(
                Aggregate::try_new(input, p.group_expr.to_owned(), p.aggr_expr.to_owned()).unwrap(),
            ))
        }
        LogicalPlan::Limit(p) => {
            let children: Vec<LogicalPlan> = plan
                .inputs()
                .iter()
                .filter_map(|node| copy_and_merge_plan(node, new_plan))
                .collect();
            let input = Arc::new(children.first().unwrap().to_owned());
            Some(LogicalPlan::Limit(Limit {
                input,
                ..p.to_owned()
            }))
        }
        LogicalPlan::Filter(_) => {
            let pred = remove_join_predicates(plan);
            let f = Filter::try_new(pred, Arc::new(new_plan.to_owned())).unwrap();
            Some(LogicalPlan::Filter(f))
        }
        _ => None,
    }
}

fn remove_join_predicates(plan: &LogicalPlan) -> Expr {
    let exprs = leaf_binary_expr(plan);
    let join_pred = get_join_predicates(plan);
    let mut v = vec![];
    for e in exprs {
        if join_pred.contains(&e) {
            continue;
        }

        v.push(e);
    }
    let v: Vec<Expr> = v.iter().map(|e| Expr::BinaryExpr(e.to_owned().to_owned())).collect();

    assert!(!v.is_empty());
    let first = v.first().unwrap().to_owned();
    let first = if let Expr::BinaryExpr(e) = first { e } else {panic!("not binary")};
    let mut root = Expr::BinaryExpr(first);

    for i in 1..v.len() {
        let t = v.get(i).unwrap().to_owned();
        root = root.and(t);
    }

    root
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
        _ => return false,
    };

    plan.inputs().iter().all(|p| verify_plan(p))
}