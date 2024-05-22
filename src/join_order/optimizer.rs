use std::ops::Deref;
use std::sync::Arc;

use datafusion::common::JoinConstraint;
use datafusion::common::JoinType::Inner;
use datafusion::logical_expr::{
    Aggregate, BinaryExpr, Expr, Filter, Join, Limit, LogicalPlan, Projection,
};
use datafusion::prelude::Column;

use crate::join_order::catalog::Catalog;
use crate::join_order::dp::{JoinNode, JoinOrderOpt, JoinTree};
use crate::join_order::query_graph::{Edge, leaf_binary_expr, QueryGraph};

pub fn optimize_df(plan: &LogicalPlan) -> LogicalPlan {
    if !verify_plan(plan) {
        panic!("Unexpected plan")
    };
    
    let catalog = Catalog::build().unwrap();
    let graph = QueryGraph::build_query_graph(plan);
    let optimized_join_tree = optimize(graph, catalog);
    println!("{}", optimized_join_tree);
    let new_df_plan = create_df_plan(optimized_join_tree, plan);
    copy_and_merge_plan(plan, &new_df_plan).unwrap()
}

fn optimize(query_graph: QueryGraph, catalog: Catalog) -> JoinTree {
    let mut opt = JoinOrderOpt::build(&query_graph, catalog);
    opt.join_order()
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
    let exprs: Vec<(Expr, Expr)> = join_tree.edges.iter().map(edge_to_expr).collect();
    let join = Join {
        left: Arc::new(left_plan.to_owned()),
        right: Arc::new(right_plan.to_owned()),
        on: exprs,
        filter: None,
        join_type: Inner,
        join_constraint: JoinConstraint::On,
        schema: Arc::new(
            left_plan
                .schema()
                .clone()
                .join(right_plan.schema())
                .expect("error in joining schemas"),
        ),
        null_equals_null: false,
    };

    LogicalPlan::Join(join)
}

fn edge_to_expr(edge: &Edge) -> (Expr, Expr) {
    let left_col = Expr::Column(Column::new(
        Some(edge.node1.to_string()),
        edge.col1.to_string(),
    ));
    let right_col = Expr::Column(Column::new(
        Some(edge.node2.to_string()),
        edge.col2.to_string(),
    ));

    (left_col, right_col)
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
    let v: Vec<Expr> = v
        .iter()
        .map(|e| Expr::BinaryExpr(e.to_owned().to_owned()))
        .collect();

    assert!(!v.is_empty());
    let first = v.first().unwrap().to_owned();
    let first = if let Expr::BinaryExpr(e) = first {
        e
    } else {
        panic!("not binary")
    };
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

#[cfg(test)]
mod test {
    use std::collections::{BTreeSet, HashMap, HashSet};
    use std::ops::Deref;

    use crate::join_order::catalog::{Catalog, TableStats};
    use crate::join_order::optimizer::optimize;
    use crate::join_order::query_graph::{Edge, QueryGraph, Relation};

    fn create_catalog() -> Catalog {
        let a = TableStats {
            rows: 100_000,
            cols: HashMap::from([("A".to_string(), 200)])
        };
        
        let b = TableStats {
            rows: 1_000_000,
            cols: HashMap::from([("A".to_string(), 1_000)])
        };
        
        let c = TableStats {
            rows: 1_000_000,
            cols: HashMap::from([("A".to_string(), 1_000_000)])
        };

        let stats = HashMap::from([
            ("A".to_string(), a),
            ("B".to_string(), b),
            ("C".to_string(), c),
        ]);
        Catalog { stats }
    }
    
    #[test]
    fn test_3_join() {
        let catalog = create_catalog();
        let edges = HashSet::from([
            Edge {
                node1: "A".to_string(),
                node2: "B".to_string(),
                col1: "A".to_string(),
                col2: "A".to_string()
            },
            Edge {
                node1: "B".to_string(),
                node2: "C".to_string(),
                col1: "A".to_string(),
                col2: "A".to_string()
            },
        ]);
        
        let graph = QueryGraph {
            edges,
            table_names: HashMap::new(),
            nodes: vec![Relation{name: "A".to_string(), id: 0}, Relation{name: "B".to_string(), id: 1}, Relation{name: "C".to_string(), id: 3}]
        };
        
        let join_tree = optimize(graph, catalog);
        let right_set = join_tree.right.as_ref().unwrap().deref();
        assert_eq!(right_set.to_set(), BTreeSet::from(["B".to_string(), "C".to_string()]));
    }
}
