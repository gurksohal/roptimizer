use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;

use datafusion::common::JoinConstraint;
use datafusion::common::JoinType::Inner;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{Aggregate, BinaryExpr, build_join_schema, Expr, Filter, Join, Limit, LogicalPlan, LogicalPlanBuilder, Projection, SubqueryAlias, TableScan};
use datafusion::prelude::Column;

use crate::join_order::catalog::Catalog;
use crate::join_order::dp::{JoinNode, JoinOrderOpt, JoinTree};
use crate::join_order::query_graph::{Edge, leaf_expr, QueryGraph};

pub fn optimize_df(plan: &LogicalPlan) -> LogicalPlan {
    if !verify_plan(plan) {
        panic!("Unexpected plan")
    };
    
    let catalog = Catalog::build().unwrap();
    let graph = QueryGraph::build_query_graph(plan);
    let optimized_join_tree = optimize(graph.clone(), catalog);
    let filter_preds = get_remaining_filters(plan);
    let mut new_df_plan = join_tree_to_df(optimized_join_tree, plan, &graph, &filter_preds);
    new_df_plan = copy_and_merge_plan(plan, &new_df_plan).unwrap();
    remove_filter_node(&new_df_plan)
}

fn optimize(query_graph: QueryGraph, catalog: Catalog) -> JoinTree {
    let mut opt = JoinOrderOpt::build(&query_graph, catalog);
    opt.join_order()
}

// Create df plan from join tree and push non join predicate filters down
fn join_tree_to_df(join_tree: JoinTree, df_plan: &LogicalPlan, graph: &QueryGraph, filter_preds: &Vec<Expr>) -> LogicalPlan {
    if join_tree.size == 1 {
        assert!(join_tree.right.is_none(), "tree size is 1, and right is none");
        let join_node = join_tree.left.unwrap();
        let t_name = if let JoinNode::Single(s) = join_node.deref() {
            s
        } else {
            panic!("left should be of single join node type");
        };

        let full_name = table_name(&graph.table_names, t_name);
        let to_apply: Vec<Expr> = filter_preds.iter().filter_map(|f| {
            let mut name = get_table_from_expr(f).unwrap();

            if &name == t_name || name == full_name {
                name = table_name(&graph.table_names, &name);
                let t = f.to_owned().transform_down(|e| {
                    match e {
                        Expr::Column(c) => {
                            let new_c = Column::new(Some(name.to_string()), c.name.to_string());
                            Ok(Transformed {
                                data: Expr::Column(new_c),
                                tnr: TreeNodeRecursion::Continue,
                                transformed: false
                            })
                        }
                        _ => {
                            Ok(Transformed {
                                data: e,
                                tnr: TreeNodeRecursion::Continue,
                                transformed: false
                            })
                        }
                    }
                }).unwrap();

                return Some(t.data);
            }

            None
        }).collect();

        let to_apply_expr = create_expr(to_apply.clone());
        let scan_node = get_logical_scan_node(df_plan, &full_name)
            .unwrap();
        
        let scan_node = LogicalPlan::TableScan(TableScan::try_new(scan_node.table_name.clone(), scan_node.source.clone(), scan_node.projection.clone(), scan_node.filters.clone(), scan_node.fetch).unwrap());

        if &full_name == t_name {
            if !to_apply.is_empty() {
                return LogicalPlan::Filter(Filter::try_new(to_apply_expr.unwrap(), Arc::from(scan_node)).unwrap());
            }
            return scan_node;
        }

        if !to_apply.is_empty() {
            let f = LogicalPlan::Filter(Filter::try_new(to_apply_expr.unwrap(), Arc::from(scan_node)).unwrap());
            return LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(Arc::from(f), t_name).unwrap());
        }

        return LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(Arc::from(scan_node), t_name).unwrap());
    }

    let left_tree = JoinTree::from_join_node(&join_tree.left.expect("left was empty"));
    let right_tree = JoinTree::from_join_node(&join_tree.right.expect("right was empty"));
    let exprs: Vec<(Expr, Expr)> = join_tree.edges.iter().map(edge_to_expr).map(|e| reorder_expr_tuple(e, &left_tree, &right_tree)).collect();

    let left_plan = join_tree_to_df(left_tree.to_owned(), df_plan, graph, filter_preds);
    let right_plan = join_tree_to_df(right_tree.to_owned(), df_plan, graph, filter_preds);
    
    let schema = build_join_schema(left_plan.schema(), right_plan.schema(), &Inner).unwrap();
    let join = Join {
        left: Arc::new(left_plan.to_owned()),
        right: Arc::new(right_plan.to_owned()),
        on: exprs,
        filter: None,
        join_type: Inner,
        join_constraint: JoinConstraint::On,
        schema: Arc::new(schema),
        null_equals_null: false,
    };

    LogicalPlan::Join(join)
}

fn create_expr(to_apply: Vec<Expr>) -> Option<Expr> {
    if to_apply.is_empty() {
        return None
    }
    
    let first = to_apply.first()?;
    
    let mut set: HashSet<Expr> = HashSet::new();
    set.insert(first.to_owned());
    let mut root = first.to_owned();

    for i in 0..to_apply.len() {
        let t = to_apply.get(i).unwrap().to_owned();
        if set.contains(&t) {
            continue;
        }
        set.insert(t.to_owned());
        root = root.and(t);
    }

    Some(root)
}

fn get_table_from_expr(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(c) => {
            Some(c.to_owned().relation.unwrap().to_string())
        }
        Expr::BinaryExpr(be) => {
            let left = &be.left;
            let right = &be.right;

            let left_ans = get_table_from_expr(left);
            if let Some(v) = left_ans {
                return Some(v)
            }
            let right_ans = get_table_from_expr(right);
            if let Some(v) = right_ans {
                return Some(v)
            }
            None
        }
        Expr::Like(like) => {
            get_table_from_expr(&like.expr)
        }
        Expr::IsNull(null) => {
            get_table_from_expr(null)
        }
        Expr::Between(between) => {
            get_table_from_expr(&between.expr)
        }
        Expr::IsNotNull(nn) => {
            get_table_from_expr(nn)
        }
        Expr::InList(list) => {
            get_table_from_expr(&list.expr)
        }
        node => panic!("unexpected: {node:#?}")
    }
}

fn reorder_expr_tuple(exprs: (Expr, Expr), left_tree: &JoinTree, right_tree: &JoinTree) -> (Expr, Expr) {
    let copy = exprs.clone();
    let left_set = left_tree.to_set();
    let right_set = right_tree.to_set();

    let left_table = if let Expr::Column(c) = exprs.0 { c.relation.unwrap() } else { panic!("not column") };
    let right_table = if let Expr::Column(c) = exprs.1 { c.relation.unwrap() } else { panic!("not column") };

    if left_set.contains(left_table.table()) && right_set.contains(right_table.table()) {
        return copy;
    }

    (copy.clone().1, copy.0)
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

fn get_join_predicates(plan: &LogicalPlan) -> Vec<BinaryExpr> {
    leaf_expr(plan)
        .iter()
        .filter_map(|e| {
            match e {
                Expr::BinaryExpr(f) => Some(f),
                _ => None
            }
        })
        .filter(|e| matches!(*e.left, Expr::Column(_)) && matches!(*e.right, Expr::Column(_)))
        .cloned()
        .collect()
}

fn get_logical_scan_node<'a>(
    df_plan: &'a LogicalPlan,
    table_name: &str,
) -> Option<&'a TableScan> {
    match df_plan {
        LogicalPlan::TableScan(table) => {
            if table.table_name.table() == table_name {
                Some(table)
            } else {
                None
            }
        }
        _ => {
            let plans: Vec<&TableScan> = df_plan
                .inputs()
                .iter()
                .filter_map(|plan| get_logical_scan_node(plan, table_name))
                .collect();
            if plans.is_empty() {
                return None;
            }
            let set: HashSet<&TableScan> = HashSet::from_iter(plans);
            assert_eq!(set.len(), 1, "{}", set.len());
            return Some(set.iter().next().unwrap());
        }
    }
}

fn remove_filter_node(plan: &LogicalPlan) -> LogicalPlan {
    let new_plan = plan.clone().transform_down(|node| {
        let inputs = node.inputs();
        let is_child_filter = inputs.iter().any(|child| matches!(child, LogicalPlan::Filter(_)));
        if is_child_filter {
            assert_eq!(inputs.len(), 1);
            if let LogicalPlan::Filter(f) = inputs.first().unwrap() {
                let filter_child = f.input.to_owned();
                match node {
                    LogicalPlan::Aggregate(a) => {
                        let new_aggr = Aggregate::try_new(filter_child, a.group_expr, a.aggr_expr);
                        return Ok(Transformed {
                            data: LogicalPlan::Aggregate(new_aggr.unwrap()),
                            transformed: true,
                            tnr: TreeNodeRecursion::Stop
                        });
                    }
                    f => { panic!("got: {f:#?}") }
                }
            } else { unreachable!(); };
        }

        Ok(Transformed {
            data: node,
            tnr: TreeNodeRecursion::Continue,
            transformed: false
        })
    }).expect("");

    new_plan.data
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

fn get_remaining_filters(plan: &LogicalPlan) -> Vec<Expr> {
    let exprs = leaf_expr(plan);
    let join_pred = get_join_predicates(plan);
    let mut v = vec![];
    for e in exprs {
        if let Expr::BinaryExpr(eb) = &e {
            if join_pred.contains(eb) {
                continue;
            } else {
                v.push(e);
            }
        } else {
            v.push(e);
        };
    }

    v
}

fn remove_join_predicates(plan: &LogicalPlan) -> Expr {
    let v: Vec<Expr> = get_remaining_filters(plan);

    assert!(!v.is_empty());
    create_expr(v).unwrap()
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

fn table_name(table_names: &HashMap<String, String>, name: &str) -> String {
    if table_names.contains_key(name) {
        return table_names.get(name).unwrap().to_string();
    }

    name.to_string()
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
        assert_eq!(right_set.to_set(), BTreeSet::from(["A".to_string(), "B".to_string()]));
    }
}
