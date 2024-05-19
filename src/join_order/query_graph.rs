use std::collections::{HashMap, HashSet, VecDeque};
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator};

#[derive(Debug)]
pub struct Graph {
    pub nodes: HashSet<String>,
    pub edges: HashSet<Edge>,
    pub table_names: HashMap<String, String>
}

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct Edge {
    pub node1: String,
    pub node2: String,
    pub col1: String,
    pub col2: String,
}
pub fn build_query_graph(plan: &LogicalPlan) -> Graph {
    // map table alias name to real table name
    let table_names: HashMap<String, String> = get_table_names(plan);

    // expr
    let leaf_expr = leaf_binary_expr(plan);

    // all binary expressions, which have a relation on left and right
    let join_predicates: Vec<&BinaryExpr> = leaf_expr.iter()
        .filter(|e| matches!(*e.left, Expr::Column(_)) && matches!(*e.right, Expr::Column(_)))
        .copied()
        .collect();

    build_graph(table_names, join_predicates)
}

fn build_graph(table_names: HashMap<String, String>, join_predicates: Vec<&BinaryExpr>) -> Graph {
    let mut nodes: HashSet<String> = HashSet::new();
    let mut edges: HashSet<Edge> = HashSet::new();

    for predicate in join_predicates {
        match (&*predicate.left, &*predicate.right) {
            (Expr::Column(c1), Expr::Column(c2)) => {
                let t1_name = c1.relation.as_ref().unwrap().table().to_owned();
                let t2_name = c2.relation.as_ref().unwrap().table().to_owned();
                let c1_name = c1.name.to_string();
                let c2_name = c2.name.to_string();
                nodes.insert(t1_name.to_string());
                nodes.insert(t2_name.to_string());
                edges.insert(Edge { node1: t1_name, node2: t2_name, col1: c1_name, col2: c2_name });
            },
            (_, _) => panic!("error join predicate where both sides aren't cols")
        }
    }
    Graph { nodes, edges, table_names }
}

fn get_table_names(plan: &LogicalPlan) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let mut vec = vec![];
    let mut q = VecDeque::new();
    q.push_back(plan);

    while let Some(curr) = q.pop_front() {
        if let LogicalPlan::SubqueryAlias(sa) = curr {
            vec.push(sa);
        } else {
            curr.inputs().iter().for_each(|f| q.push_back(f));
        }
    }

    vec.iter().for_each(|f| {
        let scan = match &*f.input {LogicalPlan::TableScan(ts) => ts, _ => panic!("Not table scan")};
        let key = f.alias.table().to_owned();
        let value = scan.table_name.table().to_owned();
        map.insert(key, value);
    });

    map
}

// Get all leaf expressions (Stop at OR)
fn leaf_binary_expr(plan: &LogicalPlan) -> Vec<&BinaryExpr> {
    let filter_node = find_filter_node(plan);
    let node = match filter_node {
        Some(LogicalPlan::Filter(filter)) => filter,
        _ => panic!("No filter node found")
    };
    let binary_expr = match &node.predicate {
        Expr::BinaryExpr(e) => e,
        _ => panic!("root expr is not binary")
    };

    let mut vec = vec![];
    walk_expr(binary_expr, &mut vec);
    vec
}

fn walk_expr<'a>(binary_expr: &'a BinaryExpr, vec: &mut Vec<&'a BinaryExpr>) {
    let left = &*binary_expr.left;
    let right = &*binary_expr.right;

    if binary_expr.op == Operator::Or {
        vec.push(binary_expr);
        return;
    }

    let mut is_leaf = true;
    if let Expr::BinaryExpr(e) = left {
        walk_expr(e, vec);
        is_leaf = false;
    }

    if let Expr::BinaryExpr(e) = right {
        walk_expr(e, vec);
        is_leaf = false;
    }

    if is_leaf {
        vec.push(binary_expr);
    }
}

// DFS to find the first filter node
fn find_filter_node(node: &LogicalPlan) -> Option<&LogicalPlan> {
    if let LogicalPlan::Filter(_) = node {
        return Some(node);
    }

    node.inputs().iter().find_map(|e| find_filter_node(e))
}