use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::ops::Sub;

use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator};

#[derive(Debug)]
pub struct Graph {
    pub nodes: HashSet<String>,
    pub edges: HashSet<Edge>,
    pub table_names: HashMap<String, String>
}

#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub struct Edge {
    pub node1: String,
    pub node2: String,
    pub col1: String,
    pub col2: String,
}

impl Graph {
    pub fn table_name(&self, name: &str) -> String {
        if self.table_names.contains_key(name) {
            return self.table_names.get(name).unwrap().to_string();
        }
        
        name.to_string()
    }
    
    pub fn csg_cmp_pairs(&self) -> Vec<(BTreeSet<String>, BTreeSet<String>)> {
        let order = assign_ordering(self);
        let subsets: Vec<BTreeSet<String>> = self.enumerate_csg(&order);
        let mut res = vec![];
        for subset in &subsets {
            let t = self.enumerate_cmp(subset, &order);
            for temp in t {
                res.push((subset.clone(), temp));
            }
        }
        
        res
    }

    fn enumerate_csg(&self, order: &HashMap<u32, &String>) -> Vec<BTreeSet<String>> {
        let mut subsets: Vec<BTreeSet<String>> = vec![];
        for i in (0..order.len()).rev() {
            let index = i as u32;
            let start = BTreeSet::from([order.get(&index).unwrap().to_string()]);
            subsets.push(start.clone());
            let x = BTreeSet::new();
            for e in self.enumerate_csg_rec(start, x, &order) {
                subsets.push(e);
            }
        }
        
        subsets
    }
    
    fn enumerate_cmp(&self, subset: &BTreeSet<String>, order: &HashMap<u32, &String>) -> Vec<BTreeSet<String>> {
        // find min in subset
        let mut res = vec![];
        let mut min = order.len();
        for i in 0..order.len() {
            if subset.contains(order.get(&(i as u32)).unwrap().as_str()) {
                min = min.min(i);
            }
        }
        
        let mut exclude = BTreeSet::new();
        for i in 0..=min {
            exclude.insert(order.get(&(i as u32)).unwrap().to_string());
        }
        
        exclude = exclude.union(subset).cloned().collect();
        let n = self.neighbourhood(subset).sub(&exclude);
        
        for i in 0..order.len() {
            let node = order.get(&(i as u32)).unwrap().to_string();
            if n.contains(&node) {
                let start = BTreeSet::from([node]);
                res.push(start.clone());
                let ex = exclude.union(&n).cloned().collect();
                for e in self.enumerate_csg_rec(start, ex, &order) {
                    res.push(e);
                }
            }
        }
        
        res
    }

    fn enumerate_csg_rec(&self, set: BTreeSet<String>, exclusion: BTreeSet<String>, order: &HashMap<u32, &String>) -> Vec<BTreeSet<String>> {
        let mut subsets: Vec<BTreeSet<String>> = vec![];
        let n: BTreeSet<String> = self.neighbourhood(&set).sub(&exclusion);
        
        let n_subsets: Vec<Vec<String>> = Self::sorted_subsets(&n, order);
        for s_prime in &n_subsets {
            let s_prime_set: BTreeSet<String> = BTreeSet::from_iter(s_prime.iter().cloned());
            let final_set: BTreeSet<String> = s_prime_set.union(&set).cloned().collect();
            subsets.push(final_set);
        }
        
        for s_prime in &n_subsets {
            let s_prime_set: BTreeSet<String> = BTreeSet::from_iter(s_prime.iter().cloned());
            let next_set: BTreeSet<String> = s_prime_set.union(&set).cloned().collect();
            let exclude: BTreeSet<String> = exclusion.union(&n).cloned().collect();
            self.enumerate_csg_rec(next_set, exclude, order);
        }
        subsets
    }

    fn neighbourhood(&self, set: &BTreeSet<String>) -> BTreeSet<String> {
        let mut res = BTreeSet::new();
        for node in set {
            res.extend(self.neighbourhood_single_node(node))
        }

        res = res.difference(set).cloned().collect();
        res
    }

    fn neighbourhood_single_node(&self, node: &String) -> BTreeSet<String> {
        let mut res = BTreeSet::new();
        for edge in &self.edges {
            if &edge.node1 == node || &edge.node2 == node {
                let mut add = edge.node1.clone();
                if &add == node {
                    add.clone_from(&edge.node2);
                }
                res.insert(add);
            }
        }
        
        println!("n for {}: {:?}", node, res);
        res
    }

    fn sorted_subsets(set: &BTreeSet<String>, order: &HashMap<u32, &String>) -> Vec<Vec<String>> {
        let mut res: Vec<Vec<String>> = vec![];
        let mut sorted_set = vec![];
        for i in 0..order.len() {
            let key = order.get(&(i as u32)).unwrap();
            if set.contains(key.as_str()) {
                sorted_set.push(key.as_str())
            }
        }
        
        let mut curr = vec![];
        Self::sorted_subsets_rec(&sorted_set, &mut res, &mut curr, 0);
        res
    }
    
    fn sorted_subsets_rec(set: &Vec<&str>, res: &mut Vec<Vec<String>>, curr: &mut Vec<String>, index: usize) {
        res.push(curr.clone());
        
        for i in index..set.len() {
            curr.push(set.get(i).unwrap().to_string());
            Self::sorted_subsets_rec(set, res, curr, i+1);
            curr.remove(curr.len() - 1);
        }
    }
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

fn assign_ordering(graph: &Graph) -> HashMap<u32, &String> {
    let mut map = HashMap::new();
    let root = graph
        .nodes
        .iter()
        .next()
        .expect("unable to assign a root in query graph");
    let mut seen = HashSet::new();
    let mut q = VecDeque::new();

    q.push_back(root);
    seen.insert(root);
    let mut i: u32 = 0;
    while !q.is_empty() {
        let curr = q.pop_front().unwrap();
        map.insert(i, curr);
        i += 1;
        for edge in get_edges(graph, curr) {
            let mut child = &edge.node1;
            if child == curr {
                child = &edge.node2;
            }

            if seen.contains(child) {
                continue;
            }

            q.push_back(child);
            seen.insert(child);
        }
    }

    map
}

fn get_edges<'a>(graph: &'a Graph, node: &str) -> Vec<&'a Edge> {
    let mut res = vec![];
    for edge in &graph.edges {
        if edge.node1 == node || edge.node2 == node {
            res.push(edge);
        }
    }

    res
}