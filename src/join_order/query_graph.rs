use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::ops::Sub;

use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator};

#[derive(Debug, Clone)]
pub struct QueryGraph {
    pub nodes: Vec<Relation>,
    pub edges: HashSet<Edge>,
    pub table_names: HashMap<String, String>,
}

#[derive(Debug)]
struct Graph {
    pub nodes: BTreeSet<String>,
    pub edges: HashSet<Edge>,
    pub table_names: HashMap<String, String>,
}

#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub struct Relation {
    pub name: String,
    pub id: u32,
}

#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub struct Edge {
    pub node1: String,
    pub node2: String,
    pub col1: String,
    pub col2: String,
}

impl Display for Edge {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{} = {}.{}",
            self.node1, self.col1, self.node2, self.col2
        )
    }
}

impl QueryGraph {
    pub fn build_query_graph(plan: &LogicalPlan) -> QueryGraph {
        // map table alias name to real table name
        let table_names: HashMap<String, String> = get_table_names(plan);

        // expr
        let leaf_expr = leaf_expr(plan);

        // all binary expressions, which have a relation on left and right
        let join_predicates: Vec<&BinaryExpr> = leaf_expr
            .iter()
            .filter_map(|e| match e {
                Expr::BinaryExpr(f) => Some(f),
                _ => None,
            })
            .filter(|e| matches!(*e.left, Expr::Column(_)) && matches!(*e.right, Expr::Column(_)))
            .collect();

        let g = build_graph(table_names, join_predicates);
        QueryGraph::from_graph(&g)
    }

    fn from_graph(graph: &Graph) -> QueryGraph {
        QueryGraph {
            nodes: assign_ordering(graph),
            edges: graph.edges.clone(),
            table_names: graph.table_names.clone(),
        }
    }

    pub fn csg_cmp_pairs(&self) -> Vec<(BTreeSet<String>, BTreeSet<String>)> {
        let subsets: Vec<HashSet<&Relation>> = self.enumerate_csg();
        let mut res = vec![];
        for subset in &subsets {
            let string_subset = Self::rel_set_to_str_set(subset);
            let cmp = self.enumerate_cmp(subset);
            for temp in cmp {
                let string_temp = Self::rel_set_to_str_set(&temp);
                res.push((string_subset.clone(), string_temp));
            }
        }

        res
    }

    fn rel_set_to_str_set(set: &HashSet<&Relation>) -> BTreeSet<String> {
        let mut res = BTreeSet::new();
        for element in set {
            res.insert(element.name.to_string());
        }

        res
    }

    fn enumerate_csg(&self) -> Vec<HashSet<&Relation>> {
        let mut res: Vec<HashSet<&Relation>> = vec![];
        for i in (0..self.nodes.len()).rev() {
            let curr = HashSet::from([self.nodes.get(i).unwrap()]);
            res.push(curr.clone());
            let x = self.nodes_with_eq_smaller_id(i as u32);
            res.extend(self.enumerate_csg_rec(curr, x));
        }

        res
    }

    fn enumerate_csg_rec<'a>(
        &'a self,
        set: HashSet<&'a Relation>,
        exclude: HashSet<&'a Relation>,
    ) -> Vec<HashSet<&'a Relation>> {
        let n = self.neighbourhood(set.clone()).sub(&exclude);
        let subsets = subsets(n.clone());
        let mut res: Vec<HashSet<&Relation>> = vec![];
        for subset in subsets.clone() {
            if subset.is_empty() {
                continue;
            }
            let hash_subset: HashSet<&Relation> = HashSet::from_iter(subset);
            let emit_val: HashSet<&Relation> = set.union(&hash_subset).copied().collect();
            res.push(emit_val);
        }

        for subset in subsets {
            if subset.is_empty() {
                continue;
            }
            let hash_subset: HashSet<&Relation> = HashSet::from_iter(subset);
            let new_set: HashSet<&Relation> = set.union(&hash_subset).copied().collect();
            let new_exclude: HashSet<&Relation> = exclude.union(&n).copied().collect();
            res.extend(self.enumerate_csg_rec(new_set, new_exclude));
        }

        res
    }

    fn enumerate_cmp<'a>(&'a self, set: &HashSet<&'a Relation>) -> Vec<HashSet<&'a Relation>> {
        let mut res: Vec<HashSet<&Relation>> = vec![];
        let min = set.iter().min_by_key(|r| r.id).expect("empty set");
        let exclude: HashSet<&Relation> = self
            .nodes_with_eq_smaller_id(min.id)
            .union(set)
            .copied()
            .collect();
        let n = self.neighbourhood(set.clone()).sub(&exclude);

        let mut n_vec = Vec::from_iter(&n);
        n_vec.sort_by_key(|rel| rel.id);
        let n_vec: Vec<&Relation> = n_vec.iter().rev().copied().copied().collect();

        for relation in n_vec {
            let curr = HashSet::from([relation]);
            res.push(curr.clone());
            let new_exclude = exclude.union(&n).copied().collect();
            res.extend(self.enumerate_csg_rec(curr, new_exclude));
        }

        res
    }

    fn neighbourhood<'a>(&'a self, set: HashSet<&'a Relation>) -> HashSet<&'a Relation> {
        let mut res: HashSet<&Relation> = HashSet::new();
        for relation in &set {
            res.extend(self.neighbourhood_single_relation(relation));
        }
        res.sub(&set)
    }

    fn neighbourhood_single_relation(&self, relation: &Relation) -> HashSet<&Relation> {
        let mut res: HashSet<&Relation> = HashSet::new();
        for edge in &self.edges {
            if edge.node1 == relation.name || edge.node2 == relation.name {
                let r1 = self
                    .nodes
                    .iter()
                    .find(|rel| rel.name == edge.node1)
                    .expect("cant find relation in nodes");

                let r2 = self
                    .nodes
                    .iter()
                    .find(|rel| rel.name == edge.node2)
                    .expect("cant find relation in nodes");

                res.insert(r1);
                res.insert(r2);
            }
        }

        res.remove(relation);
        res
    }

    fn nodes_with_eq_smaller_id(&self, i: u32) -> HashSet<&Relation> {
        self.nodes.iter().filter(|node| node.id <= i).collect()
    }
}

fn build_graph(table_names: HashMap<String, String>, join_predicates: Vec<&BinaryExpr>) -> Graph {
    let mut nodes: BTreeSet<String> = BTreeSet::new();
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

                edges.insert(Edge {
                    node1: t1_name,
                    node2: t2_name,
                    col1: c1_name,
                    col2: c2_name,
                });
            }

            (_, _) => panic!("error join predicate where both sides aren't cols"),
        }
    }

    Graph {
        nodes,
        edges,
        table_names,
    }
}

pub fn leaf_expr(plan: &LogicalPlan) -> Vec<Expr> {
    let mut res = HashSet::new();
    let filter_node = find_filter_node(plan);
    let node = match filter_node {
        Some(LogicalPlan::Filter(filter)) => filter,
        _ => panic!("No filter node found"),
    };

    node.predicate
        .to_owned()
        .apply(|expr| {
            let binary_expr = if let Expr::BinaryExpr(e) = expr {
                e
            } else {
                res.insert(expr.to_owned());
                return Ok(TreeNodeRecursion::Jump);
            };

            let left = &*binary_expr.left;
            let right = &*binary_expr.right;

            if binary_expr.op != Operator::And {
                res.insert(expr.to_owned());
                return Ok(TreeNodeRecursion::Jump);
            }

            match (left, right) {
                (Expr::BinaryExpr(_), Expr::BinaryExpr(_)) => Ok(TreeNodeRecursion::Continue),
                (Expr::BinaryExpr(_), f) => {
                    res.insert(f.to_owned());
                    Ok(TreeNodeRecursion::Continue)
                }
                (f, Expr::BinaryExpr(_)) => {
                    res.insert(f.to_owned());
                    Ok(TreeNodeRecursion::Continue)
                }
                (x, y) => {
                    res.insert(x.to_owned());
                    res.insert(y.to_owned());
                    Ok(TreeNodeRecursion::Jump)
                }
            }
        })
        .expect("shouldn't fail");

    Vec::from_iter(res)
}

// DFS to find the first filter node
fn find_filter_node(node: &LogicalPlan) -> Option<&LogicalPlan> {
    if let LogicalPlan::Filter(_) = node {
        return Some(node);
    }

    node.inputs().iter().find_map(|e| find_filter_node(e))
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
        let scan = match &*f.input {
            LogicalPlan::TableScan(ts) => ts,
            _ => panic!("Not table scan"),
        };
        let key = f.alias.table().to_owned();
        let value = scan.table_name.table().to_owned();
        map.insert(key, value);
    });

    map
}

fn subsets(set: HashSet<&Relation>) -> Vec<Vec<&Relation>> {
    let arr_set: Vec<&Relation> = Vec::from_iter(set);
    let mut res: Vec<Vec<&Relation>> = vec![];
    let mut curr: Vec<&Relation> = vec![];
    subsets_rec(&arr_set, &mut res, &mut curr, 0);
    res
}

fn subsets_rec<'a>(
    set: &Vec<&'a Relation>,
    res: &mut Vec<Vec<&'a Relation>>,
    curr: &mut Vec<&'a Relation>,
    index: usize,
) {
    res.push(curr.clone());

    for i in index..set.len() {
        curr.push(set.get(i).unwrap());
        subsets_rec(set, res, curr, i + 1);
        curr.remove(curr.len() - 1);
    }
}

fn assign_ordering(graph: &Graph) -> Vec<Relation> {
    let mut new_nodes = Vec::new();
    let root = graph
        .nodes
        .first()
        .expect("unable to assign a root in query graph");
    let mut seen = HashSet::new();
    let mut q = VecDeque::new();

    q.push_back(root);
    seen.insert(root);
    let mut i: u32 = 0;
    while !q.is_empty() {
        let curr = q.pop_front().unwrap();
        new_nodes.push(Relation {
            name: curr.to_string(),
            id: i,
        });

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

    new_nodes
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
