use std::collections::{BTreeSet, HashMap};
use std::fmt::{Display, Formatter};
use std::ops::Deref;

use crate::join_order::catalog::Catalog;
use crate::join_order::cost_estimator::CostEstimator;
use crate::join_order::query_graph::QueryGraph;
use crate::join_order::query_graph::{Edge, Graph};

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
enum JoinNode {
    Tree(JoinTree),
    Single(String),
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
struct JoinTree {
    left: Option<Box<JoinNode>>,
    right: Option<Box<JoinNode>>,
    size: u32
}

impl JoinNode {
    fn to_set(&self) -> BTreeSet<String> {
        match self {
            JoinNode::Tree(t) => { t.to_set() }
            JoinNode::Single(s) => { BTreeSet::from([s.to_owned()]) }
        }
    } 
}
impl JoinTree {
    fn build(left: Option<Box<JoinNode>>, right: Option<Box<JoinNode>>) -> JoinTree {
        let mut size = 0;
        if left.is_some() {
            size += 1;
            if let JoinNode::Tree(t) = left.as_ref().unwrap().deref() {
                size += t.size;
            }
        }
        
        if right.is_some() {
            if let JoinNode::Tree(t) = right.as_ref().unwrap().deref() {
                size += t.size;
            } else {
                size += 1;
            }
        }
        JoinTree { left, right, size }
    }

    fn create_single_node(node: String) -> JoinTree {
        let left = Some(Box::new(JoinNode::Single(node)));
        let right = None;
        let size = 1;
        JoinTree { left, right, size }
    }
    
    fn from_join_node(join_node: &JoinNode) -> JoinTree {
        match join_node {
            JoinNode::Tree(tree) => { tree.clone() }
            JoinNode::Single(single) => { Self::create_single_node(single.to_string()) }
        }
    }
    
    fn to_set(&self) -> BTreeSet<String> {
        let mut set = BTreeSet::new();
        if self.left.is_some() {
            set = set.union(&self.left.as_ref().unwrap().to_set()).cloned().collect();
        }
        
        if self.right.is_some() {
            set = set.union(&self.right.as_ref().unwrap().to_set()).cloned().collect();
        }
        
        return set;
    }
    
    fn join(&self, other: &JoinTree) -> JoinTree {
        let size = self.size + other.size;
        let left = self.clone();
        let right = other.clone();
        
        JoinTree {
            left: Some(Box::new(JoinNode::Tree(left))),
            right: Some(Box::new(JoinNode::Tree(right))),
            size
        }
    } 
}

impl Display for JoinNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinNode::Tree(n) => write!(f, "{}", n),
            JoinNode::Single(s) => write!(f, "{}", s),
        }
    }
}
impl Display for JoinTree {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.left.is_none() {
            let r: &Box<JoinNode> = if let Some(s) = self.right.as_ref() {
                s
            } else {
                panic!("left and right are none");
            };

            return write!(f, "{}", r);
        }

        if self.right.is_none() {
            let r: &Box<JoinNode> = if let Some(s) = self.left.as_ref() {
                s
            } else {
                panic!("left and right are none");
            };

            return write!(f, "{}", r);
        }

        write!(
            f,
            "({} ⋈ {})",
            self.left.as_ref().unwrap(),
            self.right.as_ref().unwrap()
        )
    }
}

pub struct JoinOrderOpt<'a> {
    graph: &'a QueryGraph,
    cost_estimator: CostEstimator,
    costs: HashMap<JoinTree, u64>
}

impl<'a> JoinOrderOpt<'a> {
    
    pub fn build(graph: &QueryGraph) -> JoinOrderOpt {
        let est = CostEstimator { catalog: Catalog::build().unwrap() };
        JoinOrderOpt {
            graph,
            costs: HashMap::new(),
            cost_estimator: est
        }
    }
    pub fn join_order(&mut self) {
        // hashset doesn't impl 'hash', use BTreeSet instead
        let mut best_plan: HashMap<BTreeSet<String>, JoinTree> = HashMap::new();

        for node in &self.graph.nodes {
            let join_tree = JoinTree::create_single_node(node.name.to_string());
            self.cost(&join_tree);
            best_plan.insert(join_tree.to_set(), join_tree);
        }
        
        for (s1, s2) in self.graph.csg_cmp_pairs() {
            let s = s1.union(&s2).cloned().collect();
            println!("{s1:?} AND {s2:?}");
            let p1 = best_plan.get(&s1).expect("unable to find plan for s1").clone();
            let p2 = best_plan.get(&s2).expect("unable to find plan for s2").clone();
            let mut curr_plan = p1.join(&p2);
            let mut curr_best = best_plan.get(&s).unwrap_or(&curr_plan).clone();
            if !best_plan.contains_key(&curr_best.to_set()) || self.cost(&curr_best) > self.cost(&curr_plan) {
                best_plan.insert(s.clone(), curr_plan.clone());
                curr_best = curr_plan;
            }

            curr_plan = p2.join(&p1);
            if self.cost(&curr_best) > self.cost(&curr_plan) {
                best_plan.insert(s, curr_plan);
            }
        }

        let mut key = BTreeSet::new();
        for node in &self.graph.nodes {
            key.insert(node.name.to_string());
        }
        //println!("{best_plan:?}");
        println!("{:?}", best_plan.get(&key).unwrap());
    }
    
    fn cost(&mut self, tree: &JoinTree) -> u64 {
        if self.costs.contains_key(&tree) {
            return self.costs.get(&tree).unwrap().to_owned();
        }
        
        if tree.size == 1 {
            let r = if let JoinNode::Single(s) = tree.left.as_ref().unwrap().deref() {
                s
            } else {
                panic!("left isn't the single relation");
            };
            let r = self.graph.table_name(r);
            let cost = self.cost_estimator.table_size(&r);
            self.costs.insert(tree.to_owned(), cost);
            return cost;
        }
        
        // find what edges connect left, and right
        let mut valid_edges: Vec<&Edge> = vec![];
        let left_set = tree.left.as_ref().unwrap().to_set();
        let right_set = tree.right.as_ref().unwrap().to_set();
        for edge in &self.graph.edges {
            if edge_in_set(&left_set, edge) && edge_in_set(&right_set, edge) {
                valid_edges.push(edge);
            }
        }
        
        // update edges to use non alias names
        let valid_edges: Vec<Edge> = valid_edges.iter_mut().map(|e| {
            Edge {
                node1: self.graph.table_name(&e.node1),
                node2: self.graph.table_name(&e.node2),
                ..e.clone()
            }
        }).collect();
        
        let left_tree = JoinTree::from_join_node(&tree.left.as_ref().unwrap());
        let right_tree = JoinTree::from_join_node(&tree.right.as_ref().unwrap());
        
        let left_cost = self.costs.get(&left_tree).expect("no cost for left plan").to_owned();
        let right_cost = self.costs.get(&right_tree).expect("no cost for right plan").to_owned();
        
        let cost: u64 = self.cost_estimator.est_cost(left_cost, right_cost, valid_edges);
        self.costs.insert(tree.to_owned(), cost);
        cost
    }
}

fn edge_in_set(join_set: &BTreeSet<String>, edge: &Edge) -> bool {
    let left = &edge.node1;
    let right = &edge.node2;

    join_set.contains(left) || join_set.contains(right)
}

#[cfg(test)]
mod test {
    use std::collections::{BTreeSet, HashMap, HashSet};

    use crate::join_order::catalog::{Catalog, TableStats};
    use crate::join_order::cost_estimator::CostEstimator;
    use crate::join_order::dp::JoinOrderOpt;
    use crate::join_order::query_graph::{Edge, Graph};

    fn setup_catalog() -> Catalog {
        // R.B == S.B AND S.C == T.C && R.A == T.A
        let table_r = TableStats {
            rows: 30_000,
            cols: HashMap::from([(String::from("B"), 3), (String::from("A"), 2)]),
        };

        let table_s = TableStats {
            rows: 200_000,
            cols: HashMap::from([(String::from("B"), 2), (String::from("C"), 10)]),
        };

        let table_t = TableStats {
            rows: 10_000,
            cols: HashMap::from([(String::from("C"), 5), (String::from("A"), 1)]),
        };

        let map = HashMap::from([
            (String::from("R"), table_r),
            (String::from("S"), table_s),
            (String::from("T"), table_t),
        ]);

        Catalog { stats: map }
    }

    fn setup_graph() -> Graph {
        let nodes = BTreeSet::from(["R".to_string(), "S".to_string(), "T".to_string()]);
        let edges = HashSet::from([
            Edge {
                node1: "R".to_owned(),
                node2: "S".to_string(),
                col1: "B".to_owned(),
                col2: "B".to_owned(),
            },
            Edge {
                node1: "S".to_owned(),
                node2: "T".to_string(),
                col1: "C".to_owned(),
                col2: "C".to_owned(),
            },
            Edge {
                node1: "R".to_owned(),
                node2: "T".to_string(),
                col1: "A".to_owned(),
                col2: "A".to_owned(),
            },
        ]);

        Graph {
            nodes,
            edges,
            table_names: HashMap::new(),
        }
    }

    #[test]
    fn test() {
        let catalog = setup_catalog();
        let graph = setup_graph();
        let cost_estimator = CostEstimator { catalog };
        let mut opt = JoinOrderOpt {
            graph: &graph,
            cost_estimator,
            costs: HashMap::new()
        };
        
        opt.join_order();
    }
}
