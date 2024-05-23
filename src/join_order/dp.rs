use std::collections::{BTreeSet, HashMap};
use std::fmt::{Display, Formatter};
use num::BigUint;

use crate::join_order::catalog::Catalog;
use crate::join_order::cost_estimator::CostEstimator;
use crate::join_order::query_graph::Edge;
use crate::join_order::query_graph::QueryGraph;

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub enum JoinNode {
    Tree(JoinTree),
    Single(String),
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct JoinTree {
    pub left: Option<Box<JoinNode>>,
    pub right: Option<Box<JoinNode>>,
    pub size: u32,
    pub edges: Vec<Edge>,
}

impl JoinNode {
    pub fn to_set(&self) -> BTreeSet<String> {
        match self {
            JoinNode::Tree(t) => t.to_set(),
            JoinNode::Single(s) => BTreeSet::from([s.to_owned()]),
        }
    }
    
    fn contains(&self, table: &str) -> bool {
        match self {
            JoinNode::Tree(t) => { t.contains(table) }
            JoinNode::Single(s) => { s == table }
        }
    } 
}
impl JoinTree {
    fn create_single_node(node: String) -> JoinTree {
        let left = Some(Box::new(JoinNode::Single(node)));
        let right = None;
        let size = 1;
        JoinTree {
            left,
            right,
            size,
            edges: vec![],
        }
    }

    pub fn from_join_node(join_node: &JoinNode) -> JoinTree {
        match join_node {
            JoinNode::Tree(tree) => tree.clone(),
            JoinNode::Single(single) => Self::create_single_node(single.to_string()),
        }
    }

    pub fn to_set(&self) -> BTreeSet<String> {
        let mut set = BTreeSet::new();
        if self.left.is_some() {
            set = set
                .union(&self.left.as_ref().unwrap().to_set())
                .cloned()
                .collect();
        }

        if self.right.is_some() {
            set = set
                .union(&self.right.as_ref().unwrap().to_set())
                .cloned()
                .collect();
        }

        set
    }

    pub fn join(&self, other: &JoinTree, edges: Vec<Edge>) -> JoinTree {
        let size = self.size + other.size;
        let left = self.clone();
        let right = other.clone();

        JoinTree {
            left: Some(Box::new(JoinNode::Tree(left))),
            right: Some(Box::new(JoinNode::Tree(right))),
            size,
            edges,
        }
    }
    
    pub fn contains(&self, table: &str) -> bool {
        if self.size == 1 {
            return self.left.as_ref().unwrap().contains(table);
        }
        
        self.left.as_ref().unwrap().contains(table) || self.right.as_ref().unwrap().contains(table)
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
            let r: &JoinNode = if let Some(s) = self.right.as_ref() {
                s
            } else {
                panic!("left and right are none");
            };

            return write!(f, "{}", r);
        }

        if self.right.is_none() {
            let r: &JoinNode = if let Some(s) = self.left.as_ref() {
                s
            } else {
                panic!("left and right are none");
            };

            return write!(f, "{}", r);
        }

        let mut s = String::new();
        self.edges.iter().for_each(|e| {
            s = format!("{} {} ", s, e);
        });
        write!(
            f,
            "({} ⋈ {} [{}])",
            self.left.as_ref().unwrap(),
            self.right.as_ref().unwrap(),
            s
        )
    }
}

pub struct JoinOrderOpt<'a> {
    graph: &'a QueryGraph,
    cost_estimator: CostEstimator,
}

impl<'a> JoinOrderOpt<'a> {
    pub fn build(graph: &QueryGraph, catalog: Catalog) -> JoinOrderOpt {
        let est = CostEstimator {
            catalog
        };
        JoinOrderOpt {
            graph,
            cost_estimator: est,
        }
    }

    pub fn join_order(&mut self) -> JoinTree {
        // hashset doesn't impl 'hash', use BTreeSet instead
        let mut best_plan: HashMap<BTreeSet<String>, JoinTree> = HashMap::new();

        for node in &self.graph.nodes {
            let join_tree = JoinTree::create_single_node(node.name.to_string());
            self.cost(&join_tree);
            best_plan.insert(join_tree.to_set(), join_tree);
        }

        for (s1, s2) in self.graph.csg_cmp_pairs() {
            let s = s1.union(&s2).cloned().collect();

            let p1 = best_plan
                .get(&s1)
                .expect("unable to find plan for s1")
                .clone();

            let p2 = best_plan
                .get(&s2)
                .expect("unable to find plan for s2")
                .clone();

            let connecting_edges = self.connecting_edges(&p1, &p2);
            let mut curr_plan = p1.join(&p2, connecting_edges.clone());
            let mut curr_best = best_plan.get(&s).unwrap_or(&curr_plan).clone();

            if !best_plan.contains_key(&curr_best.to_set())
                || self.cost(&curr_best) > self.cost(&curr_plan)
            {
                best_plan.insert(s.clone(), curr_plan.clone());
                curr_best = curr_plan;
            }

            curr_plan = p2.join(&p1, connecting_edges);
            if self.cost(&curr_best) > self.cost(&curr_plan) {
                best_plan.insert(s, curr_plan);
            }
        }

        let mut key = BTreeSet::new();
        for node in &self.graph.nodes {
            key.insert(node.name.to_string());
        }

        best_plan.get(&key).unwrap().to_owned()
    }

    // Get edges, which connect some node from left tree, to some node in right tree
    fn connecting_edges(&self, p1: &JoinTree, p2: &JoinTree) -> Vec<Edge> {
        let left_set = p1.to_set();
        let right_set = p2.to_set();
        let edges: Vec<Edge> = self
            .graph
            .edges
            .iter()
            .filter_map(|edge| {
                if p1.edges.contains(edge) || p2.edges.contains(edge) {
                    return None;
                }

                let is_connecting = edge_in_set(&left_set, edge) && edge_in_set(&right_set, edge);
                if is_connecting {
                    return Some(edge.clone());
                }

                None
            })
            .collect();

        edges
    }

    fn cost(&self, join_tree: &JoinTree) -> BigUint {
        self.cost_estimator
            .est_cost(join_tree, &self.graph.table_names)
    }
}

fn edge_in_set(join_set: &BTreeSet<String>, edge: &Edge) -> bool {
    let left = &edge.node1;
    let right = &edge.node2;

    join_set.contains(left)
        || join_set.contains(right) && !(join_set.contains(left) && join_set.contains(right))
}
