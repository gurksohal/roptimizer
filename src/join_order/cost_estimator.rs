use std::collections::HashMap;
use std::ops::{Deref, Mul};

use num::bigint::ToBigInt;
use num::{BigRational, BigUint};

use crate::join_order::catalog::Catalog;
use crate::join_order::dp::{JoinNode, JoinTree};

pub struct CostEstimator {
    pub catalog: Catalog,
}

impl CostEstimator {
    pub fn est_cost(&self, tree: &JoinTree, table_names: &HashMap<String, String>) -> BigUint {
        if tree.size == 1 {
            return self.get_card(tree, table_names);
        }

        let left_tree = JoinTree::from_join_node(tree.left.as_ref().unwrap());
        let right_tree = JoinTree::from_join_node(tree.right.as_ref().unwrap());

        let left_cost = self.est_cost(&left_tree, table_names);
        let right_cost = self.est_cost(&right_tree, table_names);
        let curr_card = self.get_card(tree, table_names);

        curr_card + left_cost + right_cost
    }

    // num of output rows
    fn get_card(&self, tree: &JoinTree, table_names: &HashMap<String, String>) -> BigUint {
        if tree.size == 1 {
            let r = if let JoinNode::Single(s) = tree.left.as_ref().unwrap().deref() {
                s
            } else {
                panic!("left isn't the single relation");
            };
            assert_eq!(tree.right, None, "Right should be empty");

            let r = self.table_name(table_names, r);
            let cost = self.table_size(&r);
            return BigUint::from(cost);
        }

        let left_tree = JoinTree::from_join_node(tree.left.as_ref().unwrap());
        let right_tree = JoinTree::from_join_node(tree.right.as_ref().unwrap());

        let left_card = self.get_card(&left_tree, table_names);
        let right_card = self.get_card(&right_tree, table_names);

        let edges = &tree.edges;
        assert!(!edges.is_empty());

        let mut sel: f64 = 1.0;
        for edge in edges {
            assert!(tree.contains(&edge.node1));
            assert!(tree.contains(&edge.node2));
            sel *= 0.2;
        }

        let sel = BigRational::from_float(sel).unwrap();
        let card = left_card * right_card;
        let card = BigRational::from_integer(card.to_bigint().unwrap());
        let ans = card.mul(sel);
        ans.to_integer().to_biguint().unwrap()
    }

    pub fn table_size(&self, name: &str) -> u64 {
        self.catalog.get_rows(name)
    }

    fn table_name(&self, table_names: &HashMap<String, String>, name: &str) -> String {
        if table_names.contains_key(name) {
            return table_names.get(name).unwrap().to_string();
        }

        name.to_string()
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use num::BigUint;

    use crate::join_order::catalog::{Catalog, TableStats};
    use crate::join_order::cost_estimator::CostEstimator;
    use crate::join_order::dp::{JoinNode, JoinTree};
    use crate::join_order::query_graph::Edge;

    fn create_catalog() -> Catalog {
        let a = TableStats {
            rows: 1000,
            cols: HashMap::from([("A".to_string(), 200)]),
        };

        let b = TableStats {
            rows: 500,
            cols: HashMap::from([("A".to_string(), 200)]),
        };

        let c = TableStats {
            rows: 2000,
            cols: HashMap::from([("A".to_string(), 4)]),
        };

        let d = TableStats {
            rows: 4,
            cols: HashMap::from([("A".to_string(), 4)]),
        };

        let stats = HashMap::from([
            ("A".to_string(), a),
            ("B".to_string(), b),
            ("C".to_string(), c),
            ("D".to_string(), d),
        ]);
        Catalog { stats }
    }

    #[test]
    fn single_relation() {
        let catalog = create_catalog();
        let cost_estimator = CostEstimator { catalog };

        let join_node = JoinNode::Single("A".to_string());
        let join_tree = JoinTree::from_join_node(&join_node);
        let table: HashMap<String, String> = HashMap::new();

        let card = cost_estimator.get_card(&join_tree, &table);
        let cost = cost_estimator.est_cost(&join_tree, &table);
        assert_eq!(card, cost);
        assert_eq!(card, BigUint::from(1000_u32));
    }

    #[test]
    fn two_relations() {
        let table: HashMap<String, String> = HashMap::new();
        let catalog = create_catalog();
        let cost_estimator = CostEstimator { catalog };

        let a_join_node = JoinNode::Single("A".to_string());
        let b_join_node = JoinNode::Single("B".to_string());
        let a_join_tree = JoinTree::from_join_node(&a_join_node);
        let b_join_tree = JoinTree::from_join_node(&b_join_node);
        let edge = Edge {
            node1: "A".to_string(),
            node2: "B".to_string(),
            col1: "A".to_string(),
            col2: "A".to_string(),
        };
        let expected_ans = BigUint::from(101500_u64);
        let join_tree = a_join_tree.join(&b_join_tree, vec![edge]);
        assert_eq!(cost_estimator.est_cost(&join_tree, &table), expected_ans);
    }

    #[test]
    fn three_relations() {
        let table: HashMap<String, String> = HashMap::new();
        let catalog = create_catalog();
        let cost_estimator = CostEstimator { catalog };

        let a_join_node = JoinNode::Single("A".to_string());
        let b_join_node = JoinNode::Single("B".to_string());
        let c_join_node = JoinNode::Single("C".to_string());

        let a_join_tree = JoinTree::from_join_node(&a_join_node);
        let b_join_tree = JoinTree::from_join_node(&b_join_node);
        let c_join_tree = JoinTree::from_join_node(&c_join_node);

        let edge_left = Edge {
            node1: "A".to_string(),
            node2: "B".to_string(),
            col1: "A".to_string(),
            col2: "A".to_string(),
        };

        let edge_right = Edge {
            node1: "C".to_string(),
            node2: "B".to_string(),
            col1: "A".to_string(),
            col2: "A".to_string(),
        };

        let tree = a_join_tree
            .join(&b_join_tree, vec![edge_left])
            .join(&c_join_tree, vec![edge_right]);

        let expected_ans = BigUint::from(40103500_u64);
        assert_eq!(cost_estimator.est_cost(&tree, &table), expected_ans);
    }

    #[test]
    fn bushy_join_tree() {
        let table: HashMap<String, String> = HashMap::new();
        let catalog = create_catalog();
        let cost_estimator = CostEstimator { catalog };

        let a_join_node = JoinNode::Single("A".to_string());
        let b_join_node = JoinNode::Single("B".to_string());
        let c_join_node = JoinNode::Single("C".to_string());
        let d_join_node = JoinNode::Single("D".to_string());

        let a_join_tree = JoinTree::from_join_node(&a_join_node);
        let b_join_tree = JoinTree::from_join_node(&b_join_node);
        let c_join_tree = JoinTree::from_join_node(&c_join_node);
        let d_join_tree = JoinTree::from_join_node(&d_join_node);

        let ab_edge = Edge {
            node1: "A".to_string(),
            node2: "B".to_string(),
            col2: "A".to_string(),
            col1: "A".to_string(),
        };

        let cd_edge = Edge {
            node1: "C".to_string(),
            node2: "D".to_string(),
            col2: "A".to_string(),
            col1: "A".to_string(),
        };

        let ac_edge = Edge {
            node1: "A".to_string(),
            node2: "C".to_string(),
            col2: "A".to_string(),
            col1: "A".to_string(),
        };

        let left_tree = a_join_tree.join(&b_join_tree, vec![ab_edge]);
        let right_tree = c_join_tree.join(&d_join_tree, vec![cd_edge]);
        let final_tree = left_tree.join(&right_tree, vec![ac_edge]);

        let expected_res = BigUint::from(32105104_u64);
        assert_eq!(cost_estimator.est_cost(&final_tree, &table), expected_res);
    }
}
