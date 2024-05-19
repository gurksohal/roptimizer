use std::collections::HashSet;

use crate::join_order::catalog::Catalog;
use crate::join_order::query_graph::Graph;

struct CostEstimator {
    catalog: Catalog,
}

impl CostEstimator {
    pub fn estimate_cost(&self, joins: HashSet<String>, graph: Graph) -> u64 {
        // assume all the tables are name normal
        let mut card: u64 = 1;
        for table_name in &joins {
            let name = graph.table_name(table_name);
            card *= self.catalog.get_rows(&name);
        }

        let mut sel: f64 = 1.0;
        for edge in &graph.edges {
            let table1 = &edge.node1;
            let table2 = &edge.node2;

            if !joins.contains(table1) || !joins.contains(table2) {
                continue;
            }

            let table1 = graph.table_name(table1);
            let table2 = graph.table_name(table2);
            let c1 = self.catalog.get_col_stats(&table1, &edge.col1) as f64;
            let c2 = self.catalog.get_col_stats(&table2, &edge.col2) as f64;

            let max = c1.max(c2);
            sel *= 1f64 / max;
        }

        ((card as f64) * sel) as u64
    }
}

#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};

    use crate::join_order::catalog::{Catalog, TableStats};
    use crate::join_order::cost_estimator::CostEstimator;
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
        let nodes = HashSet::from(["R".to_string(), "S".to_string(), "T".to_string()]);
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
            table_names: HashMap::new()
        }
    }
    
    #[test]
    fn test_3_joins() {
        let catalog = setup_catalog();
        let graph = setup_graph();
        let joins = HashSet::from(["R".to_string(), "S".to_string(), "T".to_string()]);
        let cost_estimator = CostEstimator { catalog };
        let cost = cost_estimator.estimate_cost(joins, graph);
        assert_eq!(cost, 1_000_000_000_000);
    }
    
    #[test]
    fn test_1_join() {
        let catalog = setup_catalog();
        let graph = setup_graph();
        let joins = HashSet::from(["R".to_string()]);
        let cost_estimator = CostEstimator { catalog };
        let cost = cost_estimator.estimate_cost(joins, graph);
        assert_eq!(cost, 30_000);
    }
}
