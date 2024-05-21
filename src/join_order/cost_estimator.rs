use crate::join_order::catalog::Catalog;
use crate::join_order::query_graph::Edge;

pub struct CostEstimator {
    pub catalog: Catalog,
}

impl CostEstimator {
    pub fn est_cost(&self, left_cost: u64, right_cost: u64, edges: Vec<Edge>) -> u64 {
        let card: u64 = left_cost * right_cost;
        let mut sel = vec![];

        for edge in edges {
            let table1 = &edge.node1;
            let table2 = &edge.node2;

            let c1 = self.catalog.get_col_stats(table1, &edge.col1) as f64;
            let c2 = self.catalog.get_col_stats(table2, &edge.col2) as f64;

            let max = c1.max(c2);
            sel.push(1.0/max);
        }
        
        let mut total_sel: f64 = 1.0;
        for s in sel {
            total_sel *= s;
        }
        
        let mut c = ((card as f64) * total_sel) as u64 + left_cost + right_cost;
        if left_cost > right_cost {
            c += 10;
        }
        c
    }

    pub fn table_size(&self, name: &str) -> u64 {
        self.catalog.get_rows(name)
    }
}
