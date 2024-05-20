use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs;

#[derive(Debug, Serialize, Deserialize)]
pub struct TableStats {
    pub rows: u64,
    pub cols: HashMap<String, u64>,
}

#[derive(Debug)]
pub struct Catalog {
    pub(crate) stats: HashMap<String, TableStats>,
}

impl Catalog {
    pub fn build() -> Result<Catalog, Box<dyn Error>> {
        let path = "./data/stats.json";
        let json = fs::read_to_string(path)?;
        let map: HashMap<String, TableStats> = serde_json::from_str(&json)?;

        Ok(Catalog { stats: map })
    }

    pub fn get_rows(&self, table: &str) -> u64 {
        self.stats.get(table).unwrap().rows
    }

    pub fn get_col_stats(&self, table: &str, col: &str) -> u64 {
        self.stats
            .get(table)
            .unwrap()
            .cols
            .get(col)
            .unwrap()
            .to_owned()
    }
}
