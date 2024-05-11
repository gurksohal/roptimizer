use std::collections::HashMap;
use std::error::Error;
use std::fs;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TableStats {
    pub rows: u64,
    pub cols: Vec<u64>
}

#[derive(Debug)]
pub struct Catalog {
    stats: HashMap<String, TableStats>
}

impl Catalog {
    pub fn build() -> Result<Catalog, Box<dyn Error>> {
        let path = "./data/stats.json";
        let json = fs::read_to_string(path)?;
        let map: HashMap<String, TableStats> = serde_json::from_str(&json)?;
        
        Ok(Catalog {
            stats: map
        })
    }
}