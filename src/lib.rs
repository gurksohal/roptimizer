use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TableStats {
    pub rows: u64,
    pub cols: Vec<u64>
}