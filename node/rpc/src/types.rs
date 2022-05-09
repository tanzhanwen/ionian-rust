use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Status {
    pub msg: String,
}
