use csv::ByteRecord;
use rust_decimal::Decimal;
use serde::Serialize;

/// Holds details for a given client
#[derive(Default, Serialize, Debug)]
pub struct Client {
    pub id: u16,
    pub available: Decimal,
    pub held: Decimal,
    pub total: Decimal,
    pub locked: bool,
}

impl Client {
    pub fn headers() -> Vec<&'static str> {
        vec!["client", "available", "held", "total", "locked"]
    }
}

/// Converts into a CSV record
impl From<Client> for csv::ByteRecord {
    fn from(client: Client) -> Self {
        ByteRecord::from(vec![
            client.id.to_string(),
            client.available.to_string(),
            client.held.to_string(),
            client.total.to_string(),
            client.locked.to_string(),
        ])
    }
}
