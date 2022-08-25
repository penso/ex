use rust_decimal::Decimal;
use serde::Deserialize;
use std::fmt::Display;

/// All available types
#[derive(Debug, Deserialize, Eq, PartialEq, Clone, strum_macros::Display)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType {
    Deposit,
    Widthdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

impl Default for TransactionType {
    fn default() -> Self {
        TransactionType::Deposit
    }
}

/// Holds a single transaction
#[derive(Debug, Deserialize, Clone, Default)]
pub struct Transaction {
    pub r#type: TransactionType,
    pub client: u16,
    pub tx: u32,
    pub amount: Option<Decimal>,
    #[serde(skip)]
    pub succeeded: bool,
}

/// For debug purpose
impl Display for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "type: {} client: {} tx: {} amount: {}",
            self.r#type,
            self.client,
            self.tx,
            self.amount.map(|a| a.to_string()).unwrap_or_default()
        )
    }
}
