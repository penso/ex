use csv::ByteRecord;
use csv_async::Trim;
use std::collections::HashMap;
use tokio::fs::File;
use tokio_stream::StreamExt;

use crate::entities::client::Client;
use crate::entities::transaction::{Transaction, TransactionType};

type TransactionHash = HashMap<u32, Transaction>;
type ClientHash = HashMap<u16, Client>;

/// Will parse the given `file_name` as a stream input then write the result in `output`
pub async fn parse_data(file_name: &str) -> anyhow::Result<()> {
    let mut rdr = csv_async::AsyncReaderBuilder::new()
        .has_headers(true)
        .trim(Trim::All)
        .create_deserializer(File::open(file_name).await?);

    let mut transactions = rdr.deserialize::<Transaction>();

    // TODO: those would usually be stored in a DB but for simplicity of this exercise we keep them in memory
    let mut clients = HashMap::new();
    let mut past_transactions = HashMap::new();
    let mut disputed_transactions = HashMap::new();

    // 1. Parsing input
    while let Some(transaction) = transactions.next().await {
        let mut transaction = transaction?;
        parse_single_transaction(
            &mut transaction,
            &mut clients,
            &mut past_transactions,
            &mut disputed_transactions,
        )?;
    }

    // 2. Output
    let mut wtr = csv_async::AsyncWriter::from_writer(vec![]);
    wtr.write_record(Client::headers()).await?;
    for (_, client) in clients {
        wtr.write_record(&ByteRecord::from(client)).await?;
    }

    let data = String::from_utf8(wtr.into_inner().await?)?;
    println!("{}", data);

    Ok(())
}

fn parse_single_transaction(
    transaction: &mut Transaction,
    clients: &mut ClientHash,
    past_transactions: &mut TransactionHash,
    disputed_transactions: &mut TransactionHash,
) -> anyhow::Result<()> {
    let client = match clients.get_mut(&transaction.client) {
        Some(client) => client,
        None => {
            let client = Client {
                id: transaction.client,
                ..Default::default()
            };
            clients.insert(transaction.client, client);
            clients
                .get_mut(&transaction.client)
                .expect("client isn't available")
        }
    };

    match transaction.r#type {
        TransactionType::Deposit => {
            let amount = transaction.amount.expect("no amount");
            client.total += amount;
            client.available += amount;
            transaction.succeeded = true;
            past_transactions.insert(transaction.tx, transaction.clone());
        }
        TransactionType::Widthdrawal => {
            let amount = transaction.amount.expect("no amount");
            if client.available < amount {
                eprintln!(
                    "Can't widthdraw amount {} for client {}, not enough fund",
                    amount, client.id
                );
            } else {
                client.available -= amount;
                client.total -= amount;
                transaction.succeeded = true;
                past_transactions.insert(transaction.tx, transaction.clone());
            }
        }
        TransactionType::Dispute => match past_transactions.get(&transaction.tx) {
            None => {
                eprintln!(
                    "Can't dispute tx {} for client {}, non-existing transaction",
                    transaction.tx, client.id
                );
            }
            Some(past_transaction) => {
                if past_transaction.r#type == TransactionType::Deposit {
                    let amount = past_transaction
                        .amount
                        .expect("no amount for past transaction");

                    if client.available < amount {
                        eprintln!(
                            "Can't dispute amount {} for client {}, not enough fund",
                            amount, client.id
                        );
                    } else {
                        client.held += amount;
                        client.available -= amount;
                        disputed_transactions.insert(past_transaction.tx, past_transaction.clone());
                        transaction.succeeded = true
                    }
                } else {
                    eprintln!(
                        "Can't dispute tx {} for client {}, isn't a deposit tx",
                        past_transaction.tx, client.id
                    );
                }
            }
        },
        TransactionType::Resolve => match disputed_transactions.get(&transaction.tx) {
            None => {
                eprintln!(
                    "Can't resolve tx {} for client {}, non-existing disputed transaction",
                    transaction.tx, client.id
                );
            }
            Some(disputed_transaction) => {
                let amount = disputed_transaction
                    .amount
                    .expect("no amount for disputed transaction");

                client.held -= amount;
                client.available += amount;
                disputed_transactions.remove(&transaction.tx);
                transaction.succeeded = true
            }
        },
        TransactionType::Chargeback => match disputed_transactions.get(&transaction.tx) {
            None => {
                eprintln!(
                    "Can't chargeback tx {} for client {}, non-existing disputed transaction",
                    transaction.tx, client.id
                );
            }
            Some(disputed_transaction) => {
                let amount = disputed_transaction
                    .amount
                    .expect("no amount for disputed transaction");

                client.held -= amount;
                client.total -= amount;
                client.locked = true;
                disputed_transactions.remove(&transaction.tx);
                transaction.succeeded = true
            }
        },
    }

    eprintln!("Transaction: {:?}", transaction);
    eprintln!("Client: {:?}", client);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assertor::*;
    use rust_decimal_macros::dec;

    #[derive(Default)]
    struct TestContext {
        clients: ClientHash,
        past_transactions: TransactionHash,
        disputed_transactions: TransactionHash,
    }

    #[tokio::test]
    async fn test_deposits_one() -> anyhow::Result<()> {
        let mut test_context = TestContext::default();
        let mut transaction = Transaction {
            r#type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(dec!(2.0)),
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        assert_that!(test_context.clients[&1].available).is_equal_to(dec!(2.0));
        assert_that!(test_context.clients[&1].held).is_equal_to(dec!(0));
        assert_that!(test_context.clients[&1].total).is_equal_to(dec!(2.0));
        assert_that!(test_context.clients[&1].locked).is_equal_to(false);
        assert_that!(test_context.clients).has_length(1);
        assert_that!(test_context.past_transactions).has_length(1);
        assert_that!(test_context.disputed_transactions).has_length(0);
        Ok(())
    }

    #[tokio::test]
    async fn test_deposits_two() -> anyhow::Result<()> {
        let mut test_context = TestContext::default();
        let mut transaction = Transaction {
            r#type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(dec!(2.0)),
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        let mut transaction = Transaction {
            r#type: TransactionType::Deposit,
            client: 1,
            tx: 2,
            amount: Some(dec!(5.890)),
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        assert_that!(test_context.clients[&1].available).is_equal_to(dec!(7.890));
        assert_that!(test_context.clients[&1].held).is_equal_to(dec!(0));
        assert_that!(test_context.clients[&1].total).is_equal_to(dec!(7.890));
        assert_that!(test_context.clients[&1].locked).is_equal_to(false);
        assert_that!(test_context.clients).has_length(1);
        assert_that!(test_context.past_transactions).has_length(2);
        assert_that!(test_context.disputed_transactions).has_length(0);
        Ok(())
    }

    #[tokio::test]
    async fn test_widthdrawal_enough_fund() -> anyhow::Result<()> {
        let mut test_context = TestContext::default();
        let mut transaction = Transaction {
            r#type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(dec!(20.1234)),
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        let mut transaction = Transaction {
            r#type: TransactionType::Widthdrawal,
            client: 1,
            tx: 2,
            amount: Some(dec!(10.001)),
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        assert_that!(test_context.clients[&1].available).is_equal_to(dec!(10.1224));
        assert_that!(test_context.clients[&1].held).is_equal_to(dec!(0));
        assert_that!(test_context.clients[&1].total).is_equal_to(dec!(10.1224));
        assert_that!(test_context.clients[&1].locked).is_equal_to(false);
        assert_that!(test_context.clients).has_length(1);
        assert_that!(test_context.past_transactions).has_length(2);
        assert_that!(test_context.disputed_transactions).has_length(0);

        Ok(())
    }

    #[tokio::test]
    async fn test_widthdrawal_not_enough_fund() -> anyhow::Result<()> {
        let mut test_context = TestContext::default();
        let mut transaction = Transaction {
            r#type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(dec!(20.1234)),
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        let mut transaction = Transaction {
            r#type: TransactionType::Widthdrawal,
            client: 1,
            tx: 2,
            amount: Some(dec!(20.12345)),
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(!transaction.succeeded);

        assert_that!(test_context.clients[&1].available).is_equal_to(dec!(20.1234));
        assert_that!(test_context.clients[&1].held).is_equal_to(dec!(0));
        assert_that!(test_context.clients[&1].total).is_equal_to(dec!(20.1234));
        assert_that!(test_context.clients[&1].locked).is_equal_to(false);
        assert_that!(test_context.clients).has_length(1);
        assert_that!(test_context.past_transactions).has_length(1);
        assert_that!(test_context.disputed_transactions).has_length(0);

        Ok(())
    }

    #[tokio::test]
    async fn test_dispute_tx_exists() -> anyhow::Result<()> {
        let mut test_context = TestContext::default();
        let mut transaction = Transaction {
            r#type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(dec!(20.1234)),
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        let mut transaction = Transaction {
            r#type: TransactionType::Deposit,
            client: 1,
            tx: 2,
            amount: Some(dec!(1.123)),
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        let mut transaction = Transaction {
            r#type: TransactionType::Dispute,
            client: 1,
            tx: 2,
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        assert_that!(test_context.clients[&1].available).is_equal_to(dec!(20.1234));
        assert_that!(test_context.clients[&1].held).is_equal_to(dec!(1.123));
        assert_that!(test_context.clients[&1].total).is_equal_to(dec!(20.1234) + dec!(1.123));
        assert_that!(test_context.clients[&1].locked).is_equal_to(false);
        assert_that!(test_context.clients).has_length(1);
        assert_that!(test_context.past_transactions).has_length(2);
        assert_that!(test_context.disputed_transactions).has_length(1);

        Ok(())
    }

    #[tokio::test]
    async fn test_dispute_tx_does_not_exist() -> anyhow::Result<()> {
        let mut test_context = TestContext::default();
        let mut transaction = Transaction {
            r#type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(dec!(20.1234)),
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        let mut transaction = Transaction {
            r#type: TransactionType::Deposit,
            client: 1,
            tx: 2,
            amount: Some(dec!(1.123)),
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        let mut transaction = Transaction {
            r#type: TransactionType::Dispute,
            client: 1,
            tx: 3,
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(!transaction.succeeded);

        assert_that!(test_context.clients[&1].available).is_equal_to(dec!(20.1234) + dec!(1.123));
        assert_that!(test_context.clients[&1].held).is_equal_to(dec!(0));
        assert_that!(test_context.clients[&1].total).is_equal_to(dec!(20.1234) + dec!(1.123));
        assert_that!(test_context.clients[&1].locked).is_equal_to(false);
        assert_that!(test_context.clients).has_length(1);
        assert_that!(test_context.past_transactions).has_length(2);
        assert_that!(test_context.disputed_transactions).has_length(0);

        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_tx_exists() -> anyhow::Result<()> {
        let mut test_context = TestContext::default();
        let mut transaction = Transaction {
            r#type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(dec!(20.1234)),
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        let mut transaction = Transaction {
            r#type: TransactionType::Deposit,
            client: 1,
            tx: 2,
            amount: Some(dec!(1.123)),
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        let mut transaction = Transaction {
            r#type: TransactionType::Dispute,
            client: 1,
            tx: 2,
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        let mut transaction = Transaction {
            r#type: TransactionType::Resolve,
            client: 1,
            tx: 2,
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        assert_that!(test_context.clients[&1].available).is_equal_to(dec!(20.1234) + dec!(1.123));
        assert_that!(test_context.clients[&1].held).is_equal_to(dec!(0));
        assert_that!(test_context.clients[&1].total).is_equal_to(dec!(20.1234) + dec!(1.123));
        assert_that!(test_context.clients[&1].locked).is_equal_to(false);
        assert_that!(test_context.clients).has_length(1);
        assert_that!(test_context.past_transactions).has_length(2);
        assert_that!(test_context.disputed_transactions).has_length(0);

        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_tx_does_not_exist() -> anyhow::Result<()> {
        let mut test_context = TestContext::default();
        let mut transaction = Transaction {
            r#type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(dec!(20.1234)),
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        let mut transaction = Transaction {
            r#type: TransactionType::Deposit,
            client: 1,
            tx: 2,
            amount: Some(dec!(1.123)),
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        let mut transaction = Transaction {
            r#type: TransactionType::Dispute,
            client: 1,
            tx: 3,
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(!transaction.succeeded);

        let mut transaction = Transaction {
            r#type: TransactionType::Resolve,
            client: 1,
            tx: 3,
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(!transaction.succeeded);

        assert_that!(test_context.clients[&1].available).is_equal_to(dec!(20.1234) + dec!(1.123));
        assert_that!(test_context.clients[&1].held).is_equal_to(dec!(0));
        assert_that!(test_context.clients[&1].total).is_equal_to(dec!(20.1234) + dec!(1.123));
        assert_that!(test_context.clients[&1].locked).is_equal_to(false);
        assert_that!(test_context.clients).has_length(1);
        assert_that!(test_context.past_transactions).has_length(2);
        assert_that!(test_context.disputed_transactions).has_length(0);

        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_chargeback_exists() -> anyhow::Result<()> {
        let mut test_context = TestContext::default();
        let mut transaction = Transaction {
            r#type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(dec!(20.1234)),
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        let mut transaction = Transaction {
            r#type: TransactionType::Deposit,
            client: 1,
            tx: 2,
            amount: Some(dec!(1.123)),
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        let mut transaction = Transaction {
            r#type: TransactionType::Dispute,
            client: 1,
            tx: 2,
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        let mut transaction = Transaction {
            r#type: TransactionType::Chargeback,
            client: 1,
            tx: 2,
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        assert_that!(test_context.clients[&1].available).is_equal_to(dec!(20.1234));
        assert_that!(test_context.clients[&1].held).is_equal_to(dec!(0));
        assert_that!(test_context.clients[&1].total).is_equal_to(dec!(20.1234));
        assert!(test_context.clients[&1].locked);
        assert_that!(test_context.clients).has_length(1);
        assert_that!(test_context.past_transactions).has_length(2);
        assert_that!(test_context.disputed_transactions).has_length(0);

        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_chargeback_does_not_exist() -> anyhow::Result<()> {
        let mut test_context = TestContext::default();
        let mut transaction = Transaction {
            r#type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(dec!(20.1234)),
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        let mut transaction = Transaction {
            r#type: TransactionType::Deposit,
            client: 1,
            tx: 2,
            amount: Some(dec!(1.123)),
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(transaction.succeeded);

        let mut transaction = Transaction {
            r#type: TransactionType::Resolve,
            client: 1,
            tx: 3,
            ..Default::default()
        };
        parse_single_transaction(
            &mut transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;
        assert!(!transaction.succeeded);

        assert_that!(test_context.clients[&1].available).is_equal_to(dec!(20.1234) + dec!(1.123));
        assert_that!(test_context.clients[&1].held).is_equal_to(dec!(0));
        assert_that!(test_context.clients[&1].total).is_equal_to(dec!(20.1234) + dec!(1.123));
        assert!(!test_context.clients[&1].locked);
        assert_that!(test_context.clients).has_length(1);
        assert_that!(test_context.past_transactions).has_length(2);
        assert_that!(test_context.disputed_transactions).has_length(0);

        Ok(())
    }
}
