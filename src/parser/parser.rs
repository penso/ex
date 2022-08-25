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
pub async fn parse_data(file_name: &str, output: &str) -> anyhow::Result<()> {
    let mut rdr = csv_async::AsyncReaderBuilder::new()
        .has_headers(true)
        .trim(Trim::All)
        .create_deserializer(File::open(file_name).await?);

    let mut wri = csv_async::AsyncWriter::from_writer(File::create(output).await?);

    let mut transactions = rdr.deserialize::<Transaction>();

    // TODO: those would usually be stored in a DB but for simplicity of this exercise we keep them in memory
    let mut clients = HashMap::new();
    let mut past_transactions = HashMap::new();
    let mut disputed_transactions = HashMap::new();

    // 1. Parsing
    while let Some(transaction) = transactions.next().await {
        let transaction = transaction?;
        parse_single_transaction(
            transaction,
            &mut clients,
            &mut past_transactions,
            &mut disputed_transactions,
        )?;
    }

    // 2. Saving
    wri.write_record(Client::headers()).await?;
    for (_, client) in clients {
        wri.write_record(&ByteRecord::from(client)).await?;
    }

    Ok(())
}

fn parse_single_transaction(
    transaction: Transaction,
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

    let amount = transaction.amount;
    match transaction.r#type {
        TransactionType::Deposit => {
            client.total += amount;
            client.available += amount;
        }
        TransactionType::Widthdrawal => {
            if client.available < amount {
                eprintln!(
                    "Can't widthdraw amount {} for client {}, not enough fund",
                    amount, client.id
                );
            } else {
                client.available -= amount;
                client.total -= amount;
            }
        }
        TransactionType::Dispute => match past_transactions.get(&transaction.tx) {
            None => {
                eprintln!(
                    "Can't dispute amount {} for client {}, non-existing transaction",
                    amount, client.id
                );
            }
            Some(past_transaction) => {
                let amount = past_transaction.amount;
                if client.available < amount {
                    eprintln!(
                        "Can't dispute amount {} for client {}, not enough fund",
                        amount, client.id
                    );
                } else {
                    client.held += amount;
                    client.available -= amount;
                    disputed_transactions.insert(past_transaction.tx, past_transaction.clone());
                }
            }
        },
        TransactionType::Resolve => match disputed_transactions.get(&transaction.tx) {
            None => {
                eprintln!(
                    "Can't resolve amount {} for client {}, non-existing disputed transaction",
                    amount, client.id
                );
            }
            Some(disputed_transaction) => {
                let amount = disputed_transaction.amount;

                client.held -= amount;
                client.available += amount;
                disputed_transactions.remove(&transaction.tx);
            }
        },
        TransactionType::Chargeback => match disputed_transactions.get(&transaction.tx) {
            None => {
                eprintln!(
                    "Can't chargeback amount {} for client {}, non-existing disputed transaction",
                    amount, client.id
                );
            }
            Some(disputed_transaction) => {
                let amount = disputed_transaction.amount;

                client.held -= amount;
                client.total -= amount;
                client.locked = true;
                disputed_transactions.remove(&transaction.tx);
            }
        },
    }

    eprintln!("Transaction: {:?}", transaction);
    past_transactions.insert(transaction.tx, transaction);
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
    async fn test_deposits() -> anyhow::Result<()> {
        let mut test_context = TestContext::default();
        let transaction = Transaction {
            r#type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: dec!(2.0),
        };
        parse_single_transaction(
            transaction,
            &mut test_context.clients,
            &mut test_context.past_transactions,
            &mut test_context.disputed_transactions,
        )?;

        assert_that!(test_context.clients[&1].available).is_equal_to(dec!(2.0));
        assert_that!(test_context.clients[&1].held).is_equal_to(dec!(0));
        assert_that!(test_context.clients[&1].total).is_equal_to(dec!(2.0));
        assert_that!(test_context.clients[&1].locked).is_equal_to(false);
        assert_that!(test_context.clients).has_length(1);
        assert_that!(test_context.past_transactions).has_length(1);
        assert_that!(test_context.disputed_transactions).has_length(0);
        Ok(())
    }
}
