use mongodb::bson::{doc, Document};
use mongodb::error::{Error, ErrorKind, WriteError, WriteFailure};
use mongodb::options::{IndexOptions, UpdateOptions};
use mongodb::{Client, IndexModel};
use std::ops::Add;
use std::time::{Duration, SystemTime};

const COLLECTION_NAME: &str = "locks";
const DEFAULT_DB_NAME: &str = "mongo-lock";

#[inline]
fn collection(mongo: &mongodb::Client) -> mongodb::Collection<Document> {
    mongo
        .default_database()
        .unwrap_or_else(|| mongo.database(DEFAULT_DB_NAME))
        .collection(COLLECTION_NAME)
}

pub async fn prepare_database(mongo: &mongodb::Client) -> Result<(), Error> {
    let options = IndexOptions::builder()
        .unique(true)
        // .partial_filter_expression(doc! { "expiresAt": 1 })
        .expire_after(Some(Duration::from_secs(0)))
        .build();

    let model = IndexModel::builder()
        .keys(doc! {"expiresAt": 1})
        .options(options)
        .build();

    collection(mongo).create_index(model, None).await?;

    Ok(())
}

const USE_UPSERT: bool = true;

pub async fn lock(mongo: &Client, key: &str, ttl: Duration) -> Result<bool, mongodb::error::Error> {
    let now = SystemTime::now();
    let expires_at = SystemTime::now().add(ttl);

    let now = mongodb::bson::DateTime::from_system_time(now);
    let expires_at = mongodb::bson::DateTime::from_system_time(expires_at);

    if USE_UPSERT {
        // Update expired locks if mongodb didn't clean it yet.
        let query = doc! {
            "_id": key,
            "expiresAt": {"$lte": now},
        };

        let update = doc! {
            "$set": {
                "expiresAt": expires_at,
            },
            "$setOnInsert": {
                "_id": key,
            },
        };

        let options = UpdateOptions::builder().upsert(true).build();

        match collection(mongo).update_one(query, update, options).await {
            Ok(result) => {
                dbg!(&result);
                Ok(result.upserted_id.is_some() || result.modified_count == 1)
            }
            Err(err) => {
                if let ErrorKind::Write(WriteFailure::WriteError(WriteError {
                    code: 11000, ..
                })) = *err.kind
                {
                    Ok(false)
                } else {
                    Err(err)
                }
            }
        }
    } else {
        let doc = doc! {
            "_id": key,
            "expiresAt": expires_at,
        };

        match collection(mongo).insert_one(doc, None).await {
            Ok(_) => Ok(true),
            Err(err) => {
                if let ErrorKind::Write(WriteFailure::WriteError(WriteError {
                    code: 11000, ..
                })) = *err.kind
                {
                    Ok(false)
                } else {
                    Err(err)
                }
            }
        }
    }
}

pub async fn release(mongo: &Client, key: &str) -> Result<bool, mongodb::error::Error> {
    let result = collection(mongo)
        .delete_one(doc! {"_id": key}, None)
        .await?;

    Ok(result.deleted_count == 1)
}

#[cfg(test)]
mod async_tests {
    use super::*;

    fn gen_random_key() -> String {
        use rand::{distributions::Alphanumeric, thread_rng, Rng};
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect()
    }

    #[tokio::test]
    async fn simple_locks() {
        let mongo = mongodb::Client::with_uri_str("mongodb://localhost")
            .await
            .unwrap();

        prepare_database(&mongo).await.unwrap();

        let key1 = gen_random_key();
        let key2 = gen_random_key();

        let locked1 = lock(&mongo, &key1, Duration::from_secs(5)).await.unwrap();
        assert!(locked1);

        let locked1 = lock(&mongo, &key1, Duration::from_secs(5)).await.unwrap();
        assert!(!locked1);

        let released1 = release(&mongo, &key1).await.unwrap();
        assert!(released1);

        let released1 = release(&mongo, &key1).await.unwrap();
        assert!(!released1);

        let locked1 = lock(&mongo, &key1, Duration::from_secs(5)).await.unwrap();
        assert!(locked1);

        let locked2 = lock(&mongo, &key2, Duration::from_secs(5)).await.unwrap();
        assert!(locked2);

        release(&mongo, &key1).await.unwrap();
        release(&mongo, &key2).await.unwrap();
    }

    #[tokio::test]
    async fn with_ttl() {
        let mongo = mongodb::Client::with_uri_str("mongodb://localhost")
            .await
            .unwrap();

        prepare_database(&mongo).await.unwrap();

        let key = gen_random_key();

        assert!(lock(&mongo, &key, Duration::from_secs(1)).await.unwrap());

        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(lock(&mongo, &key, Duration::from_secs(1)).await.unwrap());

        assert!(release(&mongo, &key).await.unwrap());
    }
}
