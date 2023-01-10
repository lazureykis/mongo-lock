#![doc(issue_tracker_base_url = "https://github.com/lazureykis/mongo-lock/issues")]

//! Distributed mutually exclusive locks in MongoDB.
//!
//! This crate contains only sync implementation.
//! If you need a async version, use [`mongo-lock-async`](https://crates.io/crates/mongo-lock-async) crate.
//!
//! This implementation relies on system time. Ensure that NTP clients on your servers are configured properly.
//!
//! Usage:
//! ```rust
//! fn main() {
//!     let mongo = mongodb::sync::Client::with_uri_str("mongodb://localhost").unwrap();
//!
//!     // We need to ensure that mongodb collection has a proper index.
//!     mongo_lock::prepare_database(&mongo).unwrap();
//!
//!     if let Ok(Some(lock)) =
//!         mongo_lock::Lock::try_acquire(
//!             &mongo,
//!             "my-key",
//!             std::time::Duration::from_secs(30)
//!         )
//!     {
//!         println!("Lock acquired.");
//!
//!         // The lock will be released automatically after leaving the scope.
//!     }
//! }
//! ```

mod util;

const COLLECTION_NAME: &str = "locks";
const DEFAULT_DB_NAME: &str = "mongo-lock";

use mongodb::bson::{doc, Document};
use mongodb::error::{Error, ErrorKind, WriteError, WriteFailure};
use mongodb::options::{IndexOptions, UpdateOptions};
use mongodb::sync::{Client, Collection};
use mongodb::IndexModel;
use std::time::Duration;

#[inline]
fn collection(mongo: &Client) -> Collection<Document> {
    mongo
        .default_database()
        .unwrap_or_else(|| mongo.database(DEFAULT_DB_NAME))
        .collection(COLLECTION_NAME)
}

/// Prepares MongoDB collection to store locks.
///
/// Creates TTL index to remove old records after they expire.
///
/// The [Lock] itself does not relies on this index,
/// because MongoDB can remove documents with some significant delay.
pub fn prepare_database(mongo: &Client) -> Result<(), Error> {
    let options = IndexOptions::builder()
        .expire_after(Some(Duration::from_secs(0)))
        .build();

    let model = IndexModel::builder()
        .keys(doc! {"expiresAt": 1})
        .options(options)
        .build();

    collection(mongo).create_index(model, None)?;

    Ok(())
}

/// Distributed mutex lock.
pub struct Lock {
    mongo: Client,
    id: String,
    acquired: bool,
}

impl Lock {
    /// Tries to acquire the lock with the given key.
    pub fn try_acquire(
        mongo: &Client,
        key: &str,
        ttl: Duration,
    ) -> Result<Option<Lock>, mongodb::error::Error> {
        let (now, expires_at) = util::now_and_expires_at(ttl);

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

        match collection(mongo).update_one(query, update, options) {
            Ok(result) => {
                if result.upserted_id.is_some() || result.modified_count == 1 {
                    Ok(Some(Lock {
                        mongo: mongo.clone(),
                        id: key.to_string(),
                        acquired: true,
                    }))
                } else {
                    Ok(None)
                }
            }
            Err(err) => {
                if let ErrorKind::Write(WriteFailure::WriteError(WriteError {
                    code: 11000, ..
                })) = *err.kind
                {
                    Ok(None)
                } else {
                    Err(err)
                }
            }
        }
    }

    fn release(&mut self) -> Result<bool, mongodb::error::Error> {
        if self.acquired {
            let result = collection(&self.mongo).delete_one(doc! {"_id": &self.id}, None)?;

            self.acquired = false;

            Ok(result.deleted_count == 1)
        } else {
            Ok(false)
        }
    }
}

impl Drop for Lock {
    fn drop(&mut self) {
        self.release().ok();
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn gen_random_key() -> String {
        use rand::{distributions::Alphanumeric, thread_rng, Rng};

        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect()
    }

    #[test]
    fn simple_locks() {
        let mongo = Client::with_uri_str("mongodb://localhost").unwrap();

        prepare_database(&mongo).unwrap();

        let key1 = gen_random_key();
        let key2 = gen_random_key();

        let lock1 = Lock::try_acquire(&mongo, &key1, Duration::from_secs(5)).unwrap();
        assert!(lock1.is_some());

        let lock1_dup = Lock::try_acquire(&mongo, &key1, Duration::from_secs(5)).unwrap();
        assert!(lock1_dup.is_none());

        let released1 = lock1.unwrap().release().unwrap();
        assert!(released1);

        let lock1 = Lock::try_acquire(&mongo, &key1, Duration::from_secs(5)).unwrap();
        assert!(lock1.is_some());

        let lock2 = Lock::try_acquire(&mongo, &key2, Duration::from_secs(5)).unwrap();
        assert!(lock2.is_some());

        lock1.unwrap().release().unwrap();
        lock2.unwrap().release().unwrap();
    }

    #[test]
    fn with_ttl() {
        let mongo = Client::with_uri_str("mongodb://localhost").unwrap();

        prepare_database(&mongo).unwrap();

        let key = gen_random_key();

        assert!(Lock::try_acquire(&mongo, &key, Duration::from_secs(1))
            .unwrap()
            .is_some());

        std::thread::sleep(Duration::from_secs(1));

        assert!(Lock::try_acquire(&mongo, &key, Duration::from_secs(1))
            .unwrap()
            .is_some());
    }

    #[test]
    fn dropped_locks() {
        let mongo = Client::with_uri_str("mongodb://localhost").unwrap();

        prepare_database(&mongo).unwrap();

        let key = gen_random_key();

        {
            assert!(Lock::try_acquire(&mongo, &key, Duration::from_secs(1))
                .unwrap()
                .is_some());
        }

        {
            assert!(Lock::try_acquire(&mongo, &key, Duration::from_secs(1))
                .unwrap()
                .is_some());
        }

        let lock1 = Lock::try_acquire(&mongo, &key, Duration::from_secs(1)).unwrap();
        let lock2 = Lock::try_acquire(&mongo, &key, Duration::from_secs(1)).unwrap();

        assert!(lock1.is_some());
        assert!(lock2.is_none());

        drop(lock1);

        let lock3 = Lock::try_acquire(&mongo, &key, Duration::from_secs(1)).unwrap();
        assert!(lock3.is_some());
    }
}
