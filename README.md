# Distributed mutually exclusive locks in MongoDB.
[![Crates.io](https://img.shields.io/crates/v/mongo_lock.svg)](https://crates.io/crates/mongo_lock) [![docs.rs](https://docs.rs/mongo_lock/badge.svg)](https://docs.rs/mongo_lock)


This crate contains only sync implementation.
If you need a async version, use [`mongo-lock-async`](https://crates.io/crates/mongo-lock-async) crate.

This implementation relies on system time. Ensure that NTP clients on your servers are configured properly.

## Installation
Add this crate to `Cargo.toml`

```toml
[dependencies]
mongo_lock = "0.2.0"
```

## Usage
```rust
fn main() {
    let mongo = mongodb::sync::Client::with_uri_str("mongodb://localhost").unwrap();

    // We need to ensure that mongodb collection has a proper index.
    mongo_lock::prepare_database(&mongo).unwrap();

    if let Ok(Some(lock)) =
        mongo_lock::Lock::try_acquire(
            &mongo,
            "my-key",
            std::time::Duration::from_secs(30)
        )
    {
        println!("Lock acquired.");

        // The lock will be released automatically after leaving the scope.
    }
}
```
