#[cfg(feature = "async")]
pub mod r#async;
#[cfg(feature = "sync")]
pub mod sync;
mod util;

const COLLECTION_NAME: &str = "locks";
const DEFAULT_DB_NAME: &str = "mongo-lock";
