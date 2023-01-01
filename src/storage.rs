use lazy_static::lazy_static;
use sled::Db;

lazy_static! {
    static ref DB_HANDLE: Db = sled::open("./db/db").expect("failed to open db.");
}
