// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use cfx_bytes::Bytes;
use cfx_types::{H256, U256};
use cfxcore::{
    executive::Executive,
    machine::new_machine_with_builtin,
    state::State,
    statedb::StateDb,
    storage::{
        state_manager::{SnapshotAndEpochIdRef, StateManagerTrait},
        storage_db::KeyValueDbTraitRead,
        KeyValueDbTrait, KvdbRocksdb, KvdbSqlite,
    },
    vm::{Env, Spec},
    vm_factory::VmFactory,
};
use criterion::{criterion_group, criterion_main, Criterion};
use ethkey::{Generator, KeyPair, Random};
use parking_lot::{Condvar, Mutex};
use primitives::{Action, Transaction};
use rand::{random, Rng, RngCore};
use std::{fs, path::Path, sync::Arc};
use sled::Db as SledDb;
use cfxcore::storage::Error;
use cfxcore::storage::storage_db::{KeyValueDbTypes, KeyValueDbTraitMultiReader, PutType};
use libbdb as libdb;
use libbdb::{Database as BdbDatabase};
use std::time::Instant;

const NUM_KEYS: usize = 100000;
const SQLITE_PATH: &str = "sqlite";
const ROCKSDB_PATH: &str = "rocksdb";
const SLED_PATH: &str = "sleddb";
const BDB_PATH: &str = "bdb";

struct KvdbSled {
    db: SledDb,
}

impl KeyValueDbTypes for KvdbSled {
    type ValueType = Box<[u8]>;
}

impl KeyValueDbTraitRead for KvdbSled {
    fn get(&self, key: &[u8]) -> Result<Option<Self::ValueType>, Error> {
        Ok(self.db.get(key).unwrap().map(|v| (*v).into()))
    }
}

impl KeyValueDbTraitMultiReader for KvdbSled {}

impl KeyValueDbTrait for KvdbSled {
    fn delete(&self, key: &[u8]) -> Result<Option<Option<Self::ValueType>>, Error> {
        unimplemented!()
    }

    fn put(&self, key: &[u8], value: &<Self::ValueType as PutType>::PutType) -> Result<Option<Option<Self::ValueType>>, Error> {
        let v = self.db.insert(key, value).unwrap();
        Ok(Some(v.map(|v| (*v).into())))
    }
}

struct KvdbBdb {
    db: BdbDatabase,
}

impl KeyValueDbTypes for KvdbBdb {
    type ValueType = Box<[u8]>;
}
impl KeyValueDbTraitMultiReader for KvdbBdb {}

impl KeyValueDbTraitRead for KvdbBdb {
    fn get(&self, key: &[u8]) -> Result<Option<Self::ValueType>, Error> {
        let mut mut_key = key.to_owned();
        Ok(self.db.get(None, &mut mut_key, libdb::flags::DB_NONE).unwrap().map(|v| (*v).into()))
    }
}

impl KeyValueDbTrait for KvdbBdb {
    fn delete(&self, key: &[u8]) -> Result<Option<Option<Self::ValueType>>, Error> {
        unimplemented!()
    }

    fn put(&self, key: &[u8], value: &<Self::ValueType as PutType>::PutType) -> Result<Option<Option<Self::ValueType>>, Error> {
        let mut mut_key = key.to_owned();
        let mut mut_value = value.to_owned();
        self.db.put(None, &mut mut_key, &mut mut_value, libdb::flags::DB_NONE).unwrap();
        Ok(None)
    }
}

fn open_bdb() -> KvdbBdb {
    if let Err(e) = fs::create_dir_all(BDB_PATH) {
        panic!("Error creating database directory: {:?}", e);
    }
    let env = libdb::EnvironmentBuilder::new()
        .home(BDB_PATH)
        .log_flags(libdb::flags::DB_LOG_AUTO_REMOVE)
        .flags(libdb::DB_CREATE | libdb::DB_INIT_MPOOL | libdb::DB_INIT_LOG | libdb::DB_INIT_TXN | libdb::DB_INIT_LOCK|libdb::DB_RECOVER|libdb::DB_THREAD)
        .open()
        .unwrap();

    let txn = env.txn(None, libdb::flags::DB_NONE).unwrap();

    let db = libdb::DatabaseBuilder::new()
        .environment(&env)
        .transaction(&txn)
        .file("db")
        .db_type(libdb::DbType::BTree)
        .flags(libdb::flags::DB_CREATE)
        .open()
        .unwrap();

    txn.commit(libdb::CommitType::Inherit).expect("Commit failed!");
    KvdbBdb {
        db
    }
}

fn open_sled() -> KvdbSled {
    KvdbSled {
        db: SledDb::open(SLED_PATH).unwrap()
    }
}

fn open_sqlite() -> KvdbSqlite<Box<[u8]>> {
    if let Err(e) = fs::create_dir_all(SQLITE_PATH) {
        panic!("Error creating database directory: {:?}", e);
    }
    KvdbSqlite::create_and_open(
        SQLITE_PATH.to_owned()+"/" + SQLITE_PATH,
        "blocks",
        &[&"value"],
        &[&"BLOB"],
        false,
    )
    .expect("Open sqlite failure")
}

fn open_rocksdb() -> KvdbRocksdb {
    if let Err(e) = fs::create_dir_all(ROCKSDB_PATH) {
        panic!("Error creating database directory: {:?}", e);
    }
    let db_config = db::db_config(
        &Path::new(ROCKSDB_PATH),
        None,
        db::DatabaseCompactionProfile::default(),
        Some(1),
        false,
    );
    let db =
        db::open_database(ROCKSDB_PATH, &db_config).expect("rocksdb open failure");
    KvdbRocksdb {
        kvdb: db.key_value().clone(),
        col: Some(0),
    }
}

fn bdb_get_benchmark(c: &mut Criterion) {
    let bdb = open_bdb();
    bench_kvdb(c, bdb);
}

fn sled_get_benchmark(c: &mut Criterion) {
    let sled = open_sled();
    bench_kvdb(c, sled);
}

fn sqlite_get_benchmark(c: &mut Criterion) {
    let kvdb_sqlite = open_sqlite();
    bench_kvdb(c, kvdb_sqlite);
}

fn rocksdb_get_benchmark(c: &mut Criterion) {
    let kvdb_rocksdb = open_rocksdb();
    bench_kvdb(c, kvdb_rocksdb);
}

fn setup_bdb(c: &mut Criterion) { setup_kvdb(c, open_bdb()) }

fn setup_sled(c: &mut Criterion) { setup_kvdb(c, open_sled()) }

fn setup_sqlite(c: &mut Criterion) { setup_kvdb(c, open_sqlite()) }

fn setup_rocksdb(c: &mut Criterion) { setup_kvdb(c, open_rocksdb()) }

fn bench_kvdb<T: 'static + KeyValueDbTrait<ValueType = Box<[u8]>>>(
    c: &mut Criterion, kvdb: T,
) {
    let mut r = rand::thread_rng();
    let keys: Vec<H256> = rlp::decode_list(
        &kvdb.get(b"keys").expect("failure on get").expect("no keys"),
    );
    c.bench_function(
        format!(
            "{:?}::get one random key from {} keys",
            std::any::type_name::<T>(),
            NUM_KEYS
        )
        .as_str(),
        move |b| {
            b.iter(|| {
                let key =
                    keys[r.gen_range(0, NUM_KEYS / 2 - 1) as usize * 2 ];
                let value = kvdb.get(key.as_ref()).unwrap().unwrap();
            });
        },
    );
}

fn setup_kvdb<T: 'static + KeyValueDbTrait<ValueType = Box<[u8]>>>(
    c: &mut Criterion, kvdb: T,
) {
    let start = Instant::now();
    let mut keys = Vec::new();
    for i in 0..NUM_KEYS {
        if i % 1000 == 0 {
            println!("{} keys inserted : {} seconds used", i, start.elapsed().as_secs_f32());
        }
        let key: H256 = random();
        let value: Vec<u8> = if i % 2 == 0 {
            (0..300000).map(|_| rand::random::<u8>()).collect()
        } else {
            vec![0]
        };
        keys.push(key);
        kvdb.put(key.as_ref(), value.as_slice());
    }
    let keys_encoded = rlp::encode_list(&keys);
    kvdb.put(b"keys", &keys_encoded);
    println!("All keys inserted: {} seconds used", start.elapsed().as_secs_f32());
}

criterion_group!(benches, bdb_get_benchmark);
criterion_group!(setup, setup_bdb);
criterion_main!(setup, benches);
