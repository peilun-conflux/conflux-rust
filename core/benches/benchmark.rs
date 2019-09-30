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

const NUM_KEYS: usize = 100000;
const SQLITE_PATH: &str = "sqlite";
const ROCKSDB_PATH: &str = "rocksdb";

fn open_sqlite() -> KvdbSqlite<Box<[u8]>> {
    if let Err(e) = fs::create_dir_all(SQLITE_PATH) {
        panic!("Error creating database directory: {:?}", e);
    }
    KvdbSqlite::create_and_open(
        SQLITE_PATH.to_owned() + SQLITE_PATH,
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

fn sqlite_get_benchmark(c: &mut Criterion) {
    let kvdb_sqlite = open_sqlite();
    bench_kvdb(c, kvdb_sqlite);
}

fn rocksdb_get_benchmark(c: &mut Criterion) {
    let kvdb_rocksdb = open_rocksdb();
    bench_kvdb(c, kvdb_rocksdb);
}

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
    let mut keys = Vec::new();
    for i in 0..NUM_KEYS {
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
}

criterion_group!(benches, rocksdb_get_benchmark, sqlite_get_benchmark);
criterion_group!(setup, setup_sqlite, setup_rocksdb);
criterion_main!(setup, benches);
