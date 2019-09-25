// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use cfx_bytes::Bytes;
use cfx_types::{U256, H256};
use cfxcore::{
    executive::Executive,
    machine::new_machine_with_builtin,
    state::State,
    statedb::StateDb,
    storage::state_manager::{SnapshotAndEpochIdRef, StateManagerTrait},
    vm::{Env, Spec},
    vm_factory::VmFactory,
};
use criterion::{criterion_group, criterion_main, Criterion};
use ethkey::{Generator, KeyPair, Random};
use parking_lot::{Condvar, Mutex};
use primitives::{Action, Transaction};
use std::sync::Arc;
use std::fs;
use std::path::Path;
use cfxcore::storage::{KvdbRocksdb, KeyValueDbTrait, KvdbSqlite};
use rand::{random, Rng, RngCore};
use cfxcore::storage::storage_db::KeyValueDbTraitRead;

const NUM_KEYS: usize = 10000;
fn sqlite_get_benchmark(c: &mut Criterion) {
    let kvdb_sqlite: KvdbSqlite<Box<[u8]>> = KvdbSqlite::create_and_open(
        "bench_sqlite",
        "blocks",
        &[&"value"],
        &[&"BLOB"],
        false,
    ).expect("Open sqlite failure");
    bench_kvdb(c, kvdb_sqlite);
}

fn rocksdb_get_benchmark(c: &mut Criterion) {
    let db_dir = "./bench_rocksdb";
    if let Err(e) = fs::create_dir_all(db_dir) {
        panic!("Error creating database directory: {:?}", e);
    }
    let db_config = db::db_config(&Path::new(db_dir), None,db::DatabaseCompactionProfile::default(), Some(1), false);
    let db = db::open_database(
        db_dir,
        &db_config,
    ).expect("rocksdb open failure");
    let kvdb_rocksdb = KvdbRocksdb {
        kvdb: db.key_value().clone(),
        col: Some(0),
    };
    bench_kvdb(c, kvdb_rocksdb);
}

fn bench_kvdb<T: 'static + KeyValueDbTrait<ValueType = Box<[u8]>>>(c: &mut Criterion, kvdb: T) {
    let mut keys = Vec::new();
    for i in 0..NUM_KEYS {
        let key: H256 = random();
        let value: Vec<u8> = if i % 2 == 0 {
            (0..300000).map(|_| { rand::random::<u8>() }).collect()
        } else {
            vec![0]
        };
        keys.push(key);
        kvdb.put(key.as_ref(), value.as_slice());
    }
    let keys_encoded = rlp::encode_list(&keys);
    kvdb.put(b"keys", &keys_encoded);
    c.bench_function("rocksdb::get one random key from 10000 keys", move |b| {
        let mut r = rand::thread_rng();
        b.iter(|| {
            let key = keys[r.gen_range(0, NUM_KEYS/2-1) as usize * 2 + 1];
            kvdb.get(key.as_ref());
        });
    });
}

criterion_group!(benches, rocksdb_get_benchmark, sqlite_get_benchmark);
criterion_main!(benches);
