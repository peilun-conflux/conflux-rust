// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use cfx_types::{Address, H256};
use cfxcore::{
    db::NUM_COLUMNS,
    statedb::StateDb,
    storage::{
        state::StateTrait,
        state_manager::{
            StateManager, StateManagerTrait, StorageConfiguration,
        },
        StateIndex,
    },
    sync::{
        delta::{Chunk, ChunkReader, StateDumper},
        restore::Restorer,
        Error,
    },
};
use clap::{App, Arg, ArgMatches};
use primitives::{Account, MerkleHash, Transaction, StorageKey, SignedTransaction};
use rlp::Rlp;
use std::{cmp::min, fmt::Debug, fs::{create_dir_all, remove_dir_all}, path::{Path, PathBuf}, str::FromStr, sync::Arc, time::{Duration, Instant}, thread};
use std::collections::HashMap;
use rand::{thread_rng, Rng, RngCore};
use ethkey::{KeyPair, Random, Generator};
use cfx_bytes::Bytes;
use primitives::transaction::Action::Call;
use cfxcore::vm::{Spec, Env};
use cfxcore::machine::new_machine_with_builtin;
use cfxcore::executive::Executive;
use cfxcore::state::State;
use cfxcore::vm_factory::VmFactory;
use std::sync::mpsc::{channel, sync_channel};

const INITIAL_BALANCE: u128 = 1_000_000_000_000;
const TRANSFER_BALANCE: u128 = 21_000;
// cargo run --release -p cfxcore --example tx_bench
// cargo run --release -p cfxcore --example tx_bench -- --help
fn main() -> Result<(), Error> {
    let matches = parse_args();

    // setup test directory
    let test_dir: PathBuf = arg_val(&matches, "test-dir");
    if test_dir.exists() {
        remove_dir_all(&test_dir)?;
    }

    // setup node 1
    println!("====================================================");
    println!("Setup accounts ...");
    let cache_size = arg_val(&matches, "cache-size");
    let state_manager = new_state_manager(test_dir.join("db1").to_str().unwrap(), cache_size)?;
    let (genesis_hash, genesis_root) = initialize_genesis(&state_manager)?;
    let accounts = arg_val(&matches, "accounts");
    let accounts_per_epoch = arg_val(&matches, "accounts-per-epoch");
    let (checkpoint, checkpoint_root, account_vec) =
        prepare_checkpoint(&state_manager, genesis_hash, accounts, accounts_per_epoch)?;
    let total_n_tx = arg_val(&matches, "n-tx");
    let tx_per_epoch : usize = arg_val(&matches, "tx-per-epoch");
    let (tx_sender, tx_receiver) = sync_channel(2);

    let rounds: usize = arg_val(&matches, "rounds");
    thread::spawn(move || {
        let mut nonce_vec = vec![0 as usize; account_vec.len()];
        let mut balance_vec = vec![INITIAL_BALANCE; account_vec.len()];
        let mut rng = thread_rng();
        let thread_pool = threadpool::ThreadPool::new(8);
        for _ in 0..rounds {
            let mut n_tx = total_n_tx;
            let sign_start = Instant::now();
            while n_tx != 0 {
                let (epoch_tx_sender, epoch_tx_receiver) = channel();
                let n_tx_epoch = min(n_tx, tx_per_epoch);
                for i in 0..n_tx_epoch {
                    let sender_index = rng.next_u32() as usize % account_vec.len();
                    let receiver_index = rng.next_u32() as usize % account_vec.len();
                    let mut sender_nonce = nonce_vec.get_mut(sender_index).unwrap();
                    let tx = Transaction {
                        nonce: (*sender_nonce).into(),
                        gas: TRANSFER_BALANCE.into(),
                        gas_price: 1.into(),
                        action: Call(account_vec[receiver_index].address()),
                        value: TRANSFER_BALANCE.into(),
                        data: Bytes::new(),
                    };
                    *sender_nonce += 1;
                    balance_vec[sender_index] -= TRANSFER_BALANCE;
                    balance_vec[receiver_index] += TRANSFER_BALANCE - 21000;
                    let sender = epoch_tx_sender.clone();
                    let secret = account_vec[sender_index].secret().clone();
                    thread_pool.execute(move || {
                        let signed = tx.sign(&secret);
                        sender.send((i, signed));
                    });
                }
                let mut tx_vec: Vec<(usize, SignedTransaction)> = epoch_tx_receiver.iter().take(n_tx_epoch).collect();
                tx_vec.sort_by(|e1, e2| e1.0.partial_cmp(&e2.0).unwrap());
                let sorted_tx_vec: Vec<SignedTransaction> = tx_vec.iter().map(|e| e.1.clone()).collect();
                n_tx -= n_tx_epoch;
                tx_sender.send(Some(sorted_tx_vec));
            }
        }
        tx_sender.send(None);
    });


    let spec = Spec::new_spec();
    let machine = new_machine_with_builtin();
    let mut epoch = checkpoint;
    let mut env = Env::default();
    env.gas_limit = (tx_per_epoch * 21000).into();
    let start = Instant::now();
    let mut n_tx = total_n_tx;
    for tx_vec_opt in tx_receiver {
        if tx_vec_opt.is_none() {
            break;
        }
        let tx_vec = tx_vec_opt.unwrap();
        let state_index = StateIndex::new_for_test_only_delta_mpt(&epoch);
        let mut state = State::new(StateDb::new(state_manager.get_state_for_next_epoch(state_index).unwrap().unwrap()), 0.into(), VmFactory::default());
        let epoch_start = Instant::now();
        for signed_tx in &tx_vec {
            let mut nonce_increased = false;
            let r = Executive::new(&mut state, &env, &machine, &spec)
                .transact(signed_tx, &mut nonce_increased);
            assert!(r.is_ok() && nonce_increased == true);
        }
        n_tx -= tx_vec.len();
        let progress = (total_n_tx - n_tx) * 100 / total_n_tx;
        println!(
            "{} tx executed, progress = {}%, elapsed = {:?}",
            tx_vec.len(),
            progress,
            epoch_start.elapsed()
        );

        let commit_start = Instant::now();
        epoch = H256::random();
        state.commit(epoch);
        println!("commit_elapsed = {:?}", commit_start.elapsed());
    }
    println!("{} tx executed in total, elapsed = {:?} tps = {}", total_n_tx, start.elapsed(), total_n_tx as u64 / start.elapsed().as_secs());

    Ok(())
}

fn parse_args<'a>() -> ArgMatches<'a> {
    App::new("tx_bench")
        .arg(
            Arg::with_name("test-dir")
                .long("test-dir")
                .takes_value(true)
                .value_name("PATH")
                .help("Root directory for test")
                .default_value("tx_bench_dir"),
        )
        .arg(
            Arg::with_name("accounts")
                .long("accounts")
                .takes_value(true)
                .value_name("NUM")
                .help("Number of accounts in checkpoint")
                .default_value("1000000"),
        )
        .arg(
            Arg::with_name("accounts-per-epoch")
                .long("accounts-per-epoch")
                .takes_value(true)
                .value_name("NUM")
                .help("Number of accounts in each epoch")
                .default_value("10000"),
        )
        .arg(
            Arg::with_name("n-tx")
                .long("number-of-transactions")
                .takes_value(true)
                .value_name("NUM")
                .help("Number of transactions executed in total")
                .default_value("10000000")
        )
        .arg(
            Arg::with_name("tx-per-epoch")
                .long("tx-per-epoch")
                .takes_value(true)
                .help("Number of transaction executed in an epoch")
                .default_value("3000")
        )
        .arg(
            Arg::with_name("cache-size")
                .long("cache-size")
                .takes_value(true)
                .help("The number of cached node in storage")
                .default_value("20000000")
        )
        .arg(
            Arg::with_name("rounds")
                .long("rounds")
                .takes_value(true)
                .default_value("1")
        )
//        .arg(
//            Arg::with_name("node-map-size")
//                .long("node-map-size")
//                .takes_value(true)
//                .help("The number of node cache metadata kept in memory")
//                .default_value("200000")
//        )
        .get_matches()
}

fn arg_val<T>(matches: &ArgMatches, arg_name: &str) -> T
where
    T: FromStr,
    <T as FromStr>::Err: Debug,
{
    let val = matches.value_of(arg_name).unwrap();
    T::from_str(val).unwrap()
}

fn new_state_manager(db_dir: &str, cache_size: u32) -> Result<Arc<StateManager>, Error> {
    create_dir_all(db_dir)?;

    let db_config = db::db_config(
        Path::new(db_dir),
        Some(128),
        db::DatabaseCompactionProfile::default(),
        NUM_COLUMNS.clone(),
        false,
    );
    let db = db::open_database(db_dir, &db_config)?;
    let storage_conf = StorageConfiguration{
        cache_start_size: cache_size,
        cache_size: cache_size,
        idle_size: cache_size,
        node_map_size: cache_size * 4,
        recent_lfu_factor: 4.0,
    };

    Ok(Arc::new(StateManager::new(
        db,
        storage_conf,
    )))
}

fn initialize_genesis(
    manager: &StateManager,
) -> Result<(H256, MerkleHash), Error> {
    let mut state = manager.get_state_for_genesis_write();

//    state.set(
//        StorageKey::AccountKey(b"123"),
//        vec![1, 2, 3].into_boxed_slice(),
//    )?;
//    state.set(
//        StorageKey::AccountKey(b"124"),
//        vec![1, 2, 4].into_boxed_slice(),
//    )?;

    let root = state.compute_state_root()?;
    println!("genesis root: {:?}", root.state_root.delta_root);

    let genesis_hash = H256::from_str(
        "fa4e44bc69cca4cb2ae88a8fd452826faab9e8764e7eed934feede46c98962fa",
    )
    .unwrap();
    state.commit(genesis_hash.clone())?;

    Ok((genesis_hash, root.state_root.delta_root))
}

fn prepare_checkpoint(
    manager: &StateManager, parent: H256, accounts: usize,
    accounts_per_epoch: usize,
) -> Result<(H256, MerkleHash, Vec<KeyPair>), Error>
{
    println!("begin to add {} accounts for checkpoint ...", accounts);
    let start = Instant::now();
    let mut checkpoint = parent;
    let mut pending = accounts;
    let mut account_vec = Vec::with_capacity(accounts);
    while pending > 0 {
        let n = min(accounts_per_epoch, pending);
        let start2 = Instant::now();
        let mut r = add_epoch_with_accounts(manager, &checkpoint, n);
        checkpoint = r.0;
        account_vec.append(&mut r.1);
        pending -= n;
        let progress = (accounts - pending) * 100 / accounts;
        println!(
            "{} accounts committed, progress = {}%, elapsed = {:?}",
            n,
            progress,
            start2.elapsed()
        );
    }

    println!("all accounts added in {:?}", start.elapsed());

    let root = manager
        // TODO consider snapshot.
        .get_state_no_commit(StateIndex::new_for_test_only_delta_mpt(
            &checkpoint,
        ))?
        .unwrap()
        .get_state_root()?
        .unwrap()
        .state_root
        .delta_root;
    println!("checkpoint root: {:?}", root);

    Ok((checkpoint, root, account_vec))
}

fn add_epoch_with_accounts(
    manager: &StateManager, parent: &H256, accounts: usize,
) -> (H256, Vec<KeyPair>) {
    let mut new_accounts = Vec::with_capacity(accounts);
    let epoch_id = StateIndex::new_for_test_only_delta_mpt(parent);
    let state = manager.get_state_for_next_epoch(epoch_id).unwrap().unwrap();
    let mut state = StateDb::new(state);
    for i in 0..accounts {
        let key_pair: KeyPair = Random.generate().unwrap();
        let addr = key_pair.address();
        new_accounts.push(key_pair);
        let account =
            Account::new_empty_with_balance(&addr, &INITIAL_BALANCE.into(), &0.into());
        state
            .set(StorageKey::new_account_key(&addr), &account)
            .unwrap();
    }
    let epoch = H256::random();
    state.commit(epoch).unwrap();
    (epoch, new_accounts)
}
