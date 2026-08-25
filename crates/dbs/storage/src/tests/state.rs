// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use crate::{storage_db::SnapshotDbManagerTrait, StorageConfiguration};
use fs_extra::dir::CopyOptions;
use primitives::EpochId;
use rand::random;
use std::{fs, path::Path};

lazy_static! {
    static ref ISOLATED_MPT_STATE_MANAGER_TEST_LOCK: parking_lot::Mutex<()> =
        parking_lot::Mutex::new(());
}

struct IsolatedMptRecoveryTestRoot {
    path: std::path::PathBuf,
}

impl IsolatedMptRecoveryTestRoot {
    fn new() -> Self {
        let path = std::env::temp_dir()
            .join(format!("conflux-isolated-mpt-repro-{}", random::<u64>()));
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    fn path(&self) -> &Path { &self.path }
}

impl Drop for IsolatedMptRecoveryTestRoot {
    fn drop(&mut self) { fs::remove_dir_all(&self.path).ok(); }
}

const ISOLATED_MPT_REPRO_SNAPSHOT_EPOCH_COUNT: u32 = 6;
const ISOLATED_MPT_REPRO_ERA_EPOCH_COUNT: u64 = 12;

fn isolated_mpt_repro_epoch_id(height: u64) -> H256 {
    let mut bytes = [0_u8; 32];
    bytes[0] = 1;
    bytes[24..].copy_from_slice(&height.to_be_bytes());
    H256::from(bytes)
}

fn isolated_mpt_repro_manager(data_dir: &Path) -> Arc<StateManager> {
    let mut storage_conf = StorageConfiguration::new_default(
        data_dir.to_str().unwrap(),
        ISOLATED_MPT_REPRO_SNAPSHOT_EPOCH_COUNT,
        ISOLATED_MPT_REPRO_ERA_EPOCH_COUNT,
    );
    storage_conf.use_isolated_db_for_mpt_table = true;
    storage_conf.delta_mpts_cache_size = 20_000_000;
    storage_conf.delta_mpts_cache_start_size = 1_000_000;
    storage_conf.delta_mpts_node_map_vec_size = 20_000_000;
    storage_conf.delta_mpts_slab_idle_size = 200_000;

    Arc::new(StateManager::new(storage_conf).unwrap())
}

fn wait_for_isolated_mpt_repro_snapshotting(state_manager: &StateManager) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !state_manager
        .get_storage_manager()
        .in_progress_snapshotting_tasks
        .read()
        .is_empty()
    {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for background snapshotting"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn commit_isolated_mpt_repro_epoch(
    state_manager: &Arc<StateManager>, parent_epoch_id: &EpochId,
    parent_state_root: &StateRootWithAuxInfo, height: u64,
) -> (EpochId, StateRootWithAuxInfo) {
    let mut state = state_manager
        .get_state_for_next_epoch(
            StateIndex::new_for_next_epoch(
                parent_epoch_id,
                parent_state_root,
                height - 1,
                ISOLATED_MPT_REPRO_SNAPSHOT_EPOCH_COUNT,
            ),
            false,
        )
        .unwrap()
        .unwrap_or_else(|| panic!("state unavailable at height {height}"));
    let key = format!("isolated-mpt-repro-key-{height}").into_bytes();
    state
        .set(
            StorageKey::AccountKey(&key).with_native_space(),
            height.to_be_bytes().to_vec().into(),
        )
        .unwrap();

    let epoch_id = isolated_mpt_repro_epoch_id(height);
    let state_root = state.compute_state_root().unwrap();
    state.commit(epoch_id).unwrap();
    wait_for_isolated_mpt_repro_snapshotting(state_manager);
    (epoch_id, state_root)
}

fn build_isolated_mpt_repro_chain(
    state_manager: &Arc<StateManager>, end_height: u64,
) -> Vec<(EpochId, StateRootWithAuxInfo)> {
    let mut genesis = state_manager.get_state_for_genesis_write();
    genesis
        .set(
            StorageKey::AccountKey(b"isolated-mpt-repro-key-0")
                .with_native_space(),
            0_u64.to_be_bytes().to_vec().into(),
        )
        .unwrap();
    let genesis_epoch_id = isolated_mpt_repro_epoch_id(0);
    let genesis_state_root = genesis.compute_state_root().unwrap();
    genesis.commit(genesis_epoch_id).unwrap();

    let mut chain = vec![(genesis_epoch_id, genesis_state_root)];
    for height in 1..=end_height {
        let (parent_epoch_id, parent_state_root) = chain.last().unwrap();
        chain.push(commit_isolated_mpt_repro_epoch(
            state_manager,
            parent_epoch_id,
            parent_state_root,
            height,
        ));
    }
    chain
}

fn continue_isolated_mpt_repro_chain(
    state_manager: &Arc<StateManager>,
    parent: &(EpochId, StateRootWithAuxInfo), start_height: u64,
    end_height: u64,
) -> Vec<(EpochId, StateRootWithAuxInfo)> {
    let mut chain =
        Vec::with_capacity((end_height - start_height + 1) as usize);
    let mut parent = parent.clone();
    for height in start_height..=end_height {
        parent = commit_isolated_mpt_repro_epoch(
            state_manager,
            &parent.0,
            &parent.1,
            height,
        );
        chain.push(parent.clone());
    }
    chain
}

fn copy_isolated_mpt_repro_data_dir(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).unwrap();
    let mut options = CopyOptions::new();
    options.content_only = true;
    fs_extra::dir::copy(source, destination, &options).unwrap();
}

// Regression test for the isolated-MPT recovery/skip sequence.
#[test]
fn isolated_mpt_recovery_rebuilds_retained_snapshots_in_order() {
    let _serial = ISOLATED_MPT_STATE_MANAGER_TEST_LOCK.lock();
    const BASE_HEIGHT: u64 = 20;
    const NEW_SNAPSHOT_HEIGHT: u64 = 24;
    const FIRST_ADOPTION_HEIGHT: u64 = 31;
    const RECOVERY_CHECKPOINT_HEIGHT: u64 = 12;

    let test_root = IsolatedMptRecoveryTestRoot::new();
    let base_dir = test_root.path().join("base");
    let canonical_dir = test_root.path().join("canonical");
    let recovered_dir = test_root.path().join("recovered");
    fs::create_dir_all(&base_dir).unwrap();

    let base_state_manager = isolated_mpt_repro_manager(&base_dir);
    let base_chain =
        build_isolated_mpt_repro_chain(&base_state_manager, BASE_HEIGHT);
    let base_tip = base_chain[BASE_HEIGHT as usize].clone();
    assert!(base_state_manager
        .get_storage_manager()
        .get_snapshot_info_at_epoch(&isolated_mpt_repro_epoch_id(18))
        .is_some());
    drop(base_state_manager);

    copy_isolated_mpt_repro_data_dir(&base_dir, &canonical_dir);
    copy_isolated_mpt_repro_data_dir(&base_dir, &recovered_dir);

    let canonical_state_manager = isolated_mpt_repro_manager(&canonical_dir);
    let canonical_retained_snapshot_root = canonical_state_manager
        .get_storage_manager()
        .get_snapshot_info_at_epoch(&isolated_mpt_repro_epoch_id(18))
        .unwrap()
        .merkle_root;
    let canonical_chain = continue_isolated_mpt_repro_chain(
        &canonical_state_manager,
        &base_tip,
        BASE_HEIGHT + 1,
        FIRST_ADOPTION_HEIGHT,
    );
    let canonical_snapshot_root = canonical_state_manager
        .get_storage_manager()
        .get_snapshot_info_at_epoch(&isolated_mpt_repro_epoch_id(
            NEW_SNAPSHOT_HEIGHT,
        ))
        .unwrap()
        .merkle_root;
    drop(canonical_state_manager);

    let recovered_state_manager = isolated_mpt_repro_manager(&recovered_dir);
    let recovered_retained_snapshot_info_before = recovered_state_manager
        .get_storage_manager()
        .get_snapshot_info_at_epoch(&isolated_mpt_repro_epoch_id(18))
        .unwrap();
    let checkpoint_epoch_id =
        isolated_mpt_repro_epoch_id(RECOVERY_CHECKPOINT_HEIGHT);
    let snapshot_db_manager = recovered_state_manager
        .get_storage_manager()
        .get_snapshot_manager()
        .get_snapshot_db_manager();
    snapshot_db_manager.update_latest_snapshot_id(
        checkpoint_epoch_id,
        RECOVERY_CHECKPOINT_HEIGHT,
    );
    snapshot_db_manager
        .recovery_latest_mpt_snapshot_from_checkpoint(
            &checkpoint_epoch_id,
            None,
        )
        .unwrap();

    let recovered_chain = continue_isolated_mpt_repro_chain(
        &recovered_state_manager,
        &base_tip,
        BASE_HEIGHT + 1,
        FIRST_ADOPTION_HEIGHT,
    );
    let recovered_retained_snapshot_info_after_replay = recovered_state_manager
        .get_storage_manager()
        .get_snapshot_info_at_epoch(&isolated_mpt_repro_epoch_id(18))
        .unwrap();
    assert_eq!(
        canonical_retained_snapshot_root,
        recovered_retained_snapshot_info_after_replay.merkle_root
    );
    assert_eq!(
        recovered_retained_snapshot_info_before.merkle_root,
        recovered_retained_snapshot_info_after_replay.merkle_root
    );
    assert_eq!(
        recovered_retained_snapshot_info_before.height,
        recovered_retained_snapshot_info_after_replay.height
    );
    assert_eq!(
        recovered_retained_snapshot_info_before.parent_snapshot_height,
        recovered_retained_snapshot_info_after_replay.parent_snapshot_height
    );
    assert_eq!(
        recovered_retained_snapshot_info_before.parent_snapshot_epoch_id,
        recovered_retained_snapshot_info_after_replay.parent_snapshot_epoch_id
    );
    assert_eq!(
        recovered_retained_snapshot_info_before.pivot_chain_parts,
        recovered_retained_snapshot_info_after_replay.pivot_chain_parts
    );
    assert_eq!(
        recovered_retained_snapshot_info_before.serve_one_step_sync,
        recovered_retained_snapshot_info_after_replay.serve_one_step_sync
    );
    assert_eq!(
        recovered_retained_snapshot_info_before
            .snapshot_info_kept_to_provide_sync,
        recovered_retained_snapshot_info_after_replay
            .snapshot_info_kept_to_provide_sync,
    );
    let recovered_snapshot_root = recovered_state_manager
        .get_storage_manager()
        .get_snapshot_info_at_epoch(&isolated_mpt_repro_epoch_id(
            NEW_SNAPSHOT_HEIGHT,
        ))
        .unwrap()
        .merkle_root;

    for (canonical, recovered) in canonical_chain
        .iter()
        .zip(&recovered_chain)
        .take((FIRST_ADOPTION_HEIGHT - BASE_HEIGHT - 1) as usize)
    {
        assert_eq!(canonical.1, recovered.1);
    }
    assert_eq!(
        canonical_snapshot_root, recovered_snapshot_root,
        "retained-snapshot recovery must reproduce the canonical H24 root",
    );

    let canonical_adopted_root = &canonical_chain.last().unwrap().1;
    let recovered_adopted_root = &recovered_chain.last().unwrap().1;
    assert_eq!(
        canonical_adopted_root.state_root.snapshot_root,
        canonical_snapshot_root
    );
    assert_eq!(
        recovered_adopted_root.state_root.snapshot_root,
        recovered_snapshot_root
    );
    assert_eq!(
        canonical_adopted_root, recovered_adopted_root,
        "the first state adopting the rebuilt snapshot must stay canonical",
    );

    println!(
        "isolated-MPT recovery gap reproduced: canonical H24 snapshot root={:?}, recovered H24 snapshot root={:?}, canonical H31 state root={:?}, recovered H31 state root={:?}",
        canonical_snapshot_root,
        recovered_snapshot_root,
        canonical_adopted_root.aux_info.state_root_hash,
        recovered_adopted_root.aux_info.state_root_hash,
    );

    drop(recovered_state_manager);
}

#[test]
fn test_empty_genesis_block() {
    let state_manager = new_state_manager_for_unit_test();

    let mut genesis_epoch_id = H256::default();
    genesis_epoch_id.as_bytes_mut()[0] = 1;
    {
        let mut genesis_state = state_manager.get_state_for_genesis_write();
        genesis_state.compute_state_root().unwrap();

        genesis_state.commit(genesis_epoch_id).unwrap();
    }

    state_manager
        .get_state_trees(
            &StateIndex::new_for_test_only_delta_mpt(&genesis_epoch_id),
            /* try_open = */ false,
            true,
        )
        .unwrap();
}

#[test]
fn test_set_get() {
    let mut rng = get_rng_for_test();
    let state_manager = new_state_manager_for_unit_test();
    let mut state = state_manager.get_state_for_genesis_write();
    let mut keys: Vec<Vec<u8>> = generate_keys(TEST_NUMBER_OF_KEYS)
        .iter()
        .filter(|_| rng.random_bool(0.5))
        .cloned()
        .collect();

    println!("Testing with {} set operations.", keys.len());

    for key in &keys {
        state
            .set(
                StorageKey::AccountKey(key).with_native_space(),
                key[..].into(),
            )
            .expect("Failed to insert key.");
    }

    keys.shuffle(&mut rng);

    for key in &keys {
        let value = state
            .get(StorageKey::AccountKey(key).with_native_space())
            .expect("Failed to get key.")
            .expect("Failed to get key");
        let equal = (&**key).eq(value.as_ref());
        assert_eq!(equal, true);
    }

    let mut epoch_id = H256::default();
    epoch_id.as_bytes_mut()[0] = 1;
    state.compute_state_root().unwrap();
    state.commit(epoch_id).unwrap();
}

#[test]
fn test_get_set_at_second_commit() {
    let state_manager = new_state_manager_for_unit_test();
    let keys: Vec<Vec<u8>> = generate_keys(TEST_NUMBER_OF_KEYS);
    let set_size = TEST_NUMBER_OF_KEYS / 10;
    let (keys_0, keys_1_new, keys_remain, keys_1_overwritten) = (
        &keys[0..set_size * 2],
        &keys[set_size * 2..set_size * 3],
        &keys[0..set_size],
        &keys[set_size..set_size * 2],
    );

    let mut state_0 = state_manager.get_state_for_genesis_write();
    println!("Setting state_0 with {} keys.", keys_0.len());

    for key in keys_0 {
        state_0
            .set(
                StorageKey::AccountKey(key).with_native_space(),
                key[..].into(),
            )
            .expect("Failed to insert key.");
    }

    let mut epoch_id_0 = H256::default();
    epoch_id_0.as_bytes_mut()[0] = 1;
    state_0.compute_state_root().unwrap();
    state_0.commit(epoch_id_0).unwrap();

    let mut state_1 = state_manager
        .get_state_for_next_epoch(
            StateIndex::new_for_test_only_delta_mpt(&epoch_id_0),
            false,
        )
        .unwrap()
        .unwrap();
    println!("Set new {} keys for state_1.", keys_1_new.len());
    for key in keys_1_new {
        let value = vec![&key[..], &key[..]].concat();
        state_1
            .set(
                StorageKey::AccountKey(key).with_native_space(),
                value.into(),
            )
            .expect("Failed to insert key.");
    }

    println!(
        "Reading overlapping {} keys from state_0 and set new keys for state_1.",
        keys_1_overwritten.len(),
    );
    for key in keys_1_overwritten {
        let old_value = state_1
            .get(StorageKey::AccountKey(key).with_native_space())
            .expect("Failed to get key.")
            .expect("Failed to get key");
        let equal = (&**key).eq(old_value.as_ref());
        assert_eq!(equal, true);
        let value = vec![&key[..], &key[..]].concat();
        state_1
            .set(
                StorageKey::AccountKey(key).with_native_space(),
                value.into(),
            )
            .expect("Failed to insert key.");
    }

    println!(
        "Reading untouched {} keys from state_0 in state_1.",
        keys_remain.len(),
    );
    for key in keys_remain {
        let value = state_1
            .get(StorageKey::AccountKey(key).with_native_space())
            .expect("Failed to get key.")
            .expect("Failed to get key");
        let equal = (&**key).eq(value.as_ref());
        assert_eq!(equal, true);
    }

    println!(
        "Reading modified {} keys in state_1.",
        keys_1_overwritten.len(),
    );
    for key in keys_1_overwritten {
        let value = state_1
            .get(StorageKey::AccountKey(key).with_native_space())
            .expect("Failed to get key.")
            .expect("Failed to get key");
        let expected_value = vec![&key[..], &key[..]].concat();
        let equal = expected_value.eq(&value.as_ref());
        assert_eq!(equal, true);
    }

    let mut epoch_id_1 = H256::default();
    epoch_id_1.as_bytes_mut()[0] = 2;
    state_1.compute_state_root().unwrap();
    state_1.commit(epoch_id_1).unwrap();
}

#[test]
fn test_snapshot_random_read_performance() {
    let state_manager = new_state_manager_for_unit_test();
    let keys: Vec<Vec<u8>> = generate_keys(TEST_NUMBER_OF_KEYS);

    const EPOCHS: u8 = 20;
    println!(
        "Build {} epochs for testing, 10 epochs a snapshot.",
        2 * EPOCHS
    );
    let mut rng = get_rng_for_test();
    let range = Uniform::new(0, keys.len()).unwrap();
    const TXS: u32 = 20000;
    let mut epoch_keys = Vec::with_capacity(EPOCHS as usize * 2);
    for _epoch in 0..EPOCHS * 2 {
        let mut e_keys = Vec::with_capacity(TXS as usize * 2);
        for _key_idx in 0..((TXS as i32) * 2) {
            e_keys.push(keys[range.sample(&mut rng)].as_slice());
        }
        epoch_keys.push(e_keys);
    }

    println!("Initializing {} accounts", keys.len());
    const DEFAULT_BALANCE: u64 = 1_000_000_000;
    let mut state_0 = state_manager.get_state_for_genesis_write();
    for key in &keys {
        let mut address = Address::from_slice(
            &[&**key; 4].concat()[0..StorageKeyWithSpace::ACCOUNT_BYTES],
        );
        address.set_user_account_type_bits();
        let address_space = address.with_native_space();
        let account = Account::new_empty_with_balance(
            &address_space,
            &DEFAULT_BALANCE.into(),
            &0.into(),
        );
        let account_key =
            StorageKey::new_account_key(&address).with_native_space();
        state_0
            .set(account_key, rlp::encode(&account).to_vec().into())
            .expect("Failed to set key");
    }

    let epoch_id_0 = H256::default();
    let mut state_root = state_0.compute_state_root().unwrap();
    state_0.commit(epoch_id_0).unwrap();

    println!("Committing initial {} epochs.", EPOCHS);

    for epoch in 0..EPOCHS {
        state_root = simulate_transactions(
            epoch,
            &state_root,
            &epoch_keys[epoch as usize],
            &state_manager,
            &mut 0,
            &mut 0,
            &mut 0,
            &mut 0,
        );
    }

    println!(
        "Benchmarking last {} epochs with {} transactions",
        EPOCHS,
        EPOCHS as u32 * TXS
    );
    let mut load_ms = 0;
    let mut update_ms = 0;
    let mut write_ms = 0;
    let mut commit_ms = 0;
    for epoch in EPOCHS..EPOCHS * 2 {
        state_root = simulate_transactions(
            epoch,
            &state_root,
            &epoch_keys[epoch as usize],
            &state_manager,
            &mut load_ms,
            &mut update_ms,
            &mut write_ms,
            &mut commit_ms,
        );
    }
    let total_ms = (load_ms + update_ms + write_ms + commit_ms) as f64;
    println!(
        "Benchmark finished, TPS = {}, \
         load = {:.2}%, rlp_and_update = {:.2}%, write = {:.2}%, commit = {:.2}%",
        1000.0 * (TXS as f64) * (EPOCHS as f64) / total_ms,
        100.0 * (load_ms as f64) / total_ms,
        100.0 * (update_ms as f64) / total_ms,
        100.0 * (write_ms as f64) / total_ms,
        100.0 * (commit_ms as f64) / total_ms,
    );
}

fn simulate_transactions(
    epoch: u8, prev_state_root: &StateRootWithAuxInfo, keys: &[&[u8]],
    state_manager: &FakeStateManager, read_ms: &mut u32, update_ms: &mut u32,
    write_ms: &mut u32, commit_ms: &mut u32,
) -> StateRootWithAuxInfo {
    // Wait for snapshotting to complete. We don't calculate the time spent in
    // making snapshot.
    while state_manager
        .get_storage_manager()
        .in_progress_snapshotting_tasks
        .read()
        .len()
        != 0
    {
        thread::sleep(Duration::from_secs(1));
    }

    let mut addresses = Vec::with_capacity(keys.len());
    for key in keys {
        let mut address = Address::from_slice(
            &[&**key; 4].concat()[0..StorageKeyWithSpace::ACCOUNT_BYTES],
        );
        address.set_user_account_type_bits();
        addresses.push(address);
    }

    let mut epoch_id = H256::default();
    epoch_id.as_bytes_mut()[0] = epoch;
    let mut state = state_manager
        .get_state_for_next_epoch(
            StateIndex::new_for_next_epoch(
                &epoch_id,
                prev_state_root,
                epoch as u64 + 1,
                state_manager
                    .get_storage_manager()
                    .get_snapshot_epoch_count(),
            ),
            false,
        )
        .unwrap()
        .unwrap();
    let mut values = vec![None; keys.len()];

    let len = keys.len();

    // Load all values.
    let now = Instant::now();
    const PREFETCH: bool = true;
    if PREFETCH {
        const THREADS: usize = 8;
        let mut join_handles = vec![];
        for thread in 0..THREADS {
            let range_start = len * thread / THREADS;
            let range_end = len * (thread + 1) / THREADS;
            let addresses_range = unsafe {
                std::mem::transmute::<&[Address], &'static [Address]>(
                    &addresses[range_start..range_end],
                )
            };
            let value_range = unsafe {
                std::mem::transmute::<
                    &mut [Option<Box<[u8]>>],
                    &'static mut [Option<Box<[u8]>>],
                >(&mut values[range_start..range_end])
            };
            let state_r = unsafe {
                std::mem::transmute::<
                    &Box<dyn StateTrait>,
                    &'static Box<dyn StateTrait>,
                >(&state)
            };
            join_handles.push(thread::spawn(move || {
                let mut i = 0;
                for address in addresses_range {
                    value_range[i] = Some(
                        state_r
                            .get(
                                StorageKey::new_account_key(&address)
                                    .with_native_space(),
                            )
                            .expect("Failed to get key.")
                            .expect("no such key"),
                    );

                    i += 1;
                }
            }));
        }
        for join_handle in join_handles {
            join_handle.join().unwrap();
        }
    } else {
        let mut i = 0;
        for address in &addresses {
            values[i] = Some(
                state
                    .get(
                        StorageKey::new_account_key(&address)
                            .with_native_space(),
                    )
                    .expect("Failed to get key.")
                    .expect("no such key"),
            );

            i += 1;
        }
    }
    *read_ms += now.elapsed().as_millis() as u32;

    // Update accounts.
    let now = Instant::now();
    for i in 0..len {
        let mut account: primitives::Account = Account::new_from_rlp(
            addresses[i],
            &Rlp::new(&values[i].as_ref().unwrap()),
        )
        .expect("failed to decode rlp");
        if i % 2 == 0 {
            account.balance -= U256::one();
        } else {
            account.balance += U256::one();
        }
        values[i] = Some(rlp::encode(&account).to_vec().into());
    }
    *update_ms += now.elapsed().as_millis() as u32;

    // Write accounts.
    let now = Instant::now();
    for i in 0..len {
        let key = keys[i];
        let mut address = Address::from_slice(
            &[key; 4].concat()[0..StorageKeyWithSpace::ACCOUNT_BYTES],
        );
        address.set_user_account_type_bits();
        let account_key =
            StorageKey::new_account_key(&address).with_native_space();

        state
            .set(account_key, values[i].take().unwrap())
            .expect("Failed to set key");
    }
    *write_ms += now.elapsed().as_millis() as u32;

    // Commit.
    let now = Instant::now();
    epoch_id.as_bytes_mut()[0] = epoch + 1;
    let state_root = state.compute_state_root().unwrap();
    state.commit(epoch_id).unwrap();
    *commit_ms += now.elapsed().as_millis() as u32;

    state_root
}

#[test]
fn test_set_delete() {
    let mut rng = get_rng_for_test();
    let state_manager = new_state_manager_for_unit_test();

    let mut state = state_manager.get_state_for_genesis_write();

    let mut keys: Vec<Vec<u8>> = generate_keys(TEST_NUMBER_OF_KEYS);
    let (keys_0, keys_1) = (
        &keys[0..TEST_NUMBER_OF_KEYS / 2],
        &keys[TEST_NUMBER_OF_KEYS / 2..],
    );

    println!("Testing with {} set operations.", keys.len());

    // Insert part 1 and commit.
    for key in keys_0.iter() {
        state
            .set(
                StorageKey::AccountKey(key).with_native_space(),
                key[..].into(),
            )
            .expect("Failed to insert key.");
    }
    let mut epoch_id = H256::default();
    epoch_id.as_bytes_mut()[0] = 1;
    state.compute_state_root().unwrap();
    state.commit(epoch_id).unwrap();

    // In second state, insert part 2, then delete everything.
    let mut state = state_manager
        .get_state_for_next_epoch(
            StateIndex::new_for_test_only_delta_mpt(&epoch_id),
            false,
        )
        .unwrap()
        .unwrap();
    for key in keys_1.iter() {
        state
            .set(
                StorageKey::AccountKey(key).with_native_space(),
                key[..].into(),
            )
            .expect("Failed to insert key.");
    }

    keys.shuffle(&mut rng);

    println!("Testing with {} delete operations.", keys.len());
    for key in &keys {
        let value = state
            .delete_test_only(StorageKey::AccountKey(key).with_native_space())
            .expect("Failed to delete key.")
            .expect("Failed to get key");
        let equal = (&**key).eq(value.as_ref());
        assert_eq!(equal, true);
    }

    let mut epoch_id = H256::default();
    epoch_id.as_bytes_mut()[0] = 2;
    state.compute_state_root().unwrap();
    state.commit(epoch_id).unwrap();
}

#[test]
fn test_set_delete_all() {
    let mut rng = get_rng_for_test();
    let state_manager = new_state_manager_for_unit_test();

    let mut state = state_manager.get_state_for_genesis_write();
    let empty_state_root = state.compute_state_root().unwrap();

    let mut keys: Vec<Vec<u8>> = generate_keys(TEST_NUMBER_OF_KEYS);
    let (keys_0, keys_1) = (
        &keys[0..TEST_NUMBER_OF_KEYS / 2],
        &keys[TEST_NUMBER_OF_KEYS / 2..],
    );

    println!("Testing with {} set operations.", keys.len());

    // Insert part 1 and commit.
    for key in keys_0.iter() {
        state
            .set(
                StorageKey::AccountKey(
                    vec![&key[..], &key[..]].concat().as_slice(),
                )
                .with_native_space(),
                key[..].into(),
            )
            .expect("Failed to insert key.");
    }
    let mut epoch_id = H256::default();
    epoch_id.as_bytes_mut()[0] = 1;
    state.compute_state_root().unwrap();
    state.commit(epoch_id).unwrap();

    // In second state, insert part 2, then delete everything.
    let mut state = state_manager
        .get_state_for_next_epoch(
            StateIndex::new_for_test_only_delta_mpt(&epoch_id),
            false,
        )
        .unwrap()
        .unwrap();
    for key in keys_1.iter() {
        state
            .set(
                StorageKey::AccountKey(
                    vec![&key[..], &key[..]].concat().as_slice(),
                )
                .with_native_space(),
                key[..].into(),
            )
            .expect("Failed to insert key.");
    }

    keys.shuffle(&mut rng);

    println!("Testing with {} delete_all operations.", keys.len());
    let mut values = Vec::with_capacity(keys.len());
    for key in &keys {
        let key_prefix = &key[0..(2 + rng.random_range(0..2))];

        let value = state
            .delete_all(StorageKey::AccountKey(key_prefix).with_native_space())
            .expect("Failed to delete key.");
        if value.is_none() {
            continue;
        }
        let mut value = value.unwrap();
        for (deleted_key, deleted_value) in &value {
            assert_eq!(key_prefix, &deleted_key[0..key_prefix.len()]);
            assert_eq!(deleted_key, &vec![deleted_value.as_ref(); 2].concat());
        }

        for item in value.drain(..) {
            values.push(item);
        }

        let value = state
            .delete_all(StorageKey::AccountKey(key).with_native_space())
            .expect("Failed to delete key.");
        assert_eq!(value, None);
    }

    let mut epoch_id = H256::default();
    epoch_id.as_bytes_mut()[0] = 2;
    let state_root = state.compute_state_root().unwrap();
    state.commit(epoch_id).unwrap();

    assert_eq!(values.len(), keys.len());
    assert_eq!(state_root, empty_state_root);
}

#[test]
fn test_set_order() {
    let mut rng = get_rng_for_test();
    let state_manager = new_state_manager_for_unit_test();
    let keys: Vec<Vec<u8>> = generate_keys(500000)
        .iter()
        .filter(|_| rng.random_bool(0.5))
        .cloned()
        .collect();

    let mut epoch_id = H256::default();
    let mut state_0 = state_manager.get_state_for_genesis_write();
    println!("Setting state_0 with {} keys.", keys.len());
    for key in &keys {
        let key_slice = &key[..];
        let actual_key = vec![key_slice; 3].concat();
        let actual_value = vec![key_slice; 1 + (key[0] % 21) as usize].concat();
        state_0
            .set(
                StorageKey::AccountKey(&actual_key).with_native_space(),
                actual_value.into(),
            )
            .expect("Failed to insert key.");
    }
    let _merkle_0 = state_0.compute_state_root().unwrap();
    epoch_id.as_bytes_mut()[0] = 1;
    state_0.commit(epoch_id).unwrap();

    let mut state_1 = state_manager.get_state_for_genesis_write();
    println!("Setting state_1 with {} keys.", keys.len());
    for key in &keys {
        let key_slice = &key[..];
        let actual_key = vec![key_slice; 3].concat();
        let actual_value = vec![key_slice; 1 + (key[0] % 32) as usize].concat();
        state_1
            .set(
                StorageKey::AccountKey(&actual_key).with_native_space(),
                actual_value.into(),
            )
            .expect("Failed to insert key.");
    }
    let merkle_1 = state_1.compute_state_root().unwrap();
    epoch_id.as_bytes_mut()[0] = 2;
    state_1.commit(epoch_id).unwrap();

    let mut state_2 = state_manager.get_state_for_genesis_write();
    println!("Setting state_2 with {} keys.", keys.len());
    for key in keys.iter().rev() {
        let key_slice = &key[..];
        let actual_key = vec![key_slice; 3].concat();
        let actual_value = vec![key_slice; 1 + (key[0] % 32) as usize].concat();
        state_2
            .set(
                StorageKey::AccountKey(&actual_key).with_native_space(),
                actual_value.into(),
            )
            .expect("Failed to insert key.");
    }
    let merkle_2 = state_2.compute_state_root().unwrap();
    epoch_id.as_bytes_mut()[0] = 3;
    state_2.commit(epoch_id).unwrap();

    assert_eq!(merkle_1, merkle_2);
}

#[test]
fn test_set_order_concurrent() {
    let mut rng = get_rng_for_test();
    let state_manager = new_state_manager_for_unit_test();
    let keys = Arc::new(
        generate_keys(TEST_NUMBER_OF_KEYS / 10)
            .iter()
            .filter(|_| rng.random_bool(0.5))
            .cloned()
            .collect::<Vec<_>>(),
    );

    let mut epoch_id = H256::default();
    let mut state_0 = state_manager.get_state_for_genesis_write();
    println!("Setting state_0 with {} keys.", keys.len());
    for key in keys.iter() {
        let key_slice = &key[..];
        let actual_key = vec![key_slice; 3].concat();
        let actual_value = vec![key_slice; 1 + (key[0] % 21) as usize].concat();
        state_0
            .set(
                StorageKey::AccountKey(&actual_key).with_native_space(),
                actual_value.into(),
            )
            .expect("Failed to insert key.");
    }
    let _merkle_0 = state_0.compute_state_root().unwrap();
    epoch_id.as_bytes_mut()[0] = 1;
    state_0.commit(epoch_id).unwrap();

    let parent_epoch_0 = epoch_id;

    let mut state_1 = state_manager
        .get_state_for_next_epoch(
            StateIndex::new_for_test_only_delta_mpt(&parent_epoch_0),
            false,
        )
        .unwrap()
        .unwrap();
    println!("Setting state_1 with {} keys.", keys.len());
    for key in keys.iter() {
        let key_slice = &key[..];
        let actual_key = vec![key_slice; 3].concat();
        let actual_value = vec![key_slice; 1 + (key[0] % 32) as usize].concat();
        state_1
            .set(
                StorageKey::AccountKey(&actual_key).with_native_space(),
                actual_value.into(),
            )
            .expect("Failed to insert key.");
    }
    let merkle_1 = state_1.compute_state_root().unwrap();
    epoch_id.as_bytes_mut()[0] = 2;
    state_1.commit(epoch_id).unwrap();

    let thread_count = if cfg!(debug_assertions) {
        // Debug build. Fewer threads.
        10
    } else {
        // Release build.
        500
    };
    let mut threads = Vec::with_capacity(thread_count);
    for thread_id in 0..thread_count {
        thread::sleep(Duration::from_millis(30));
        let keys = keys.clone();
        let state_manager = state_manager.clone();
        let merkle_1 = merkle_1.clone();
        threads.push(thread::spawn(move || {
            let mut state_2 = state_manager
                .get_state_for_next_epoch(
                    StateIndex::new_for_test_only_delta_mpt(&parent_epoch_0),
                    false,
                )
                .unwrap()
                .unwrap();
            //            println!(
            //                "Setting state_{} with {} keys.",
            //                2 + thread_id,
            //                keys.len()
            //            );
            for key in keys.iter().rev() {
                let key_slice = &key[..];
                let actual_key = vec![key_slice; 3].concat();
                let actual_value =
                    vec![key_slice; 1 + (key[0] % 32) as usize].concat();
                state_2
                    .set(
                        StorageKey::AccountKey(&actual_key).with_native_space(),
                        actual_value.into(),
                    )
                    .expect("Failed to insert key.");
            }
            let merkle_2 = state_2.compute_state_root().unwrap();
            epoch_id.as_bytes_mut()[0] = ((3 + thread_id) % 256) as u8;
            epoch_id.as_bytes_mut()[1] = ((3 + thread_id) / 256) as u8;
            state_2.commit(epoch_id).unwrap();

            assert_eq!(merkle_1, merkle_2);
        }));
    }
    {
        let mut thread_id = 0;
        for thread in threads.drain(..) {
            thread
                .join()
                .expect(&format!("Thread {} failed.", thread_id));
            thread_id += 1;
        }
    }
}

use crate::{
    state::*,
    state_manager::*,
    tests::{
        generate_keys, get_rng_for_test, new_state_manager_for_unit_test,
        FakeStateManager, TEST_NUMBER_OF_KEYS,
    },
    StateRootWithAuxInfo,
};
use cfx_types::{
    address_util::AddressUtil, Address, AddressSpaceUtil, H256, U256,
};
use primitives::{Account, StorageKey, StorageKeyWithSpace};
use rand::{
    distr::{Distribution, Uniform},
    seq::SliceRandom,
    Rng,
};
use rlp::Rlp;
use std::{
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
