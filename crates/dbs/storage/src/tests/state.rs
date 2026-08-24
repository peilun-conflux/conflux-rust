// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use crate::{
    impls::storage_manager::storage_manager::StorageManager,
    storage_db::SnapshotDbManagerTrait, StorageConfiguration,
};
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

const ISOLATED_MPT_WRITER_SNAPSHOT_EPOCH_COUNT: u32 = 2;
const ISOLATED_MPT_WRITER_ERA_EPOCH_COUNT: u64 = 2;

#[derive(Clone)]
struct IsolatedMptWriterBuild {
    parent_id: EpochId,
    target_id: EpochId,
    snapshot_info: crate::storage_db::SnapshotInfo,
    delta_mpt: crate::impls::delta_mpt::DeltaMptIterator,
    recover_mpt_with_kv_snapshot_exist: bool,
}

struct IsolatedMptWriterFixture {
    test_root: std::path::PathBuf,
    state_manager: Option<Arc<StateManager>>,
    checkpoint: (EpochId, u64),
    logical_gap: Option<IsolatedMptWriterBuild>,
    same_height_gap: Option<IsolatedMptWriterBuild>,
    queued_parent: Option<IsolatedMptWriterBuild>,
    queued_child: Option<IsolatedMptWriterBuild>,
}

impl Drop for IsolatedMptWriterFixture {
    fn drop(&mut self) {
        self.logical_gap.take();
        self.same_height_gap.take();
        self.queued_parent.take();
        self.queued_child.take();
        self.state_manager.take();
        fs::remove_dir_all(&self.test_root).ok();
    }
}

fn isolated_mpt_writer_epoch_id(tag: u8, height: u64) -> EpochId {
    let mut bytes = [0_u8; 32];
    bytes[0] = tag;
    bytes[24..].copy_from_slice(&height.to_be_bytes());
    H256::from(bytes)
}

fn isolated_mpt_writer_manager(data_dir: &Path) -> Arc<StateManager> {
    let mut storage_conf = StorageConfiguration::new_default(
        data_dir.to_str().unwrap(),
        ISOLATED_MPT_WRITER_SNAPSHOT_EPOCH_COUNT,
        ISOLATED_MPT_WRITER_ERA_EPOCH_COUNT,
    );
    storage_conf.use_isolated_db_for_mpt_table = true;
    storage_conf.delta_mpts_cache_size = 20_000_000;
    storage_conf.delta_mpts_cache_start_size = 1_000_000;
    storage_conf.delta_mpts_node_map_vec_size = 20_000_000;
    storage_conf.delta_mpts_slab_idle_size = 200_000;

    Arc::new(StateManager::new(storage_conf).unwrap())
}

fn isolated_mpt_transition_manager(data_dir: &Path) -> Arc<StateManager> {
    let mut storage_conf = StorageConfiguration::new_default(
        data_dir.to_str().unwrap(),
        ISOLATED_MPT_WRITER_SNAPSHOT_EPOCH_COUNT,
        ISOLATED_MPT_WRITER_ERA_EPOCH_COUNT,
    );
    storage_conf.use_isolated_db_for_mpt_table = true;
    storage_conf.use_isolated_db_for_mpt_table_height = Some(4);
    storage_conf.delta_mpts_cache_size = 20_000_000;
    storage_conf.delta_mpts_cache_start_size = 1_000_000;
    storage_conf.delta_mpts_node_map_vec_size = 20_000_000;
    storage_conf.delta_mpts_slab_idle_size = 200_000;

    Arc::new(StateManager::new(storage_conf).unwrap())
}

struct IsolatedMptSpecialModeFixture {
    test_root: std::path::PathBuf,
    state_manager: Option<Arc<StateManager>>,
}

impl Drop for IsolatedMptSpecialModeFixture {
    fn drop(&mut self) {
        self.state_manager.take();
        fs::remove_dir_all(&self.test_root).ok();
    }
}

fn prepare_isolated_mpt_special_mode_fixture() -> IsolatedMptSpecialModeFixture
{
    let test_root = std::env::temp_dir().join(format!(
        "conflux-isolated-mpt-special-mode-{}",
        random::<u64>()
    ));
    fs::create_dir_all(&test_root).unwrap();
    let state_manager = isolated_mpt_writer_manager(&test_root);

    IsolatedMptSpecialModeFixture {
        test_root,
        state_manager: Some(state_manager),
    }
}

#[test]
fn isolated_mpt_null_genesis_creation_publishes_latest() {
    let _serial = ISOLATED_MPT_STATE_MANAGER_TEST_LOCK.lock();
    let fixture = prepare_isolated_mpt_special_mode_fixture();
    let state_manager = fixture.state_manager.as_ref().unwrap();
    let genesis_snapshot_id = isolated_mpt_writer_epoch_id(6, 0);
    let storage_manager = state_manager.get_storage_manager_arc().clone();
    let _genesis_state = state_manager.get_state_for_genesis_write();
    let delta_mpt = storage_manager
        .get_intermediate_mpt(&primitives::NULL_EPOCH)
        .unwrap()
        .unwrap();
    let mut genesis_snapshot_info = storage_manager
        .get_snapshot_info_at_epoch(&primitives::NULL_EPOCH)
        .unwrap();
    *genesis_snapshot_info.pivot_chain_parts.last_mut().unwrap() =
        genesis_snapshot_id;
    let snapshot_db_manager = storage_manager
        .get_snapshot_manager()
        .get_snapshot_db_manager();
    let (snapshot_info_map, _) = snapshot_db_manager
        .new_snapshot_by_merging(
            &primitives::NULL_EPOCH,
            genesis_snapshot_id,
            crate::impls::delta_mpt::DeltaMptIterator {
                mpt: delta_mpt,
                maybe_root_node: None,
            },
            genesis_snapshot_info,
            &storage_manager.snapshot_info_map_by_epoch,
            0,
            false,
        )
        .unwrap();
    drop(snapshot_info_map);
    assert_eq!(
        snapshot_db_manager.latest_snapshot_id(),
        (genesis_snapshot_id, 0)
    );
}

#[test]
fn isolated_mpt_full_sync_temp_creation_publishes_latest() {
    let _serial = ISOLATED_MPT_STATE_MANAGER_TEST_LOCK.lock();
    let fixture = prepare_isolated_mpt_special_mode_fixture();
    let state_manager = fixture.state_manager.as_ref().unwrap();
    let snapshot_db_manager = state_manager
        .get_storage_manager()
        .get_snapshot_manager()
        .get_snapshot_db_manager();
    let sync_id = isolated_mpt_writer_epoch_id(7, 4);
    let root = H256::from([7_u8; 32]);
    let height = 4;

    let temp = snapshot_db_manager
        .new_temp_snapshot_for_full_sync(&sync_id, &root, height)
        .unwrap();
    assert_eq!(snapshot_db_manager.latest_snapshot_id(), (sync_id, height));
    drop(temp);
}

#[test]
fn isolated_mpt_full_sync_checkpoint_copy_failure_still_finalizes() {
    let _serial = ISOLATED_MPT_STATE_MANAGER_TEST_LOCK.lock();
    let fixture = prepare_isolated_mpt_special_mode_fixture();
    let state_manager = fixture.state_manager.as_ref().unwrap();
    let storage_manager = state_manager.get_storage_manager();
    let snapshot_db_manager = storage_manager
        .get_snapshot_manager()
        .get_snapshot_db_manager();
    let sync_id = isolated_mpt_writer_epoch_id(8, 4);
    let root = H256::from([8_u8; 32]);
    let height = 4;

    let temp = snapshot_db_manager
        .new_temp_snapshot_for_full_sync(&sync_id, &root, height)
        .unwrap();
    drop(temp);
    let hook: Arc<dyn Fn() -> crate::Result<()> + Send + Sync + 'static> =
        Arc::new(|| Err(crate::Error::SnapshotCopyFailure));
    snapshot_db_manager.set_snapshot_copy_test_hook(Some(hook));

    let finalize_result = snapshot_db_manager.finalize_full_sync_snapshot(
        &sync_id,
        &root,
        &storage_manager.snapshot_info_map_by_epoch,
    );
    snapshot_db_manager.set_snapshot_copy_test_hook(None);
    drop(finalize_result.unwrap());

    assert_eq!(snapshot_db_manager.latest_snapshot_id(), (sync_id, height));
    assert!(snapshot_db_manager.get_snapshot_db_path(&sync_id).exists());
    assert!(!snapshot_db_manager
        .get_mpt_snapshot_dir()
        .join(snapshot_db_manager.get_snapshot_db_name(&sync_id))
        .exists());
    assert_eq!(
        snapshot_db_manager.latest_mpt_snapshot_available_permits_for_test(),
        1
    );
}

fn commit_isolated_mpt_writer_epoch(
    state_manager: &Arc<StateManager>, parent_epoch_id: &EpochId,
    parent_state_root: &StateRootWithAuxInfo, height: u64,
) -> (EpochId, StateRootWithAuxInfo) {
    let mut state = state_manager
        .get_state_for_next_epoch(
            StateIndex::new_for_next_epoch(
                parent_epoch_id,
                parent_state_root,
                height - 1,
                ISOLATED_MPT_WRITER_SNAPSHOT_EPOCH_COUNT,
            ),
            false,
        )
        .unwrap()
        .unwrap_or_else(|| panic!("state unavailable at height {height}"));
    let key = format!("isolated-mpt-writer-key-{height}").into_bytes();
    state
        .set(
            StorageKey::AccountKey(&key).with_native_space(),
            height.to_be_bytes().to_vec().into(),
        )
        .unwrap();

    let epoch_id = isolated_mpt_writer_epoch_id(1, height);
    let state_root = state.compute_state_root().unwrap();
    state.commit(epoch_id).unwrap();
    wait_for_isolated_mpt_repro_snapshotting(state_manager);
    (epoch_id, state_root)
}

fn build_isolated_mpt_writer_chain(
    state_manager: &Arc<StateManager>, end_height: u64,
) -> Vec<(EpochId, StateRootWithAuxInfo)> {
    let mut genesis = state_manager.get_state_for_genesis_write();
    genesis
        .set(
            StorageKey::AccountKey(b"isolated-mpt-writer-key-0")
                .with_native_space(),
            0_u64.to_be_bytes().to_vec().into(),
        )
        .unwrap();
    let genesis_id = isolated_mpt_writer_epoch_id(1, 0);
    let genesis_root = genesis.compute_state_root().unwrap();
    genesis.commit(genesis_id).unwrap();

    let mut chain = vec![(genesis_id, genesis_root)];
    for height in 1..=end_height {
        let (parent_id, parent_root) = chain.last().unwrap();
        chain.push(commit_isolated_mpt_writer_epoch(
            state_manager,
            parent_id,
            parent_root,
            height,
        ));
    }
    chain
}

fn isolated_mpt_writer_build(
    state_manager: &Arc<StateManager>, canonical_target: &EpochId,
    target_id: EpochId, parent_id: EpochId,
    recover_mpt_with_kv_snapshot_exist: bool,
) -> IsolatedMptWriterBuild {
    let storage_manager = state_manager.get_storage_manager();
    let mut snapshot_info = storage_manager
        .get_snapshot_info_at_epoch(canonical_target)
        .unwrap();
    let canonical_parent_id = snapshot_info.parent_snapshot_epoch_id.clone();
    snapshot_info.parent_snapshot_epoch_id = parent_id.clone();
    *snapshot_info.pivot_chain_parts.last_mut().unwrap() = target_id.clone();

    let delta_mpt = storage_manager
        .get_intermediate_mpt(&canonical_parent_id)
        .unwrap()
        .unwrap();
    let maybe_root_node = delta_mpt
        .get_root_node_ref_by_epoch(canonical_target)
        .unwrap()
        .flatten();
    assert!(maybe_root_node.is_some());
    IsolatedMptWriterBuild {
        parent_id,
        target_id,
        snapshot_info,
        delta_mpt: crate::impls::delta_mpt::DeltaMptIterator {
            mpt: delta_mpt,
            maybe_root_node,
        },
        recover_mpt_with_kv_snapshot_exist,
    }
}

fn prepare_isolated_mpt_writer_fixture() -> IsolatedMptWriterFixture {
    let test_root = std::env::temp_dir()
        .join(format!("conflux-isolated-mpt-writer-{}", random::<u64>()));
    fs::create_dir_all(&test_root).unwrap();
    let state_manager = isolated_mpt_writer_manager(&test_root);
    // Entering height 7 triggers creation of the snapshot at height 6.
    build_isolated_mpt_writer_chain(&state_manager, 7);

    let checkpoint = (isolated_mpt_writer_epoch_id(1, 2), 2);
    let canonical_parent = isolated_mpt_writer_epoch_id(1, 4);
    let canonical_child = isolated_mpt_writer_epoch_id(1, 6);

    let logical_gap = isolated_mpt_writer_build(
        &state_manager,
        &canonical_child,
        isolated_mpt_writer_epoch_id(4, 6),
        canonical_parent.clone(),
        false,
    );
    let mut same_height_gap = isolated_mpt_writer_build(
        &state_manager,
        &canonical_child,
        isolated_mpt_writer_epoch_id(5, 6),
        isolated_mpt_writer_epoch_id(9, 2),
        false,
    );
    same_height_gap.snapshot_info.parent_snapshot_height = checkpoint.1;

    let queued_parent_id = isolated_mpt_writer_epoch_id(2, 4);
    let queued_child_id = isolated_mpt_writer_epoch_id(3, 6);
    let queued_parent = isolated_mpt_writer_build(
        &state_manager,
        &canonical_parent,
        queued_parent_id.clone(),
        checkpoint.0.clone(),
        true,
    );
    let queued_child = isolated_mpt_writer_build(
        &state_manager,
        &canonical_child,
        queued_child_id.clone(),
        queued_parent_id.clone(),
        true,
    );

    let snapshot_db_manager = state_manager
        .get_storage_manager()
        .get_snapshot_manager()
        .get_snapshot_db_manager();
    copy_isolated_mpt_repro_data_dir(
        &snapshot_db_manager.get_snapshot_db_path(&canonical_parent),
        &snapshot_db_manager.get_snapshot_db_path(&queued_parent_id),
    );
    copy_isolated_mpt_repro_data_dir(
        &snapshot_db_manager.get_snapshot_db_path(&canonical_child),
        &snapshot_db_manager.get_snapshot_db_path(&queued_child_id),
    );
    snapshot_db_manager.update_latest_snapshot_id(checkpoint.0, checkpoint.1);
    snapshot_db_manager
        .recovery_latest_mpt_snapshot_from_checkpoint(&checkpoint.0, None)
        .unwrap();

    IsolatedMptWriterFixture {
        test_root,
        state_manager: Some(state_manager),
        checkpoint,
        logical_gap: Some(logical_gap),
        same_height_gap: Some(same_height_gap),
        queued_parent: Some(queued_parent),
        queued_child: Some(queued_child),
    }
}

fn build_isolated_mpt_snapshot_with_parent(
    state_manager: &Arc<StateManager>, build: IsolatedMptWriterBuild,
) -> crate::Result<()> {
    let storage_manager = state_manager.get_storage_manager_arc().clone();
    let snapshot_db_manager = storage_manager
        .get_snapshot_manager()
        .get_snapshot_db_manager();
    let (mut snapshot_info_map, new_snapshot_info) = snapshot_db_manager
        .new_snapshot_by_merging(
            &build.parent_id,
            build.target_id,
            build.delta_mpt,
            build.snapshot_info.clone(),
            &storage_manager.snapshot_info_map_by_epoch,
            build.snapshot_info.height,
            build.recover_mpt_with_kv_snapshot_exist,
        )?;
    storage_manager
        .register_new_snapshot(new_snapshot_info, &mut snapshot_info_map)
}

fn wait_for_condition(
    timeout: Duration, mut condition: impl FnMut() -> bool,
) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        thread::sleep(Duration::from_millis(5));
    }
    condition()
}

fn install_isolated_mpt_parent_failure_hook(
    state_manager: &Arc<StateManager>, parent: &IsolatedMptWriterBuild,
) -> (Arc<std::sync::atomic::AtomicBool>, Arc<std::sync::Barrier>) {
    let parent_opened = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let release_parent = Arc::new(std::sync::Barrier::new(2));
    let hooked_parent_id = parent.target_id.clone();
    let expected_parent_id = parent.parent_id.clone();
    let parent_height = parent.snapshot_info.height;
    let expected_parent_height = parent.snapshot_info.parent_snapshot_height;
    let parent_opened_for_hook = parent_opened.clone();
    let release_parent_for_hook = release_parent.clone();
    let hook: Arc<
        dyn Fn(&EpochId) -> crate::Result<()> + Send + Sync + 'static,
    > = Arc::new(move |target_id| {
        if *target_id != hooked_parent_id {
            return Ok(());
        }
        parent_opened_for_hook
            .store(true, std::sync::atomic::Ordering::Release);
        release_parent_for_hook.wait();
        Err(crate::Error::IsolatedMptParentGap {
            target_id: target_id.clone(),
            target_height: parent_height,
            expected_parent_id: expected_parent_id.clone(),
            expected_parent_height,
            latest_id: expected_parent_id.clone(),
            latest_height: expected_parent_height,
        })
    });
    state_manager
        .get_storage_manager()
        .get_snapshot_manager()
        .get_snapshot_db_manager()
        .set_snapshot_merge_test_hook(Some(hook));
    (parent_opened, release_parent)
}

fn install_background_snapshot_parent_pause_hook(
    state_manager: &Arc<StateManager>, parent_id: EpochId, child_id: EpochId,
    parent_error: bool,
) -> (
    Arc<std::sync::Barrier>,
    Arc<std::sync::Barrier>,
    Arc<std::sync::atomic::AtomicBool>,
) {
    let parent_reached = Arc::new(std::sync::Barrier::new(2));
    let release_parent = Arc::new(std::sync::Barrier::new(2));
    let child_writer_started =
        Arc::new(std::sync::atomic::AtomicBool::new(false));
    let parent_reached_for_hook = parent_reached.clone();
    let release_parent_for_hook = release_parent.clone();
    let child_writer_started_for_hook = child_writer_started.clone();
    let hook: Arc<
        dyn Fn(&EpochId) -> crate::Result<()> + Send + Sync + 'static,
    > = Arc::new(move |target_id| {
        if *target_id == parent_id {
            parent_reached_for_hook.wait();
            release_parent_for_hook.wait();
            if parent_error {
                return Err(crate::Error::SnapshotCopyFailure);
            }
        } else if *target_id == child_id {
            child_writer_started_for_hook
                .store(true, std::sync::atomic::Ordering::Release);
        }
        Ok(())
    });
    state_manager
        .get_storage_manager()
        .set_snapshot_task_before_writer_test_hook(Some(hook));
    (parent_reached, release_parent, child_writer_started)
}

#[test]
fn isolated_mpt_parent_gap_rejects_logical_non_parent_before_registration() {
    let _serial = ISOLATED_MPT_STATE_MANAGER_TEST_LOCK.lock();
    let fixture = prepare_isolated_mpt_writer_fixture();
    let state_manager = fixture.state_manager.as_ref().unwrap();
    let target = fixture.logical_gap.as_ref().unwrap().target_id.clone();
    let old_latest = fixture.checkpoint;

    let err = build_isolated_mpt_snapshot_with_parent(
        state_manager,
        fixture.logical_gap.as_ref().unwrap().clone(),
    )
    .unwrap_err();

    assert!(matches!(
        err,
        crate::Error::IsolatedMptParentGap { target_id, .. }
            if target_id == target
    ));
    let snapshot_db_manager = state_manager
        .get_storage_manager()
        .get_snapshot_manager()
        .get_snapshot_db_manager();
    assert_eq!(snapshot_db_manager.latest_snapshot_id(), old_latest);
    assert!(state_manager
        .get_storage_manager()
        .get_snapshot_info_at_epoch(&target)
        .is_none());

    let same_height_gap = fixture.same_height_gap.as_ref().unwrap().clone();
    let same_height_target = same_height_gap.target_id;
    let same_height_expected_parent = same_height_gap.parent_id;
    let err =
        build_isolated_mpt_snapshot_with_parent(state_manager, same_height_gap)
            .unwrap_err();
    match err {
        crate::Error::IsolatedMptParentGap {
            target_id,
            target_height,
            expected_parent_id,
            expected_parent_height,
            latest_id,
            latest_height,
        } => {
            assert_eq!(target_id, same_height_target);
            assert_eq!(target_height, 6);
            assert_eq!(expected_parent_id, same_height_expected_parent);
            assert_eq!(expected_parent_height, old_latest.1);
            assert_eq!(latest_id, old_latest.0);
            assert_eq!(latest_height, old_latest.1);
        }
        other => panic!("expected exact-pair parent gap, got {other:?}"),
    }
    assert_eq!(snapshot_db_manager.latest_snapshot_id(), old_latest);
    assert!(state_manager
        .get_storage_manager()
        .get_snapshot_info_at_epoch(&same_height_target)
        .is_none());
}

fn assert_snapshot_info_fields_equal(
    expected: &crate::storage_db::SnapshotInfo,
    actual: &crate::storage_db::SnapshotInfo,
) {
    assert_eq!(expected.merkle_root, actual.merkle_root);
    assert_eq!(expected.height, actual.height);
    assert_eq!(
        expected.parent_snapshot_height,
        actual.parent_snapshot_height
    );
    assert_eq!(
        expected.parent_snapshot_epoch_id,
        actual.parent_snapshot_epoch_id
    );
    assert_eq!(expected.pivot_chain_parts, actual.pivot_chain_parts);
    assert_eq!(expected.serve_one_step_sync, actual.serve_one_step_sync);
    assert_eq!(
        expected.snapshot_info_kept_to_provide_sync,
        actual.snapshot_info_kept_to_provide_sync,
    );
}

#[test]
fn isolated_mpt_retained_rebuild_rejects_stale_physical_base_root() {
    let _serial = ISOLATED_MPT_STATE_MANAGER_TEST_LOCK.lock();
    let fixture = prepare_isolated_mpt_writer_fixture();
    let state_manager = fixture.state_manager.as_ref().unwrap();
    let storage_manager = state_manager.get_storage_manager();
    let snapshot_db_manager = storage_manager
        .get_snapshot_manager()
        .get_snapshot_db_manager();
    let physical_base_id = fixture.checkpoint.0;
    let logical_parent_id = isolated_mpt_writer_epoch_id(1, 4);
    let target_id = isolated_mpt_writer_epoch_id(1, 6);
    let target_info_before = storage_manager
        .get_snapshot_info_at_epoch(&target_id)
        .unwrap();
    let physical_base_info = storage_manager
        .get_snapshot_info_at_epoch(&physical_base_id)
        .unwrap();
    let physical_root_before = snapshot_db_manager
        .latest_mpt_merkle_root_for_test(&physical_base_id)
        .unwrap()
        .unwrap();
    assert_eq!(physical_root_before, physical_base_info.merkle_root);

    let mut retained_build = fixture.queued_child.as_ref().unwrap().clone();
    retained_build.parent_id = logical_parent_id;
    retained_build.target_id = target_id;
    retained_build.snapshot_info = target_info_before.clone();
    retained_build.recover_mpt_with_kv_snapshot_exist = true;
    let old_latest = (
        retained_build.parent_id,
        retained_build.snapshot_info.parent_snapshot_height,
    );
    snapshot_db_manager.update_latest_snapshot_id(old_latest.0, old_latest.1);

    let err =
        build_isolated_mpt_snapshot_with_parent(state_manager, retained_build)
            .unwrap_err();
    let error_message = err.to_string();
    match &err {
        crate::Error::IsolatedMptRootMismatch {
            target,
            expected,
            actual,
        } => {
            assert_eq!(
                target,
                &snapshot_db_manager.get_snapshot_db_path(&target_id)
            );
            assert_eq!(*expected, target_info_before.merkle_root);
            assert_ne!(actual, expected);
        }
        other => panic!("expected retained-root mismatch, got {other:?}"),
    }
    let target_info_after = storage_manager
        .get_snapshot_info_at_epoch(&target_id)
        .unwrap();
    let physical_root_after = snapshot_db_manager
        .latest_mpt_merkle_root_for_test(&old_latest.0)
        .unwrap()
        .unwrap();

    assert!(error_message.contains("retained isolated MPT root mismatch"));
    assert!(error_message.contains("expected"));
    assert!(error_message.contains("actual"));
    assert!(error_message.contains("target"));
    assert!(error_message.contains("enable recovery_latest_mpt_snapshot"));
    assert!(error_message.contains("restore a clean snapshot"));
    assert!(error_message.contains("or resync"));
    assert_eq!(snapshot_db_manager.latest_snapshot_id(), old_latest);
    assert_snapshot_info_fields_equal(&target_info_before, &target_info_after);
    assert_eq!(physical_root_after, physical_root_before);
    assert_ne!(physical_root_after, target_info_before.merkle_root);
    assert_eq!(
        snapshot_db_manager.latest_mpt_snapshot_available_permits_for_test(),
        1
    );
}

#[test]
fn isolated_mpt_retained_transition_mismatch_rolls_back_seed_and_publication() {
    let _serial = ISOLATED_MPT_STATE_MANAGER_TEST_LOCK.lock();
    let test_root = IsolatedMptRecoveryTestRoot::new();
    let state_manager = isolated_mpt_transition_manager(test_root.path());
    let chain = build_isolated_mpt_writer_chain(&state_manager, 5);
    let storage_manager = state_manager.get_storage_manager();
    let snapshot_db_manager = storage_manager
        .get_snapshot_manager()
        .get_snapshot_db_manager();
    let target_id = chain[4].0;
    let wrong_delta_epoch_id = chain[3].0;
    let target_info_before = storage_manager
        .get_snapshot_info_at_epoch(&target_id)
        .unwrap();
    let parent_id = target_info_before.parent_snapshot_epoch_id;
    let parent_info = storage_manager
        .get_snapshot_info_at_epoch(&parent_id)
        .unwrap();
    let parent_intermediate_mpt = storage_manager
        .get_intermediate_mpt(&parent_id)
        .unwrap()
        .unwrap();
    let wrong_delta_root = parent_intermediate_mpt
        .get_root_node_ref_by_epoch(&wrong_delta_epoch_id)
        .unwrap()
        .flatten()
        .unwrap();
    let correct_delta_root = parent_intermediate_mpt
        .get_root_node_ref_by_epoch(&target_id)
        .unwrap()
        .flatten()
        .unwrap();
    let retained_build = IsolatedMptWriterBuild {
        parent_id,
        target_id,
        snapshot_info: target_info_before.clone(),
        delta_mpt: crate::impls::delta_mpt::DeltaMptIterator {
            mpt: parent_intermediate_mpt.clone(),
            maybe_root_node: Some(wrong_delta_root),
        },
        recover_mpt_with_kv_snapshot_exist: true,
    };

    assert_eq!(snapshot_db_manager.latest_snapshot_id(), (target_id, 4));
    assert_eq!(
        snapshot_db_manager
            .latest_mpt_merkle_root_for_test(&target_id)
            .unwrap()
            .unwrap(),
        target_info_before.merkle_root,
        "new transition seed must retain open-time publication",
    );
    snapshot_db_manager.recreate_latest_mpt_snapshot().unwrap();
    let old_latest = (primitives::NULL_EPOCH, 0);
    snapshot_db_manager.update_latest_snapshot_id(old_latest.0, old_latest.1);
    let physical_root_before = snapshot_db_manager
        .latest_mpt_merkle_root_for_test(&old_latest.0)
        .unwrap()
        .unwrap();
    assert_eq!(physical_root_before, primitives::MERKLE_NULL_NODE);

    let err =
        build_isolated_mpt_snapshot_with_parent(&state_manager, retained_build)
            .unwrap_err();
    let latest_after_failure = snapshot_db_manager.latest_snapshot_id();
    snapshot_db_manager.update_latest_snapshot_id(old_latest.0, old_latest.1);
    let physical_root_after = snapshot_db_manager
        .latest_mpt_merkle_root_for_test(&old_latest.0)
        .unwrap()
        .unwrap();
    let target_info_after = storage_manager
        .get_snapshot_info_at_epoch(&target_id)
        .unwrap();

    assert!(matches!(
        err,
        crate::Error::IsolatedMptRootMismatch {
            expected,
            actual,
            ..
        } if expected == target_info_before.merkle_root && actual != expected
    ));
    assert_ne!(parent_info.merkle_root, physical_root_before);
    assert_eq!(
        (latest_after_failure, physical_root_after),
        (old_latest, physical_root_before),
        "retained transition mismatch must roll back parent seeding and target publication",
    );
    assert_snapshot_info_fields_equal(&target_info_before, &target_info_after);
    assert_eq!(
        snapshot_db_manager.latest_mpt_snapshot_available_permits_for_test(),
        1
    );

    build_isolated_mpt_snapshot_with_parent(
        &state_manager,
        IsolatedMptWriterBuild {
            parent_id,
            target_id,
            snapshot_info: target_info_before.clone(),
            delta_mpt: crate::impls::delta_mpt::DeltaMptIterator {
                mpt: parent_intermediate_mpt,
                maybe_root_node: Some(correct_delta_root),
            },
            recover_mpt_with_kv_snapshot_exist: true,
        },
    )
    .unwrap();
    assert_eq!(snapshot_db_manager.latest_snapshot_id(), (target_id, 4));
    assert_eq!(
        snapshot_db_manager
            .latest_mpt_merkle_root_for_test(&target_id)
            .unwrap()
            .unwrap(),
        target_info_before.merkle_root,
    );
}

#[test]
fn isolated_mpt_retained_null_mismatch_does_not_publish_target() {
    let _serial = ISOLATED_MPT_STATE_MANAGER_TEST_LOCK.lock();
    let fixture = prepare_isolated_mpt_special_mode_fixture();
    let state_manager = fixture.state_manager.as_ref().unwrap();
    let target_id = isolated_mpt_writer_epoch_id(8, 0);
    let mut genesis_state = state_manager.get_state_for_genesis_write();
    genesis_state
        .set(
            StorageKey::AccountKey(b"retained-null-target").with_native_space(),
            b"non-empty".to_vec().into(),
        )
        .unwrap();
    genesis_state.compute_state_root().unwrap();
    genesis_state.commit(target_id).unwrap();
    drop(genesis_state);

    let storage_manager = state_manager.get_storage_manager_arc().clone();
    let snapshot_db_manager = storage_manager
        .get_snapshot_manager()
        .get_snapshot_db_manager();
    let target_delta_mpt = storage_manager
        .get_intermediate_mpt(&primitives::NULL_EPOCH)
        .unwrap()
        .unwrap();
    let target_delta_root = target_delta_mpt
        .get_root_node_ref_by_epoch(&target_id)
        .unwrap()
        .flatten()
        .unwrap();
    let mut target_info = storage_manager
        .get_snapshot_info_at_epoch(&primitives::NULL_EPOCH)
        .unwrap();
    *target_info.pivot_chain_parts.last_mut().unwrap() = target_id;
    let (mut snapshot_info_map, new_snapshot_info) = snapshot_db_manager
        .new_snapshot_by_merging(
            &primitives::NULL_EPOCH,
            target_id,
            crate::impls::delta_mpt::DeltaMptIterator {
                mpt: target_delta_mpt.clone(),
                maybe_root_node: Some(target_delta_root.clone()),
            },
            target_info,
            &storage_manager.snapshot_info_map_by_epoch,
            0,
            false,
        )
        .unwrap();
    storage_manager
        .register_new_snapshot(new_snapshot_info, &mut snapshot_info_map)
        .unwrap();
    drop(snapshot_info_map);
    let target_info_before = storage_manager
        .get_snapshot_info_at_epoch(&target_id)
        .unwrap();
    assert_ne!(target_info_before.merkle_root, primitives::MERKLE_NULL_NODE);
    let retained_build = IsolatedMptWriterBuild {
        parent_id: primitives::NULL_EPOCH,
        target_id,
        snapshot_info: target_info_before.clone(),
        delta_mpt: crate::impls::delta_mpt::DeltaMptIterator {
            mpt: target_delta_mpt.clone(),
            maybe_root_node: None,
        },
        recover_mpt_with_kv_snapshot_exist: true,
    };

    snapshot_db_manager.recreate_latest_mpt_snapshot().unwrap();
    let old_latest = (primitives::NULL_EPOCH, 0);
    snapshot_db_manager.update_latest_snapshot_id(old_latest.0, old_latest.1);
    let physical_root_before = snapshot_db_manager
        .latest_mpt_merkle_root_for_test(&old_latest.0)
        .unwrap()
        .unwrap();

    let err =
        build_isolated_mpt_snapshot_with_parent(state_manager, retained_build)
            .unwrap_err();
    let latest_after_failure = snapshot_db_manager.latest_snapshot_id();
    snapshot_db_manager.update_latest_snapshot_id(old_latest.0, old_latest.1);
    let physical_root_after = snapshot_db_manager
        .latest_mpt_merkle_root_for_test(&old_latest.0)
        .unwrap()
        .unwrap();
    let target_info_after = storage_manager
        .get_snapshot_info_at_epoch(&target_id)
        .unwrap();

    assert!(matches!(
        err,
        crate::Error::IsolatedMptRootMismatch {
            expected,
            actual,
            ..
        } if expected == target_info_before.merkle_root && actual != expected
    ));
    assert_eq!(
        (latest_after_failure, physical_root_after),
        (old_latest, physical_root_before),
        "retained NULL-parent mismatch must not publish the target",
    );
    assert_snapshot_info_fields_equal(&target_info_before, &target_info_after);
    assert_eq!(
        snapshot_db_manager.latest_mpt_snapshot_available_permits_for_test(),
        1
    );

    build_isolated_mpt_snapshot_with_parent(
        state_manager,
        IsolatedMptWriterBuild {
            parent_id: primitives::NULL_EPOCH,
            target_id,
            snapshot_info: target_info_before.clone(),
            delta_mpt: crate::impls::delta_mpt::DeltaMptIterator {
                mpt: target_delta_mpt,
                maybe_root_node: Some(target_delta_root),
            },
            recover_mpt_with_kv_snapshot_exist: true,
        },
    )
    .unwrap();
    assert_eq!(snapshot_db_manager.latest_snapshot_id(), (target_id, 0));
    assert_eq!(
        snapshot_db_manager
            .latest_mpt_merkle_root_for_test(&target_id)
            .unwrap()
            .unwrap(),
        target_info_before.merkle_root,
    );
}

#[test]
fn isolated_mpt_checkpoint_copy_failure_keeps_snapshot_pipeline_live() {
    let _serial = ISOLATED_MPT_STATE_MANAGER_TEST_LOCK.lock();
    let fixture = prepare_isolated_mpt_writer_fixture();
    let state_manager = fixture.state_manager.as_ref().unwrap().clone();
    let build = fixture.queued_parent.as_ref().unwrap().clone();
    let child = fixture.queued_child.as_ref().unwrap().clone();
    let target_id = build.target_id.clone();
    let target_height = build.snapshot_info.height;
    let child_id = child.target_id.clone();
    let snapshot_db_manager = state_manager
        .get_storage_manager()
        .get_snapshot_manager()
        .get_snapshot_db_manager();
    let temp_mpt_path = snapshot_db_manager
        .merge_temp_mpt_snapshot_db_path_for_test(&target_id);
    fs::create_dir_all(&temp_mpt_path).unwrap();
    fs::write(temp_mpt_path.join("partial-copy"), b"partial").unwrap();
    let hook: Arc<dyn Fn() -> crate::Result<()> + Send + Sync + 'static> =
        Arc::new(|| Err(crate::Error::SnapshotCopyFailure));
    snapshot_db_manager.set_snapshot_copy_test_hook(Some(hook));

    let (result_sender, result_receiver) = std::sync::mpsc::channel();
    let build_state_manager = state_manager.clone();
    let first_build = build.clone();
    let build_thread = thread::spawn(move || {
        let result = build_isolated_mpt_snapshot_with_parent(
            &build_state_manager,
            first_build,
        );
        result_sender.send(result).unwrap();
    });
    let result = result_receiver
        .recv_timeout(Duration::from_secs(3))
        .expect("copy failure did not unwind the snapshot builder");
    build_thread.join().unwrap();
    snapshot_db_manager.set_snapshot_copy_test_hook(None);
    result.unwrap();

    let target_info = state_manager
        .get_storage_manager()
        .get_snapshot_info_at_epoch(&target_id)
        .unwrap();
    assert_eq!(
        snapshot_db_manager.latest_snapshot_id(),
        (target_id.clone(), target_height)
    );
    assert_eq!(
        snapshot_db_manager
            .latest_mpt_merkle_root_for_test(&target_id)
            .unwrap()
            .unwrap(),
        target_info.merkle_root,
    );
    assert!(!temp_mpt_path.exists());
    assert!(!snapshot_db_manager
        .get_mpt_snapshot_dir()
        .join(snapshot_db_manager.get_snapshot_db_name(&target_id))
        .exists());
    assert_eq!(
        snapshot_db_manager.latest_mpt_snapshot_available_permits_for_test(),
        1
    );

    build_isolated_mpt_snapshot_with_parent(&state_manager, child).unwrap();
    assert!(state_manager
        .get_storage_manager()
        .get_snapshot_info_at_epoch(&child_id)
        .is_some());
}

#[test]
fn isolated_mpt_checkpoint_copy_retry_removes_partial_temp() {
    let _serial = ISOLATED_MPT_STATE_MANAGER_TEST_LOCK.lock();
    let fixture = prepare_isolated_mpt_writer_fixture();
    let state_manager = fixture.state_manager.as_ref().unwrap().clone();
    let build = fixture.queued_parent.as_ref().unwrap().clone();
    let target_id = build.target_id.clone();
    let target_height = build.snapshot_info.height;
    let snapshot_db_manager = state_manager
        .get_storage_manager()
        .get_snapshot_manager()
        .get_snapshot_db_manager();
    let temp_mpt_path = snapshot_db_manager
        .merge_temp_mpt_snapshot_db_path_for_test(&target_id);
    let partial_marker = temp_mpt_path.join("partial-copy");
    fs::create_dir_all(&temp_mpt_path).unwrap();
    fs::write(&partial_marker, b"partial").unwrap();

    build_isolated_mpt_snapshot_with_parent(&state_manager, build).unwrap();

    let checkpoint_mpt_path = snapshot_db_manager
        .get_mpt_snapshot_dir()
        .join(snapshot_db_manager.get_snapshot_db_name(&target_id));
    assert!(!partial_marker.exists());
    assert!(!checkpoint_mpt_path.join("partial-copy").exists());
    assert_eq!(
        snapshot_db_manager.latest_snapshot_id(),
        (target_id.clone(), target_height)
    );
    assert!(state_manager
        .get_storage_manager()
        .get_snapshot_info_at_epoch(&target_id)
        .is_some());
}

#[test]
fn isolated_mpt_queued_child_rejects_when_parent_merge_fails() {
    let _serial = ISOLATED_MPT_STATE_MANAGER_TEST_LOCK.lock();
    let fixture = prepare_isolated_mpt_writer_fixture();
    let state_manager = fixture.state_manager.as_ref().unwrap().clone();
    let parent = fixture.queued_parent.as_ref().unwrap().clone();
    let child = fixture.queued_child.as_ref().unwrap().clone();
    let child_id = child.target_id.clone();
    let old_latest = fixture.checkpoint;
    let (parent_opened, release_parent) =
        install_isolated_mpt_parent_failure_hook(&state_manager, &parent);

    let parent_state_manager = state_manager.clone();
    let parent_thread = thread::spawn(move || {
        build_isolated_mpt_snapshot_with_parent(&parent_state_manager, parent)
    });
    let parent_reached_hook =
        wait_for_condition(Duration::from_secs(10), || {
            parent_opened.load(std::sync::atomic::Ordering::Acquire)
        });
    if !parent_reached_hook {
        let parent_result = parent_thread.join().unwrap();
        panic!("parent did not reach writer-open hook: {parent_result:?}");
    }

    let child_state_manager = state_manager.clone();
    let child_thread = thread::spawn(move || {
        build_isolated_mpt_snapshot_with_parent(&child_state_manager, child)
    });
    let snapshot_db_manager = state_manager
        .get_storage_manager()
        .get_snapshot_manager()
        .get_snapshot_db_manager();
    let child_reached_writer_queue =
        wait_for_condition(Duration::from_secs(10), || {
            snapshot_db_manager
                .is_kv_snapshot_open_for_write_for_test(&child_id)
        });
    let latest_while_parent_is_paused =
        snapshot_db_manager.latest_snapshot_id();
    release_parent.wait();

    let parent_result = parent_thread.join().unwrap();
    let child_result = child_thread.join().unwrap();
    snapshot_db_manager.set_snapshot_merge_test_hook(None);

    assert!(
        child_reached_writer_queue,
        "child did not reach writer queue"
    );
    assert!(parent_result.is_err());
    assert!(matches!(
        child_result,
        Err(crate::Error::IsolatedMptParentGap { target_id, .. })
            if target_id == child_id
    ));
    assert_eq!(latest_while_parent_is_paused, old_latest);
    assert_eq!(snapshot_db_manager.latest_snapshot_id(), old_latest);
    assert!(state_manager
        .get_storage_manager()
        .get_snapshot_info_at_epoch(&child_id)
        .is_none());
}

#[test]
fn isolated_mpt_background_child_waits_for_parent_success() {
    let _serial = ISOLATED_MPT_STATE_MANAGER_TEST_LOCK.lock();
    let fixture = prepare_isolated_mpt_writer_fixture();
    let state_manager = fixture.state_manager.as_ref().unwrap().clone();
    let storage_manager = state_manager.get_storage_manager_arc().clone();
    let parent_id = isolated_mpt_writer_epoch_id(1, 4);
    let child_id = isolated_mpt_writer_epoch_id(1, 6);
    let parent_delta =
        fixture.queued_parent.as_ref().unwrap().delta_mpt.clone();
    let child_delta = fixture.queued_child.as_ref().unwrap().delta_mpt.clone();
    let (parent_reached, release_parent, child_writer_started) =
        install_background_snapshot_parent_pause_hook(
            &state_manager,
            parent_id.clone(),
            child_id.clone(),
            false,
        );

    StorageManager::check_make_register_snapshot_background(
        storage_manager.clone(),
        parent_id.clone(),
        4,
        Some(parent_delta),
        false,
    )
    .unwrap();
    parent_reached.wait();
    StorageManager::check_make_register_snapshot_background(
        storage_manager.clone(),
        child_id.clone(),
        6,
        Some(child_delta),
        false,
    )
    .unwrap();

    let child_started_before_parent =
        wait_for_condition(Duration::from_millis(500), || {
            child_writer_started.load(std::sync::atomic::Ordering::Acquire)
        });
    let child_active_while_parent_paused = storage_manager
        .in_progress_snapshotting_tasks
        .read()
        .contains_key(&child_id);
    release_parent.wait();
    let child_started_after_parent =
        wait_for_condition(Duration::from_secs(10), || {
            child_writer_started.load(std::sync::atomic::Ordering::Acquire)
        });
    let tasks_finished = wait_for_condition(Duration::from_secs(10), || {
        !storage_manager
            .in_progress_snapshotting_tasks
            .read()
            .contains_key(&parent_id)
            && !storage_manager
                .in_progress_snapshotting_tasks
                .read()
                .contains_key(&child_id)
    });
    storage_manager.set_snapshot_task_before_writer_test_hook(None);

    assert!(
        !child_started_before_parent,
        "child began writer work before its parent task completed"
    );
    assert!(child_active_while_parent_paused);
    assert!(child_started_after_parent);
    assert!(tasks_finished);
    assert_eq!(
        storage_manager
            .get_snapshot_manager()
            .get_snapshot_db_manager()
            .latest_snapshot_id(),
        (child_id, 6)
    );
}

#[test]
fn isolated_mpt_background_parent_error_stops_child_writer() {
    let _serial = ISOLATED_MPT_STATE_MANAGER_TEST_LOCK.lock();
    let fixture = prepare_isolated_mpt_writer_fixture();
    let state_manager = fixture.state_manager.as_ref().unwrap().clone();
    let storage_manager = state_manager.get_storage_manager_arc().clone();
    let parent_id = isolated_mpt_writer_epoch_id(1, 4);
    let child_id = isolated_mpt_writer_epoch_id(1, 6);
    let parent_delta =
        fixture.queued_parent.as_ref().unwrap().delta_mpt.clone();
    let child_delta = fixture.queued_child.as_ref().unwrap().delta_mpt.clone();
    let old_latest = fixture.checkpoint.clone();
    let (parent_reached, release_parent, child_writer_started) =
        install_background_snapshot_parent_pause_hook(
            &state_manager,
            parent_id.clone(),
            child_id.clone(),
            true,
        );

    StorageManager::check_make_register_snapshot_background(
        storage_manager.clone(),
        parent_id.clone(),
        4,
        Some(parent_delta),
        false,
    )
    .unwrap();
    parent_reached.wait();
    StorageManager::check_make_register_snapshot_background(
        storage_manager.clone(),
        child_id.clone(),
        6,
        Some(child_delta),
        false,
    )
    .unwrap();

    let child_started_before_parent =
        wait_for_condition(Duration::from_millis(500), || {
            child_writer_started.load(std::sync::atomic::Ordering::Acquire)
        });
    release_parent.wait();
    let tasks_finished = wait_for_condition(Duration::from_secs(10), || {
        !storage_manager
            .in_progress_snapshotting_tasks
            .read()
            .contains_key(&parent_id)
            && !storage_manager
                .in_progress_snapshotting_tasks
                .read()
                .contains_key(&child_id)
    });
    let child_writer_started =
        child_writer_started.load(std::sync::atomic::Ordering::Acquire);
    storage_manager.set_snapshot_task_before_writer_test_hook(None);

    assert!(
        !child_started_before_parent,
        "child began writer work while its parent task was paused"
    );
    assert!(
        !child_writer_started,
        "child writer ran after its parent task failed"
    );
    assert!(tasks_finished);
    assert_eq!(
        storage_manager
            .get_snapshot_manager()
            .get_snapshot_db_manager()
            .latest_snapshot_id(),
        old_latest
    );
}

#[test]
fn isolated_mpt_open_lock_order_releases_open_lock_before_latest_wait() {
    let _serial = ISOLATED_MPT_STATE_MANAGER_TEST_LOCK.lock();
    let fixture = prepare_isolated_mpt_writer_fixture();
    let state_manager = fixture.state_manager.as_ref().unwrap().clone();
    let parent = fixture.queued_parent.as_ref().unwrap().clone();
    let child = fixture.queued_child.as_ref().unwrap().clone();
    let child_id = child.target_id.clone();
    let read_epoch_id = fixture.checkpoint.0;
    let (parent_opened, release_parent) =
        install_isolated_mpt_parent_failure_hook(&state_manager, &parent);

    let parent_state_manager = state_manager.clone();
    let parent_thread = thread::spawn(move || {
        build_isolated_mpt_snapshot_with_parent(&parent_state_manager, parent)
    });
    let parent_reached_hook =
        wait_for_condition(Duration::from_secs(10), || {
            parent_opened.load(std::sync::atomic::Ordering::Acquire)
        });
    if !parent_reached_hook {
        let parent_result = parent_thread.join().unwrap();
        panic!("parent did not reach writer-open hook: {parent_result:?}");
    }

    let child_state_manager = state_manager.clone();
    let child_thread = thread::spawn(move || {
        build_isolated_mpt_snapshot_with_parent(&child_state_manager, child)
    });
    let snapshot_db_manager = state_manager
        .get_storage_manager()
        .get_snapshot_manager()
        .get_snapshot_db_manager();
    let child_reached_writer_queue =
        wait_for_condition(Duration::from_secs(10), || {
            snapshot_db_manager
                .is_kv_snapshot_open_for_write_for_test(&child_id)
        });

    let (reader_sender, reader_receiver) = std::sync::mpsc::channel();
    let reader_state_manager = state_manager.clone();
    let reader_thread = thread::spawn(move || {
        let result = reader_state_manager
            .get_storage_manager()
            .get_snapshot_manager()
            .get_snapshot_db_manager()
            .get_snapshot_by_epoch_id(&read_epoch_id, false, false)
            .map(|snapshot| snapshot.is_some());
        reader_sender.send(result).unwrap();
    });
    let reader_result_before_parent_release =
        reader_receiver.recv_timeout(Duration::from_secs(5)).ok();
    let reader_completed_before_parent_release =
        reader_result_before_parent_release.is_some();

    release_parent.wait();
    let _ = parent_thread.join().unwrap();
    let _ = child_thread.join().unwrap();
    let reader_result = match reader_result_before_parent_release {
        Some(result) => result,
        None => reader_receiver
            .recv_timeout(Duration::from_secs(10))
            .unwrap(),
    };
    reader_thread.join().unwrap();
    snapshot_db_manager.set_snapshot_merge_test_hook(None);

    assert!(
        child_reached_writer_queue,
        "child did not reach writer queue"
    );
    assert!(
        reader_completed_before_parent_release,
        "readonly open was blocked by a writer waiting for latest MPT"
    );
    assert_eq!(reader_result.unwrap(), true);
}

#[test]
fn isolated_mpt_readonly_latest_path_revalidates_after_writer_publish() {
    let _serial = ISOLATED_MPT_STATE_MANAGER_TEST_LOCK.lock();
    let fixture = prepare_isolated_mpt_writer_fixture();
    let state_manager = fixture.state_manager.as_ref().unwrap().clone();
    let writer_target_id = isolated_mpt_writer_epoch_id(1, 4);
    let requested_old_latest_id = fixture.checkpoint.0;
    let writer_target_info = state_manager
        .get_storage_manager()
        .get_snapshot_info_at_epoch(&writer_target_id)
        .unwrap();
    let snapshot_db_manager = state_manager
        .get_storage_manager()
        .get_snapshot_manager()
        .get_snapshot_db_manager();
    let old_checkpoint_mpt_path =
        snapshot_db_manager.get_mpt_snapshot_dir().join(
            snapshot_db_manager.get_snapshot_db_name(&requested_old_latest_id),
        );
    let old_physical_root = snapshot_db_manager
        .latest_mpt_merkle_root_for_test(&requested_old_latest_id)
        .unwrap()
        .unwrap();
    fs::remove_dir_all(&old_checkpoint_mpt_path).unwrap();

    let (reader_ready_sender, reader_ready_receiver) =
        std::sync::mpsc::channel();
    let (release_reader_sender, release_reader_receiver) =
        std::sync::mpsc::channel();
    let release_reader_receiver =
        Arc::new(parking_lot::Mutex::new(release_reader_receiver));
    let release_reader_receiver_for_hook = release_reader_receiver.clone();
    let hooked_reader_target = requested_old_latest_id;
    snapshot_db_manager.set_snapshot_reader_before_latest_pin_test_hook(Some(
        Arc::new(move |target_id| {
            if *target_id == hooked_reader_target {
                reader_ready_sender
                    .send(())
                    .map_err(|_| crate::Error::MpscError)?;
                release_reader_receiver_for_hook
                    .lock()
                    .recv()
                    .map_err(|_| crate::Error::MpscError)?;
            }
            Ok(())
        }),
    ));

    let reader_state_manager = state_manager.clone();
    let (reader_result_sender, reader_result_receiver) =
        std::sync::mpsc::channel();
    let reader_thread = thread::spawn(move || {
        let result = reader_state_manager
            .get_storage_manager()
            .get_snapshot_manager()
            .get_snapshot_db_manager()
            .get_snapshot_by_epoch_id(&requested_old_latest_id, false, true);
        reader_result_sender.send(result).unwrap();
    });
    reader_ready_receiver
        .recv_timeout(Duration::from_secs(10))
        .expect("reader did not pause after selecting shared latest MPT");

    snapshot_db_manager
        .publish_mpt_checkpoint_as_latest_for_test(writer_target_id, 4)
        .unwrap();
    assert_eq!(
        snapshot_db_manager.latest_snapshot_id(),
        (writer_target_id, 4)
    );
    let new_physical_root = snapshot_db_manager
        .latest_mpt_merkle_root_for_test(&writer_target_id)
        .unwrap()
        .unwrap();
    assert_eq!(new_physical_root, writer_target_info.merkle_root);
    assert_ne!(new_physical_root, old_physical_root);

    release_reader_sender.send(()).unwrap();
    let reader_result = reader_result_receiver
        .recv_timeout(Duration::from_secs(10))
        .expect("reader did not resume after writer publication");
    reader_thread.join().unwrap();
    snapshot_db_manager.set_snapshot_reader_before_latest_pin_test_hook(None);

    let reader_snapshot = reader_result.unwrap().unwrap();
    assert!(
        reader_snapshot.mpt_snapshot_db.is_none(),
        "reader returned old KV paired with the newly published latest MPT"
    );
    assert_eq!(
        snapshot_db_manager.latest_mpt_snapshot_available_permits_for_test(),
        1
    );
}

#[test]
fn isolated_mpt_snapshot_error_cleanup_allows_retry() {
    let _serial = ISOLATED_MPT_STATE_MANAGER_TEST_LOCK.lock();
    let fixture = prepare_isolated_mpt_writer_fixture();
    let state_manager = fixture.state_manager.as_ref().unwrap().clone();
    let mut rebuild = fixture.logical_gap.as_ref().unwrap().clone();
    let target_id = isolated_mpt_writer_epoch_id(1, 6);
    let parent_id = isolated_mpt_writer_epoch_id(1, 4);
    rebuild.target_id = target_id.clone();
    rebuild.parent_id = parent_id.clone();
    rebuild.snapshot_info.parent_snapshot_epoch_id = parent_id.clone();
    *rebuild.snapshot_info.pivot_chain_parts.last_mut().unwrap() =
        target_id.clone();

    let snapshot_db_manager = state_manager
        .get_storage_manager()
        .get_snapshot_manager()
        .get_snapshot_db_manager();
    snapshot_db_manager.update_latest_snapshot_id(parent_id, 4);
    let (fault_reached, release_fault) =
        install_isolated_mpt_parent_failure_hook(&state_manager, &rebuild);

    let storage_manager = state_manager.get_storage_manager_arc().clone();
    StorageManager::check_make_register_snapshot_background(
        storage_manager.clone(),
        target_id.clone(),
        rebuild.snapshot_info.height,
        Some(rebuild.delta_mpt.clone()),
        false,
    )
    .unwrap();
    assert!(wait_for_condition(Duration::from_secs(10), || {
        fault_reached.load(std::sync::atomic::Ordering::Acquire)
    }));
    release_fault.wait();
    assert!(wait_for_condition(Duration::from_secs(10), || {
        !storage_manager
            .in_progress_snapshotting_tasks
            .read()
            .contains_key(&target_id)
    }));

    snapshot_db_manager.set_snapshot_merge_test_hook(None);
    let retry_reached = Arc::new(std::sync::Barrier::new(2));
    let release_retry = Arc::new(std::sync::Barrier::new(2));
    let retry_reached_for_hook = retry_reached.clone();
    let release_retry_for_hook = release_retry.clone();
    let retry_target_id = target_id.clone();
    storage_manager.set_snapshot_task_before_writer_test_hook(Some(Arc::new(
        move |snapshot_epoch_id| {
            if *snapshot_epoch_id == retry_target_id {
                retry_reached_for_hook.wait();
                release_retry_for_hook.wait();
            }
            Ok(())
        },
    )));
    StorageManager::check_make_register_snapshot_background(
        storage_manager.clone(),
        target_id.clone(),
        rebuild.snapshot_info.height,
        Some(rebuild.delta_mpt),
        false,
    )
    .unwrap();
    retry_reached.wait();
    assert!(storage_manager
        .in_progress_snapshotting_tasks
        .read()
        .contains_key(&target_id));
    release_retry.wait();
    assert!(wait_for_condition(Duration::from_secs(10), || {
        !storage_manager
            .in_progress_snapshotting_tasks
            .read()
            .contains_key(&target_id)
    }));
    storage_manager.set_snapshot_task_before_writer_test_hook(None);
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
