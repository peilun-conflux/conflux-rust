use crate::{
    block_data_manager::{
        BlockExecutionResultWithEpoch, CheckpointHashes,
        ConsensusGraphExecutionInfo, DBType, EpochExecutionContext,
        LocalBlockInfo,
    },
    db::{COL_BLOCKS, COL_EPOCH_NUMBER, COL_MISC, COL_TX_ADDRESS},
    storage::{storage_db::KeyValueDbTrait, KvdbRocksdb, KvdbSqlite},
    verification::VerificationConfig,
};
use byteorder::{ByteOrder, LittleEndian};
use cfx_types::H256;
use db::SystemDB;
use primitives::{Block, BlockHeader, SignedTransaction, TransactionAddress};
use rlp::{Decodable, Encodable, Rlp};
use std::{collections::HashMap, fs, path::Path, str::FromStr, sync::Arc};

use flate2::Compression;
use flate2::write::ZlibEncoder;
use flate2::read::GzDecoder;


use metrics::{register_meter_with_group, Meter, MeterTimer};

lazy_static! {
    static ref SYNC_INSERT_HEADER_LOCAL_BLOCK_FROM_DB: Arc<dyn Meter> =
    register_meter_with_group("timer", "sync::insert_header_local_block_from_db");
    static ref SYNC_INSERT_HEADER_LOAD_DECODABLE_VAL_1: Arc<dyn Meter> =
    register_meter_with_group("timer", "sync::insert_header_load_decodable_val_1");
    static ref SYNC_INSERT_HEADER_LOAD_DECODABLE_VAL_2: Arc<dyn Meter> =
    register_meter_with_group("timer", "sync::insert_header_load_decodable_val_2");
}

const LOCAL_BLOCK_INFO_SUFFIX_BYTE: u8 = 1;
const BLOCK_BODY_SUFFIX_BYTE: u8 = 2;
const BLOCK_EXECUTION_RESULT_SUFFIX_BYTE: u8 = 3;
const EPOCH_EXECUTION_CONTEXT_SUFFIX_BYTE: u8 = 4;
const EPOCH_CONSENSUS_EXECUTION_INFO_SUFFIX_BYTE: u8 = 5;

#[derive(Clone, Copy, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub enum DBTable {
    Misc,
    Blocks,
    Transactions,
    EpochNumbers,
}

impl FromStr for DBTable {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "misc" => Ok(DBTable::Misc),
            "blocks" => Ok(DBTable::Blocks),
            "transactions" => Ok(DBTable::Transactions),
            "epoch_numbers" => Ok(DBTable::EpochNumbers),
            _ => Err(()),
        }
    }
}


fn rocks_db_col(table: DBTable) -> Option<u32> {
    match table {
        DBTable::Misc => COL_MISC,
        DBTable::Blocks => COL_BLOCKS,
        DBTable::Transactions => COL_TX_ADDRESS,
        DBTable::EpochNumbers => COL_EPOCH_NUMBER,
    }
}

fn sqlite_db_table(table: DBTable) -> String {
    match table {
        DBTable::Misc => "misc",
        DBTable::Blocks => "blocks",
        DBTable::Transactions => "transactions",
        DBTable::EpochNumbers => "epoch_numbers",
    }
    .into()
}

pub struct DBManager {
    table_db_map:
        HashMap<DBTable, Box<dyn KeyValueDbTrait<ValueType = Box<[u8]>>>>,
}

impl DBManager {
    /// Use sqlite for block-related data and use rocksdb for
    /// transaction-related data sqlite has less write amplification for
    /// large data, but rocksdb is more suitable for small updates.
    pub fn new(
        rocksdb: Arc<SystemDB>, sqlite_db_path: &Path,
        db_types: &HashMap<DBTable, DBType>,
    ) -> Self
    {
        if let Err(e) = fs::create_dir_all(sqlite_db_path) {
            panic!("Error creating database directory: {:?}", e);
        }
        let mut table_db_map = HashMap::new();

        for table in vec![
            DBTable::Misc,
            DBTable::Blocks,
            DBTable::EpochNumbers,
            DBTable::Transactions,
        ] {
            let table_db = match db_types.get(&table) {
                Some(DBType::Rocksdb) => {
                    Self::new_table_rocksdb(table, rocksdb.clone())
                }
                Some(DBType::Sqlite) => {
                    Self::new_table_sqlite(table, sqlite_db_path)
                }
                None => {
                    // TODO support in_mem db
                    unimplemented!()
                }
            };
            table_db_map.insert(table, table_db);
        }

        Self { table_db_map }
    }

    fn new_table_sqlite(
        table: DBTable, db_path: &Path,
    ) -> Box<dyn KeyValueDbTrait<ValueType = Box<[u8]>>> {
        let table_str = sqlite_db_table(table);
        let sqlite_db = KvdbSqlite::create_and_open(
            &db_path.join(table_str.as_str()), /* Use separate database
                                                * for
                                                * different table */
            table_str.as_str(),
            &[&"value"],
            &[&"BLOB"],
            false,
        )
        .expect("Open sqlite failure");
        Box::new(sqlite_db) as Box<dyn KeyValueDbTrait<ValueType = Box<[u8]>>>
    }

    fn new_table_rocksdb(
        table: DBTable, db: Arc<SystemDB>,
    ) -> Box<dyn KeyValueDbTrait<ValueType = Box<[u8]>>> {
        Box::new(KvdbRocksdb {
            kvdb: db.key_value().clone(),
            col: rocks_db_col(table),
        }) as Box<dyn KeyValueDbTrait<ValueType = Box<[u8]>>>
    }
}

impl DBManager {
    /// TODO Use new_with_rlp_size
    pub fn block_from_db(&self, block_hash: &H256) -> Option<Block> {
        Some(Block::new(
            self.block_header_from_db(block_hash)?,
            self.block_body_from_db(block_hash)?,
        ))
    }

    pub fn insert_block_header_to_db(&self, header: &BlockHeader) {
        self.insert_encodable_val(
            DBTable::Blocks,
            header.hash().as_bytes(),
            header,
        );
    }

    pub fn block_header_from_db(&self, hash: &H256) -> Option<BlockHeader> {
        let mut block_header =
            self.load_decodable_val(DBTable::Blocks, hash.as_bytes())?;
        VerificationConfig::compute_header_pow_quality(&mut block_header);
        Some(block_header)
    }

    pub fn remove_block_header_from_db(&self, hash: &H256) {
        self.remove_from_db(DBTable::Blocks, hash.as_bytes());
    }

    pub fn insert_transaction_address_to_db(
        &self, hash: &H256, value: &TransactionAddress,
    ) {
        self.insert_encodable_val(DBTable::Transactions, hash.as_bytes(), value)
    }

    pub fn transaction_address_from_db(
        &self, hash: &H256,
    ) -> Option<TransactionAddress> {
        self.load_decodable_val(DBTable::Transactions, hash.as_bytes())
    }

    /// Store block info to db. Block info includes block status and
    /// the sequence number when the block enters consensus graph.
    /// The db key is the block hash plus one extra byte, so we can get better
    /// data locality if we get both a block and its info from db.
    /// The info is not a part of the block because the block is inserted
    /// before we know its info, and we do not want to insert a large chunk
    /// again. TODO Maybe we can use in-place modification (operator `merge`
    /// in rocksdb) to keep the info together with the block.
    pub fn insert_local_block_info_to_db(
        &self, block_hash: &H256, value: &LocalBlockInfo,
    ) {
        self.insert_encodable_val(
            DBTable::Blocks,
            &local_block_info_key(block_hash),
            value,
        );
    }

    /// Get block info from db.
    pub fn local_block_info_from_db(
        &self, block_hash: &H256,
    ) -> Option<LocalBlockInfo> {
        let _timer = MeterTimer::time_func(SYNC_INSERT_HEADER_LOCAL_BLOCK_FROM_DB.as_ref());
        self.load_decodable_val(
            DBTable::Blocks,
            &local_block_info_key(block_hash),
        )
    }

    pub fn insert_block_body_to_db(&self, block: &Block) {
        self.insert_to_db(
            DBTable::Blocks,
            &block_body_key(&block.hash()),
            block.encode_body_with_tx_public(),
        )
    }

    pub fn block_body_from_db(
        &self, hash: &H256,
    ) -> Option<Vec<Arc<SignedTransaction>>> {
        let encoded =
            self.load_from_db(DBTable::Blocks, &block_body_key(hash))?;
        let rlp = Rlp::new(&encoded);
        Some(
            Block::decode_body_with_tx_public(&rlp)
                .expect("Wrong block rlp format!"),
        )
    }

    pub fn remove_block_body_from_db(&self, hash: &H256) {
        self.remove_from_db(DBTable::Blocks, &block_body_key(hash))
    }

    pub fn insert_block_execution_result_to_db(
        &self, hash: &H256, value: &BlockExecutionResultWithEpoch,
    ) {
        self.insert_encodable_val(
            DBTable::Blocks,
            &block_execution_result_key(hash),
            value,
        )
    }

    pub fn block_execution_result_from_db(
        &self, hash: &H256,
    ) -> Option<BlockExecutionResultWithEpoch> {
        self.load_decodable_val(
            DBTable::Blocks,
            &block_execution_result_key(hash),
        )
    }

    pub fn insert_checkpoint_hashes_to_db(
        &self, checkpoint_prev: &H256, checkpoint_cur: &H256,
    ) {
        self.insert_encodable_val(
            DBTable::Misc,
            b"checkpoint",
            &CheckpointHashes::new(*checkpoint_prev, *checkpoint_cur),
        );
    }

    pub fn checkpoint_hashes_from_db(&self) -> Option<(H256, H256)> {
        let checkpoints: CheckpointHashes =
            self.load_decodable_val(DBTable::Misc, b"checkpoint")?;
        Some((checkpoints.prev_hash, checkpoints.cur_hash))
    }

    pub fn insert_epoch_set_hashes_to_db(
        &self, epoch: u64, hashes: &Vec<H256>,
    ) {
        self.insert_encodable_list(
            DBTable::EpochNumbers,
            &epoch_set_key(epoch)[0..8],
            hashes,
        );
    }

    pub fn epoch_set_hashes_from_db(&self, epoch: u64) -> Option<Vec<H256>> {
        self.load_decodable_list(
            DBTable::EpochNumbers,
            &epoch_set_key(epoch)[0..8],
        )
    }

    pub fn insert_terminals_to_db(&self, terminals: &Vec<H256>) {
        self.insert_encodable_list(DBTable::Misc, b"terminals", terminals);
    }

    pub fn terminals_from_db(&self) -> Option<Vec<H256>> {
        self.load_decodable_list(DBTable::Misc, b"terminals")
    }

    pub fn insert_consensus_graph_execution_info_to_db(
        &self, hash: &H256, ctx: &ConsensusGraphExecutionInfo,
    ) {
        self.insert_encodable_val(
            DBTable::Blocks,
            &epoch_consensus_execution_info_key(hash),
            ctx,
        );
    }

    pub fn consensus_graph_execution_info_from_db(
        &self, hash: &H256,
    ) -> Option<ConsensusGraphExecutionInfo> {
        self.load_decodable_val(
            DBTable::Blocks,
            &epoch_consensus_execution_info_key(hash),
        )
    }

    pub fn insert_instance_id_to_db(&self, instance_id: u64) {
        self.insert_encodable_val(DBTable::Misc, b"instance", &instance_id);
    }

    pub fn instance_id_from_db(&self) -> Option<u64> {
        self.load_decodable_val(DBTable::Misc, b"instance")
    }

    pub fn insert_execution_context_to_db(
        &self, hash: &H256, ctx: &EpochExecutionContext,
    ) {
        self.insert_encodable_val(
            DBTable::Blocks,
            &epoch_execution_context_key(hash),
            ctx,
        )
    }

    pub fn execution_context_from_db(
        &self, hash: &H256,
    ) -> Option<EpochExecutionContext> {
        self.load_decodable_val(
            DBTable::Blocks,
            &epoch_execution_context_key(hash),
        )
    }

    /// The functions below are private utils used by the DBManager to access
    /// database
    fn insert_to_db(&self, table: DBTable, db_key: &[u8], value: Vec<u8>) {
//        let mut e = GzEncoder::new(value, Compression::default());
//        let compressed_value = e.finish();
        self.table_db_map
            .get(&table)
            .unwrap()
            .put(db_key, &value)
            .ok();
    }

    fn remove_from_db(&self, table: DBTable, db_key: &[u8]) {
        self.table_db_map.get(&table).unwrap().delete(db_key).ok();
    }

    fn load_from_db(&self, table: DBTable, db_key: &[u8]) -> Option<Box<[u8]>> {
        let _timer1 = MeterTimer::time_func(SYNC_INSERT_HEADER_LOAD_DECODABLE_VAL_1.as_ref());
        let tmp =self.table_db_map.get(&table).unwrap();
        drop(_timer1);
        let _timer2 = MeterTimer::time_func(SYNC_INSERT_HEADER_LOAD_DECODABLE_VAL_2.as_ref());
        tmp.get(db_key).unwrap()
//        let mut d = GzDecoder::new(value.as_ref());
//        d.read()
    }

    fn insert_encodable_val<V>(
        &self, table: DBTable, db_key: &[u8], value: &V,
    ) where V: Encodable {
        self.insert_to_db(table, db_key, rlp::encode(value))
    }

    fn insert_encodable_list<V>(
        &self, table: DBTable, db_key: &[u8], value: &Vec<V>,
    ) where V: Encodable {
        self.insert_to_db(table, db_key, rlp::encode_list(value))
    }

    fn load_decodable_val<V>(
        &self, table: DBTable, db_key: &[u8],
    ) -> Option<V>
    where V: Decodable {
        let encoded = self.load_from_db(table, db_key)?;
        Some(Rlp::new(&encoded).as_val().expect("decode succeeds"))
    }

    fn load_decodable_list<V>(
        &self, table: DBTable, db_key: &[u8],
    ) -> Option<Vec<V>>
    where V: Decodable {
        let encoded = self.load_from_db(table, db_key)?;
        Some(Rlp::new(&encoded).as_list().expect("decode succeeds"))
    }
}

fn append_suffix(h: &H256, suffix: u8) -> Vec<u8> {
    let mut key = Vec::with_capacity(H256::len_bytes() + 1);
    key.extend_from_slice(h.as_bytes());
    key.push(suffix);
    key
}

fn local_block_info_key(block_hash: &H256) -> Vec<u8> {
    append_suffix(block_hash, LOCAL_BLOCK_INFO_SUFFIX_BYTE)
}

fn block_body_key(block_hash: &H256) -> Vec<u8> {
    append_suffix(block_hash, BLOCK_BODY_SUFFIX_BYTE)
}

fn epoch_set_key(epoch_number: u64) -> [u8; 8] {
    let mut epoch_key = [0; 8];
    LittleEndian::write_u64(&mut epoch_key[0..8], epoch_number);
    epoch_key
}

fn block_execution_result_key(hash: &H256) -> Vec<u8> {
    append_suffix(hash, BLOCK_EXECUTION_RESULT_SUFFIX_BYTE)
}

fn epoch_execution_context_key(hash: &H256) -> Vec<u8> {
    append_suffix(hash, EPOCH_EXECUTION_CONTEXT_SUFFIX_BYTE)
}

fn epoch_consensus_execution_info_key(hash: &H256) -> Vec<u8> {
    append_suffix(hash, EPOCH_CONSENSUS_EXECUTION_INFO_SUFFIX_BYTE)
}
