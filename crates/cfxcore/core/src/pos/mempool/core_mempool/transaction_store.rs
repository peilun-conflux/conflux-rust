// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

// Copyright 2021 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use crate::pos::mempool::{
    core_mempool::{
        index::{
            AccountTransactionIter, AccountTransactions, TTLIndex,
            TimelineIndex,
        },
        transaction::{MempoolTransaction, TimelineState},
    },
    logging::{LogEntry, LogEvent, LogSchema, TxnsLog},
};
use diem_config::config::MempoolConfig;
use diem_crypto::{hash::CryptoHash, HashValue};
use diem_logger::prelude::*;
use diem_types::{
    account_address::AccountAddress,
    mempool_status::{MempoolStatus, MempoolStatusCode},
    transaction::{SignedTransaction, TransactionPayload},
};
use std::{
    collections::{hash_map::Values, HashMap},
    time::Duration,
};

/// Per-signer pivot-decision transactions for one decision, keyed by sender
/// (one per signer), tagged with the decision `height` for the commit-time
/// sweep.
pub(crate) struct PivotDecisionSet {
    height: u64,
    signers: HashMap<AccountAddress, HashValue>,
}

impl PivotDecisionSet {
    fn new(height: u64) -> Self {
        Self {
            height,
            signers: HashMap::new(),
        }
    }

    pub(crate) fn iter(
        &self,
    ) -> impl Iterator<Item = (AccountAddress, HashValue)> + '_ {
        self.signers.iter().map(|(&addr, &hash)| (addr, hash))
    }
}

/// TransactionStore is in-memory storage for all transactions in mempool.
pub struct TransactionStore {
    // normal transactions
    transactions: AccountTransactions,
    // pivot decision helper structure
    pivot_decisions: HashMap<HashValue, PivotDecisionSet>,

    // Evicts txns after `system_transaction_timeout` so stalled commit
    // callbacks cannot clog the mempool indefinitely.
    system_ttl_index: TTLIndex,
    timeline_index: TimelineIndex,

    // Per-sender cap against Byzantine spam. Invariant: every removal
    // of `self.transactions` must route through `index_remove`.
    per_sender_count: HashMap<AccountAddress, usize>,
    capacity_per_sender: usize,
}

pub type PivotDecisionIter<'a> = Values<'a, HashValue, PivotDecisionSet>;

impl TransactionStore {
    pub(crate) fn new(config: &MempoolConfig) -> Self {
        assert!(
            config.capacity_per_sender > 0,
            "mempool.capacity_per_sender must be > 0",
        );
        Self {
            // main DS
            transactions: AccountTransactions::new(),
            pivot_decisions: HashMap::new(),

            // various indexes
            system_ttl_index: TTLIndex::new(Box::new(
                |t: &MempoolTransaction| t.expiration_time,
            )),
            timeline_index: TimelineIndex::new(),

            per_sender_count: HashMap::new(),
            capacity_per_sender: config.capacity_per_sender,
        }
    }

    /// Fetch transaction by account address + hash.
    pub(crate) fn get(&self, hash: &HashValue) -> Option<SignedTransaction> {
        if let Some(txn) = self.transactions.get(hash) {
            return Some(txn.txn.clone());
        }
        None
    }

    /// Fetch pivot decisions by pivot hash.
    pub(crate) fn get_pivot_decisions(
        &self, hash: &HashValue,
    ) -> Vec<HashValue> {
        if let Some(decisions) = self.pivot_decisions.get(hash) {
            decisions.signers.values().copied().collect()
        } else {
            vec![]
        }
    }

    /// Insert transaction into TransactionStore. Performs validation checks and
    /// updates indexes.
    pub(crate) fn insert(
        &mut self, mut txn: MempoolTransaction,
    ) -> MempoolStatus {
        let address = txn.get_sender();
        let hash = txn.get_hash();
        let has_tx = self.get(&hash).is_some();

        if has_tx {
            return MempoolStatus::new(MempoolStatusCode::Accepted);
        }

        if let TransactionPayload::PivotDecision(pivot_decision) =
            txn.txn.payload()
        {
            // Only the payload is signed, so a validator could mint
            // distinct-hash duplicates of one pivot decision by varying
            // `chain_id`; keep one per sender.
            let already_signed = self
                .pivot_decisions
                .get(&pivot_decision.hash())
                .is_some_and(|set| set.signers.contains_key(&address));
            if already_signed {
                return MempoolStatus::new(MempoolStatusCode::Accepted);
            }
        }

        let sender_entry = self.per_sender_count.entry(address).or_insert(0);
        if *sender_entry >= self.capacity_per_sender {
            let sender_count = *sender_entry;
            // Rate-limited so a sustained attack doesn't flood logs.
            diem_sample!(
                SampleRate::Duration(Duration::from_secs(60)),
                diem_warn!(
                    sender = %address,
                    sender_count = sender_count,
                    cap = self.capacity_per_sender,
                    "mempool: per-sender capacity reached, rejecting txn",
                )
            );
            return MempoolStatus::new(MempoolStatusCode::TooManyTransactions)
                .with_message(format!(
                    "sender {} already has {} transactions (cap {})",
                    address, sender_count, self.capacity_per_sender,
                ));
        }
        *sender_entry += 1;

        self.timeline_index.insert(&mut txn);
        self.system_ttl_index.insert(&txn);

        if let TransactionPayload::PivotDecision(pivot_decision) =
            txn.txn.payload()
        {
            let pivot_decision_hash = pivot_decision.hash();
            let entry = self
                .pivot_decisions
                .entry(pivot_decision_hash)
                .or_insert_with(|| {
                    PivotDecisionSet::new(pivot_decision.height)
                });
            diem_debug!("txpool::insert pivot {:?}", hash);
            entry.signers.insert(address, hash);
            self.transactions.insert(hash, txn, true);
        } else {
            self.transactions.insert(hash, txn, false);
        }
        diem_debug!(
            LogSchema::new(LogEntry::AddTxn)
                .txns(TxnsLog::new_txn(address, hash)),
            hash = hash,
            has_tx = has_tx
        );

        MempoolStatus::new(MempoolStatusCode::Accepted)
    }

    /// The one path every `self.transactions` removal takes: log it, then
    /// update the indexes.
    fn remove_logged(
        &mut self, hash: &HashValue, log: &mut TxnsLog,
    ) -> Option<MempoolTransaction> {
        let txn = self.transactions.remove(hash)?;
        log.add(txn.get_sender(), txn.get_hash());
        self.index_remove(&txn);
        Some(txn)
    }

    /// Removes a committed transaction by hash.
    pub(crate) fn commit_transaction(&mut self, hash: HashValue) {
        let mut txns_log = TxnsLog::new();
        self.remove_logged(&hash, &mut txns_log);
        diem_debug!(LogSchema::new(LogEntry::CleanCommittedTxn).txns(txns_log));
    }

    /// Sweeps every pivot-decision set at or below a committed height,
    /// including obsolete lower and conflicting same-height sets that can no
    /// longer commit.
    pub(crate) fn commit_pivot_height(&mut self, height: u64) {
        let mut txns_log = TxnsLog::new();
        let obsolete: Vec<HashValue> = self
            .pivot_decisions
            .iter()
            .filter(|(_, v)| v.height <= height)
            .map(|(k, _)| *k)
            .collect();
        for key in obsolete {
            if let Some(entry) = self.pivot_decisions.remove(&key) {
                for (_, tx_hash) in entry.signers {
                    self.remove_logged(&tx_hash, &mut txns_log);
                }
            }
        }
        diem_debug!(LogSchema::new(LogEntry::CleanCommittedTxn).txns(txns_log));
    }

    /// Removes transaction from all indexes.
    fn index_remove(&mut self, txn: &MempoolTransaction) {
        self.system_ttl_index.remove(&txn);
        self.timeline_index.remove(&txn);
        let sender = txn.get_sender();
        debug_assert!(
            self.per_sender_count.contains_key(&sender),
            "per_sender_count missing entry for {} at index_remove",
            sender,
        );
        if let Some(count) = self.per_sender_count.get_mut(&sender) {
            *count -= 1;
            if *count == 0 {
                self.per_sender_count.remove(&sender);
            }
        }
    }

    /// Read `count` transactions from timeline since `timeline_id`.
    /// Returns block of transactions and new last_timeline_id.
    pub(crate) fn read_timeline(
        &mut self, timeline_id: u64, count: usize,
    ) -> (Vec<SignedTransaction>, u64) {
        let mut batch = vec![];
        let mut last_timeline_id = timeline_id;
        for (_, hash) in self.timeline_index.read_timeline(timeline_id, count) {
            if let Some(txn) = self.transactions.get(&hash) {
                batch.push(txn.txn.clone());
                if let TimelineState::Ready(timeline_id) = txn.timeline_state {
                    last_timeline_id = timeline_id;
                }
            }
        }
        (batch, last_timeline_id)
    }

    pub(crate) fn timeline_range(
        &mut self, start_id: u64, end_id: u64,
    ) -> Vec<SignedTransaction> {
        self.timeline_index
            .timeline_range(start_id, end_id)
            .iter()
            .filter_map(|(_, hash)| {
                self.transactions.get(hash).map(|txn| txn.txn.clone())
            })
            .collect()
    }

    /// Garbage collect old transactions by system TTL.
    pub(crate) fn gc_by_system_ttl(&mut self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("System time is before UNIX_EPOCH");

        let mut gc_txns = self.system_ttl_index.gc(now);
        gc_txns.sort_by_key(|key| (key.address, key.hash));

        let mut gc_txns_log = TxnsLog::new();
        for key in gc_txns.iter() {
            if let Some(txn) = self.remove_logged(&key.hash, &mut gc_txns_log) {
                let sender = txn.get_sender();
                if let TransactionPayload::PivotDecision(pivot_decision) =
                    txn.txn.into_raw_transaction().into_payload()
                {
                    // Drop only this entry: other signers' live transactions
                    // share this set.
                    let pivot_decision_hash = pivot_decision.hash();
                    if let Some(entry) =
                        self.pivot_decisions.get_mut(&pivot_decision_hash)
                    {
                        entry.signers.remove(&sender);
                        if entry.signers.is_empty() {
                            self.pivot_decisions.remove(&pivot_decision_hash);
                        }
                    }
                }
            }
        }

        diem_debug!(LogSchema::event_log(
            LogEntry::GCRemoveTxns,
            LogEvent::SystemTTLExpiration
        )
        .txns(gc_txns_log));
    }

    pub(crate) fn iter(&self) -> AccountTransactionIter<'_> {
        self.transactions.iter()
    }

    pub(crate) fn iter_pivot_decision(&self) -> PivotDecisionIter<'_> {
        self.pivot_decisions.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cfx_types::H256;
    use diem_crypto::{
        bls::{BLSPrivateKey, BLSPublicKey},
        PrivateKey, SigningKey, Uniform,
    };
    use diem_types::{
        block_info::PivotBlockDecision,
        chain_id::ChainId,
        transaction::{RawTransaction, RetirePayload, TransactionPayload},
    };
    use std::time::Duration;

    fn store_with_cap(cap: usize) -> TransactionStore {
        let mut cfg = MempoolConfig::default();
        cfg.capacity_per_sender = cap;
        TransactionStore::new(&cfg)
    }

    // Address is arbitrary — `per_sender_count` keys on address only.
    fn new_sender() -> (BLSPrivateKey, BLSPublicKey, AccountAddress) {
        let sk = BLSPrivateKey::generate_for_testing();
        let pk = sk.public_key();
        (sk, pk, AccountAddress::random())
    }

    fn mk_txn(
        sk: &BLSPrivateKey, pk: &BLSPublicKey, sender: AccountAddress,
        nonce: u64,
    ) -> MempoolTransaction {
        let payload = TransactionPayload::Retire(RetirePayload {
            node_id: sender,
            votes: nonce,
        });
        let raw =
            RawTransaction::new(sender, payload, u64::MAX, ChainId::test());
        let sig = sk.sign(&raw);
        MempoolTransaction::new(
            SignedTransaction::new(raw, pk.clone(), sig),
            Duration::from_secs(3600),
            TimelineState::NotReady,
        )
    }

    fn pivot(height: u64, block_hash_byte: u8) -> PivotBlockDecision {
        PivotBlockDecision {
            block_hash: H256::from([block_hash_byte; 32]),
            height,
        }
    }

    fn mk_pivot_txn(
        sk: &BLSPrivateKey, sender: AccountAddress,
        decision: &PivotBlockDecision, chain_id: u64,
    ) -> MempoolTransaction {
        // Only the payload is signed, so varying `chain_id` gives a distinct
        // hash.
        let signed = RawTransaction::new_pivot_decision(
            sender,
            decision.clone(),
            ChainId::new(chain_id),
        )
        .sign(sk)
        .unwrap()
        .into_inner();
        MempoolTransaction::new(
            signed,
            Duration::from_secs(3600),
            TimelineState::NotReady,
        )
    }

    #[test]
    fn duplicate_pivot_decision_from_one_validator_stored_once() {
        let mut store = store_with_cap(128);
        let (sk, _pk, sender) = new_sender();
        let decision = PivotBlockDecision {
            block_hash: H256::from([9u8; 32]),
            height: 1,
        };
        let pivot_hash = decision.hash();

        assert_eq!(
            store.insert(mk_pivot_txn(&sk, sender, &decision, 1)).code,
            MempoolStatusCode::Accepted
        );
        assert_eq!(store.per_sender_count[&sender], 1);
        assert_eq!(store.get_pivot_decisions(&pivot_hash).len(), 1);

        // Distinct-hash duplicates (different `chain_id`) must not add entries.
        for chain_id in 2..=5 {
            assert_eq!(
                store
                    .insert(mk_pivot_txn(&sk, sender, &decision, chain_id))
                    .code,
                MempoolStatusCode::Accepted
            );
        }
        assert_eq!(store.per_sender_count[&sender], 1);
        assert_eq!(store.get_pivot_decisions(&pivot_hash).len(), 1);
    }

    #[test]
    fn gc_drops_only_expired_pivot_entry_keeping_other_validators() {
        let mut store = store_with_cap(8);
        let (sk_a, _, sender_a) = new_sender();
        let (sk_b, _, sender_b) = new_sender();
        let decision = PivotBlockDecision {
            block_hash: H256::from([7u8; 32]),
            height: 1,
        };
        let pivot_hash = decision.hash();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();

        // Validator A's transaction is already past its system TTL; B's is
        // still live.
        let mut a = mk_pivot_txn(&sk_a, sender_a, &decision, 1);
        a.expiration_time = Duration::from_secs(1);
        let mut b = mk_pivot_txn(&sk_b, sender_b, &decision, 1);
        b.expiration_time = now + Duration::from_secs(3600);
        assert_eq!(store.insert(a).code, MempoolStatusCode::Accepted);
        assert_eq!(store.insert(b).code, MempoolStatusCode::Accepted);
        assert_eq!(store.get_pivot_decisions(&pivot_hash).len(), 2);

        store.gc_by_system_ttl();

        // B's live entry survives; only A's expired entry is removed.
        assert_eq!(store.get_pivot_decisions(&pivot_hash).len(), 1);
        assert_eq!(store.per_sender_count.get(&sender_a), None);
        assert_eq!(store.per_sender_count[&sender_b], 1);

        // Dedup still holds for B afterwards.
        assert_eq!(
            store
                .insert(mk_pivot_txn(&sk_b, sender_b, &decision, 2))
                .code,
            MempoolStatusCode::Accepted
        );
        assert_eq!(store.get_pivot_decisions(&pivot_hash).len(), 1);
        assert_eq!(store.per_sender_count[&sender_b], 1);
    }

    /// Cleanup keys on committed height, not the envelope hash: committing d1
    /// via a `chain_id` wrapper never stored here still clears d1's set, plus
    /// every resident obsolete set (lower d0, conflicting same-height d2); a
    /// higher d_hi survives, and a repeat sweep is a no-op.
    #[test]
    fn cleanup_by_committed_height() {
        let mut store = store_with_cap(8);
        let (sk0, _, s0) = new_sender();
        let (sk1, _, s1) = new_sender();
        let (sk2, _, s2) = new_sender();
        let (sk_hi, _, s_hi) = new_sender();
        let d0 = pivot(100, 10);
        let d1 = pivot(120, 11);
        let d2 = pivot(120, 12); // same height as d1, conflicting branch
        let d_hi = pivot(121, 13);
        for (sk, s, d) in [(&sk0, s0, &d0), (&sk1, s1, &d1), (&sk2, s2, &d2)] {
            assert_eq!(
                store.insert(mk_pivot_txn(sk, s, d, 1)).code,
                MempoolStatusCode::Accepted
            );
        }
        assert_eq!(
            store.insert(mk_pivot_txn(&sk_hi, s_hi, &d_hi, 1)).code,
            MempoolStatusCode::Accepted
        );

        // The network commits d1 via a chain_id=2 wrapper never stored here.
        let committed_d1 = mk_pivot_txn(&sk1, s1, &d1, 2).get_hash();
        assert!(store.get(&committed_d1).is_none());
        store.commit_transaction(committed_d1);
        store.commit_pivot_height(d1.height);
        store.commit_pivot_height(d1.height); // repeat = no-op

        for (d, s) in [(&d0, &s0), (&d1, &s1), (&d2, &s2)] {
            assert!(store.get_pivot_decisions(&d.hash()).is_empty());
            assert_eq!(store.per_sender_count.get(s), None);
        }
        assert_eq!(store.get_pivot_decisions(&d_hi.hash()).len(), 1);
        assert_eq!(store.per_sender_count[&s_hi], 1);
    }

    #[test]
    fn per_sender_count_insert_and_commit_lifecycle() {
        let mut store = store_with_cap(3);
        let (sk, pk, sender) = new_sender();
        assert!(!store.per_sender_count.contains_key(&sender));

        let mut hashes = Vec::new();
        for n in 0..3 {
            let txn = mk_txn(&sk, &pk, sender, n);
            hashes.push(txn.get_hash());
            assert_eq!(store.insert(txn).code, MempoolStatusCode::Accepted);
        }
        assert_eq!(store.per_sender_count[&sender], 3);

        for (i, h) in hashes.iter().enumerate() {
            store.commit_transaction(*h);
            let remaining = 3 - (i + 1);
            if remaining == 0 {
                assert!(!store.per_sender_count.contains_key(&sender));
            } else {
                assert_eq!(store.per_sender_count[&sender], remaining);
            }
        }
    }

    #[test]
    fn per_sender_count_cap_rejects_without_growth() {
        let mut store = store_with_cap(2);
        let (sk, pk, sender) = new_sender();

        for n in 0..2 {
            assert_eq!(
                store.insert(mk_txn(&sk, &pk, sender, n)).code,
                MempoolStatusCode::Accepted
            );
        }
        assert_eq!(store.per_sender_count[&sender], 2);

        for n in 2..6 {
            assert_eq!(
                store.insert(mk_txn(&sk, &pk, sender, n)).code,
                MempoolStatusCode::TooManyTransactions
            );
            assert_eq!(store.per_sender_count[&sender], 2);
        }
    }

    #[test]
    fn per_sender_count_duplicate_hash_no_double_count() {
        let mut store = store_with_cap(8);
        let (sk, pk, sender) = new_sender();
        let txn = mk_txn(&sk, &pk, sender, 0);
        let dup = MempoolTransaction::new(
            txn.txn.clone(),
            txn.expiration_time,
            txn.timeline_state,
        );

        assert_eq!(store.insert(txn).code, MempoolStatusCode::Accepted);
        assert_eq!(store.per_sender_count[&sender], 1);

        assert_eq!(store.insert(dup).code, MempoolStatusCode::Accepted);
        assert_eq!(store.per_sender_count[&sender], 1);
    }

    #[test]
    #[should_panic(expected = "mempool.capacity_per_sender must be > 0")]
    fn capacity_per_sender_zero_panics_on_construction() {
        let _ = store_with_cap(0);
    }
}
