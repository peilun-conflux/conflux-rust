use primal::is_prime;
use static_assertions::assert_eq_size;

pub const DATASET_BYTES_INIT: u64 = 2 * (1 << 31);
pub const DATASET_BYTES_GROWTH: u64 = 1 << 24;
pub const CACHE_BYTES_INIT: u64 = 2 * (1 << 23);
pub const CACHE_BYTES_GROWTH: u64 = 1 << 16;

pub const POW_STAGE_LENGTH: u64 = 1 << 19;

/// Upper bound on the block height used to size the PoW cache/dataset.
///
/// A peer-supplied header carries an unvalidated `u64` height (the
/// parent-height check happens far later, in `verify_header_graph_ready_block`)
/// that flows into `get_cache_size`/`get_data_size` and the cache allocation
/// during `verify_pow`. Without a bound, an absurd height drives
/// `Vec::with_capacity` (cache.rs) into a multi-TB / abort allocation, and with
/// `overflow-checks = true` (the release profile) the `DATASET_BYTES_GROWTH *
/// stage` multiply panics outright -- a remote, unauthenticated DoS.
///
/// Heights are clamped to this value everywhere they feed PoW sizing. The clamp
/// is applied identically by every node, so it never causes consensus
/// divergence; honest block heights are far below it (mainnet block height is
/// ~3.6e8, this bound is ~2.1e9 -- roughly two decades of headroom at the
/// current rate) and are therefore never affected. Beyond the bound the clamp
/// only plateaus the PoW dataset growth, again uniformly across nodes. The
/// corresponding worst-case single cache is `CACHE_BYTES_INIT +
/// CACHE_BYTES_GROWTH * (MAX / POW_STAGE_LENGTH)` (~0.27 GiB), and all heights
/// at or above the bound share one memo stage.
pub const MAX_POW_CACHE_HEIGHT: u64 = 1 << 31;

/// Maximum number of distinct PoW-cache stages retained in memory.
///
/// The clamp above bounds the size of any single cache, but heights *below* the
/// clamp still map to distinct stages, and the cache map is otherwise
/// insert-only. An unauthenticated peer can therefore stream headers at many
/// distinct large-but-in-range heights (`MAX - k * POW_STAGE_LENGTH`) to retain
/// an unbounded number of caches. Bounding the map to the least-recently-used
/// `MAX_POW_CACHE_ENTRIES` stages caps retained cache memory to roughly this
/// many worst-case caches (peak adds the one being built plus any still
/// referenced by in-flight computations). Honest verification only ever touches
/// a tiny working set near the chain frontier (one stage spans
/// `POW_STAGE_LENGTH` ~= 5e5 blocks), so this never evicts a stage a node
/// legitimately needs.
pub const MAX_POW_CACHE_ENTRIES: usize = 8;
pub const POW_CACHE_ROUNDS: usize = 3;
pub const POW_MIX_BYTES: usize = 256;
pub const POW_ACCESSES: usize = 32;
pub const POW_DATASET_PARENTS: u32 = 256;
pub const POW_MOD: u32 = 1032193;
pub const POW_MOD_B: u32 = 11;

pub const POW_NK: u64 = 10;
pub const POW_N: u64 = 1 << POW_NK;
pub const POW_WARP_SIZE: u64 = 32;
pub const POW_DATA_PER_THREAD: u64 = POW_N / POW_WARP_SIZE;

pub const NODE_DWORDS: usize = NODE_WORDS / 2;
pub const NODE_WORDS: usize = NODE_BYTES / 4;
pub const NODE_BYTES: usize = 64;

pub fn stage(block_height: u64) -> u64 { block_height / POW_STAGE_LENGTH }

#[allow(dead_code)]
static CHARS: &'static [u8] = b"0123456789abcdef";

#[allow(dead_code)]
pub fn to_hex(bytes: &[u8]) -> String {
    let mut v = Vec::with_capacity(bytes.len() * 2);
    for &byte in bytes.iter() {
        v.push(CHARS[(byte >> 4) as usize]);
        v.push(CHARS[(byte & 0xf) as usize]);
    }

    unsafe { String::from_utf8_unchecked(v) }
}

pub fn get_cache_size(block_height: u64) -> usize {
    // TODO: Memoise
    // Clamp so an attacker-supplied height cannot drive the size unbounded or
    // overflow the multiply. See `MAX_POW_CACHE_HEIGHT`.
    let block_height = block_height.min(MAX_POW_CACHE_HEIGHT);
    let mut sz: u64 =
        CACHE_BYTES_INIT + CACHE_BYTES_GROWTH * stage(block_height);
    sz = sz - NODE_BYTES as u64;
    while !is_prime(sz / NODE_BYTES as u64) {
        sz = sz - 2 * NODE_BYTES as u64;
    }
    sz as usize
}

pub fn get_data_size(block_height: u64) -> usize {
    // TODO: Memoise
    // Clamp so an attacker-supplied height cannot drive the size unbounded or
    // overflow the multiply. See `MAX_POW_CACHE_HEIGHT`.
    let block_height = block_height.min(MAX_POW_CACHE_HEIGHT);
    let mut sz: u64 =
        DATASET_BYTES_INIT + DATASET_BYTES_GROWTH * stage(block_height);
    sz = sz - POW_MIX_BYTES as u64;
    while !is_prime(sz / POW_MIX_BYTES as u64) {
        sz = sz - 2 * POW_MIX_BYTES as u64;
    }
    sz as usize
}

pub type NodeBytes = [u8; NODE_BYTES];
pub type NodeWords = [u32; NODE_WORDS];
pub type NodeDwords = [u64; NODE_DWORDS];

assert_eq_size!(Node, NodeBytes, NodeWords, NodeDwords);

#[repr(C)]
pub union Node {
    pub dwords: NodeDwords,
    pub words: NodeWords,
    pub bytes: NodeBytes,
}

impl Clone for Node {
    fn clone(&self) -> Self {
        unsafe {
            Node {
                bytes: *&self.bytes,
            }
        }
    }
}

// We use `inline(always)` because I was experiencing an 100% slowdown and
// `perf` showed that these calls were taking up ~30% of the runtime. Adding
// these annotations fixes the issue. Remove at your peril, if and only if you
// have benchmarks to prove that this doesn't reintroduce the performance
// regression. It's not caused by the `debug_assert_eq!` either, your guess is
// as good as mine.
impl Node {
    #[inline(always)]
    pub fn as_bytes(&self) -> &NodeBytes { unsafe { &self.bytes } }

    #[inline(always)]
    pub fn as_bytes_mut(&mut self) -> &mut NodeBytes {
        unsafe { &mut self.bytes }
    }

    #[inline(always)]
    pub fn as_words(&self) -> &NodeWords { unsafe { &self.words } }

    #[inline(always)]
    pub fn as_words_mut(&mut self) -> &mut NodeWords {
        unsafe { &mut self.words }
    }

    #[inline(always)]
    pub fn as_dwords(&self) -> &NodeDwords { unsafe { &self.dwords } }

    #[inline(always)]
    pub fn as_dwords_mut(&mut self) -> &mut NodeDwords {
        unsafe { &mut self.dwords }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        get_cache_size, get_data_size, MAX_POW_CACHE_HEIGHT, POW_STAGE_LENGTH,
    };

    // An attacker-supplied header height must not overflow the sizing multiply
    // (the test/release profiles enable `overflow-checks`) nor return an
    // unbounded size. Every out-of-range height must collapse onto the clamp.
    #[test]
    fn sizing_clamps_out_of_range_height() {
        let bound_cache = get_cache_size(MAX_POW_CACHE_HEIGHT);
        let bound_data = get_data_size(MAX_POW_CACHE_HEIGHT);

        for &h in &[
            MAX_POW_CACHE_HEIGHT + 1,
            MAX_POW_CACHE_HEIGHT + POW_STAGE_LENGTH,
            1u64 << 59, // historic overflow trigger for get_data_size
            u64::MAX,
        ] {
            assert_eq!(get_cache_size(h), bound_cache);
            assert_eq!(get_data_size(h), bound_data);
        }
    }

    // Honest heights are far below the bound and must be left untouched, so the
    // clamp can never change a real block's pow_hash.
    #[test]
    fn sizing_untouched_below_bound() {
        let h = 1_000_000_000u64; // above current tip, still << MAX_POW_CACHE_HEIGHT
        assert!(get_cache_size(h) < get_cache_size(MAX_POW_CACHE_HEIGHT));
        assert!(get_data_size(h) < get_data_size(MAX_POW_CACHE_HEIGHT));
    }
}
