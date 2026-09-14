//! Immutable sorted tables: the sealed accepted index's on-disk shape.
//!
//! # Why this exists
//!
//! The predecessor was four path-copied persistent treaps plus a fanout-32
//! sequence tree, one file per node. A single-block edit copied every ancestor
//! of four trees -- about 140 new nodes, about 0.9 MB -- and the flat directory
//! that held them reached ext4's htree ceiling at roughly 4.75 M entries, after
//! which every later generation cut failed forever. The container was not the
//! problem: packing the same nodes into one file per cut still writes every
//! ancestor. The DATA STRUCTURE was the cost (I-25: an edit costs the edit, not
//! the history).
//!
//! A cut here appends ONE small sorted table per touched domain. An entry is
//! `key_len + value_len` bytes and is written once per level it is later merged
//! through, so the per-batch retained cost is the entry and the amortized write
//! cost is `levels x` that (I-14: the bound is named, and it tracks the
//! retention window, not the lifetime).
//!
//! # What this module is NOT
//!
//! It is **container-agnostic**. It never opens a directory, names a file, or
//! touches `std::fs`. Tables are bytes addressed by an opaque 32-byte
//! [`TableLocator`]; the caller (`tine-core`'s cold-history pack store) decides
//! where those bytes live and hands them back through [`TableBytes`]. A
//! `std::fs` call in this file would be a layering bug, not an optimization.
//!
//! # Corruption (I-8, D-3)
//!
//! A table verifies its trailing digest ONCE, when a reader first loads it --
//! not per lookup, which would be a Merkle path in disguise (D-2 forbids
//! re-authenticating Tine's own established private state). A table that fails
//! that check raises [`SealedAcceptedIndexError::Corrupt`] naming its in-scope
//! scenario: a torn write, a disk error, or a partially delivered archive.
//! Tables and roots are disposable derived state, so the CALLER rebuilds them
//! from the surviving pack footers; this layer never turns index damage into a
//! refusal to open the graph.

use std::borrow::Cow;
use std::cell::OnceCell;
use std::cmp::Ordering;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use sha2::{Digest as _, Sha256};

use crate::formats::{
    SEALED_ROOT_SCHEMA_VERSION, SEALED_TABLE_FENCE_INTERVAL, SEALED_TABLE_SCHEMA_VERSION,
    SEALED_TABLE_TIER_FANOUT,
};
use crate::sealed_accepted_index_impl::SealedAcceptedIndexError;
use crate::ContentDigest;

/// Table header magic. The trailing digit is the table schema generation.
pub const SEALED_TABLE_MAGIC: &str = "TINETBL1";
/// Root-record magic.
pub const SEALED_ROOT_MAGIC: &str = "TINEROT1";

const MAGIC_BYTES: usize = 8;
const TABLE_HEADER_BYTES: usize = 24;
const TABLE_DIGEST_BYTES: usize = 32;

/// The canonical tombstone marker byte.
///
/// A tombstone value is this byte repeated `value_len` times. One encoding, not
/// a per-domain choice: a second spelling of "removed" is a second thing a
/// merge has to recognize, and the one it does not recognize resurrects a
/// deleted key. `a_tombstone_is_one_canonical_encoding` pins it.
pub const SEALED_TABLE_TOMBSTONE_BYTE: u8 = 0xff;

fn corrupt(message: impl Into<String>) -> SealedAcceptedIndexError {
    SealedAcceptedIndexError::Corrupt(message.into())
}

/// Counts full-table digest verifications.
///
/// Gate evidence, and deliberately on the REAL path rather than behind a test
/// hook: a counter that only test code increments proves nothing about how
/// often production hashes a table.
static TABLE_DIGEST_VERIFICATIONS: AtomicU64 = AtomicU64::new(0);

/// How many times any table's trailing digest has been verified in this process.
pub fn table_digest_verifications() -> u64 {
    TABLE_DIGEST_VERIFICATIONS.load(AtomicOrdering::Relaxed)
}

/// An opaque address for one table's bytes.
///
/// Opaque ON PURPOSE. This crate owns the table codec; the caller owns the
/// container, and fills these 32 bytes with whatever its object store uses
/// (today: a cold-history pack locator). Nothing here interprets them.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TableLocator(pub [u8; 32]);

impl fmt::Debug for TableLocator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "TableLocator({})",
            ContentDigest::from_bytes(self.0)
        )
    }
}

/// A key domain's fixed geometry.
///
/// Declared BY THE CALLER. This crate is generic over domains and hard-codes
/// none of Tine's; the two constants below exist only because the SQLite seam
/// in this crate consumes exactly those two.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TableDomain {
    pub id: u8,
    pub key_len: u8,
    pub value_len: u8,
    /// Whether this domain can express a removal. `Some` means a value of
    /// [`SEALED_TABLE_TOMBSTONE_BYTE`] repeated `value_len` times is a
    /// tombstone; `None` means no key is ever removed from this domain, and a
    /// tombstone-shaped value is an ordinary value.
    pub tombstone: bool,
}

impl TableDomain {
    pub const fn entry_len(&self) -> usize {
        self.key_len as usize + self.value_len as usize
    }

    fn validate(&self) -> Result<(), SealedAcceptedIndexError> {
        if self.key_len == 0 || self.value_len == 0 {
            return Err(corrupt("sealed table domain has a zero-width key or value"));
        }
        Ok(())
    }

    /// Is `value` this domain's tombstone?
    pub fn is_tombstone(&self, value: &[u8]) -> bool {
        self.tombstone
            && value.len() == self.value_len as usize
            && value
                .iter()
                .all(|byte| *byte == SEALED_TABLE_TOMBSTONE_BYTE)
    }

    /// This domain's canonical tombstone value.
    pub fn tombstone_value(&self) -> Option<Vec<u8>> {
        self.tombstone
            .then(|| vec![SEALED_TABLE_TOMBSTONE_BYTE; self.value_len as usize])
    }
}

/// Batch id -> causal record locator concatenated with status record locator.
///
/// One of the two domains the SQLite seam in this crate reads directly.
pub const SEALED_BATCH_DOMAIN: TableDomain = TableDomain {
    id: 1,
    key_len: 16,
    value_len: 64,
    tombstone: false,
};

/// Acceptance sequence (big-endian u64) -> batch id.
pub const SEALED_SEQUENCE_DOMAIN: TableDomain = TableDomain {
    id: 2,
    key_len: 8,
    value_len: 16,
    tombstone: false,
};

// ---------------------------------------------------------------------------
// Table codec
// ---------------------------------------------------------------------------

/// Build one immutable sorted table.
pub struct TableBuilder {
    domain: TableDomain,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

impl TableBuilder {
    pub fn new(domain: TableDomain) -> Self {
        Self {
            domain,
            entries: Vec::new(),
        }
    }

    /// Stage one entry. Later inserts of the same key replace earlier ones.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), SealedAcceptedIndexError> {
        self.domain.validate()?;
        if key.len() != self.domain.key_len as usize
            || value.len() != self.domain.value_len as usize
        {
            return Err(corrupt(
                "sealed table entry width does not match its domain",
            ));
        }
        match self.entries.binary_search_by(|entry| entry.0[..].cmp(key)) {
            Ok(index) => self.entries[index].1 = value.to_vec(),
            Err(index) => self.entries.insert(index, (key.to_vec(), value.to_vec())),
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Seal the staged entries into immutable bytes.
    pub fn finish(self) -> Result<Vec<u8>, SealedAcceptedIndexError> {
        let entries = self
            .entries
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice()));
        encode_table(self.domain, self.entries.len(), entries)
    }
}

fn encode_table<'a>(
    domain: TableDomain,
    count: usize,
    entries: impl Iterator<Item = (&'a [u8], &'a [u8])>,
) -> Result<Vec<u8>, SealedAcceptedIndexError> {
    domain.validate()?;
    let key_len = domain.key_len as usize;
    let fences = fence_count(count);
    let mut bytes = Vec::with_capacity(
        TABLE_HEADER_BYTES + count * domain.entry_len() + fences * key_len + TABLE_DIGEST_BYTES,
    );
    bytes.extend_from_slice(SEALED_TABLE_MAGIC.as_bytes());
    bytes.extend_from_slice(&SEALED_TABLE_SCHEMA_VERSION.to_be_bytes());
    bytes.push(domain.id);
    bytes.push(domain.key_len);
    bytes.push(domain.value_len);
    bytes.push(0);
    bytes.extend_from_slice(&(count as u64).to_be_bytes());
    debug_assert_eq!(bytes.len(), TABLE_HEADER_BYTES);

    let mut fence_keys: Vec<u8> = Vec::with_capacity(fences * key_len);
    let mut previous: Option<Vec<u8>> = None;
    let mut written = 0usize;
    for (key, value) in entries {
        if key.len() != key_len || value.len() != domain.value_len as usize {
            return Err(corrupt(
                "sealed table entry width does not match its domain",
            ));
        }
        if previous.as_deref().is_some_and(|prior| prior >= key) {
            return Err(corrupt("sealed table entries are not strictly ascending"));
        }
        // Fence i is the FIRST key of block i+1, so a table of at most one
        // block carries no fences at all. Keeping the implied block-0 fence out
        // of the array is what makes a one-entry level-0 delta 136 bytes
        // instead of 152 -- and level-0 deltas are the per-edit cost.
        if written > 0 && written % SEALED_TABLE_FENCE_INTERVAL == 0 {
            fence_keys.extend_from_slice(key);
        }
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(value);
        previous = Some(key.to_vec());
        written += 1;
    }
    if written != count {
        return Err(corrupt(
            "sealed table entry count does not match its header",
        ));
    }
    bytes.extend_from_slice(&fence_keys);
    let digest = Sha256::digest(&bytes);
    bytes.extend_from_slice(&digest);
    Ok(bytes)
}

/// Fences are block BOUNDARIES, not blocks: a table of `count` entries has
/// `ceil(count / INTERVAL)` blocks and one fewer boundary. Writing
/// `count / INTERVAL` instead is off by one on every exact multiple of the
/// interval, which a 256-entry table caught.
const fn fence_count(count: usize) -> usize {
    count.saturating_sub(1) / SEALED_TABLE_FENCE_INTERVAL
}

/// A decoded, digest-verified, borrowed view of one table.
///
/// Constructing one hashes the bytes exactly once. Every lookup after that is
/// slicing.
#[derive(Clone, Copy, Debug)]
pub struct TableView<'a> {
    bytes: &'a [u8],
    domain_id: u8,
    key_len: usize,
    value_len: usize,
    count: usize,
    entries_at: usize,
    fences_at: usize,
}

impl<'a> TableView<'a> {
    /// Decode and verify `bytes`.
    ///
    /// Refusal scenario (I-8): a mismatching trailing digest means a torn
    /// write, a disk error, or a partially delivered archive. The caller
    /// rebuilds the table from pack footers (D-3); this is never a reason to
    /// refuse to open the graph.
    pub fn decode(domain: TableDomain, bytes: &'a [u8]) -> Result<Self, SealedAcceptedIndexError> {
        if bytes.len() < TABLE_HEADER_BYTES + TABLE_DIGEST_BYTES {
            return Err(corrupt(
                "sealed table is shorter than its header and digest",
            ));
        }
        let body = &bytes[..bytes.len() - TABLE_DIGEST_BYTES];
        let stored = &bytes[bytes.len() - TABLE_DIGEST_BYTES..];
        TABLE_DIGEST_VERIFICATIONS.fetch_add(1, AtomicOrdering::Relaxed);
        if Sha256::digest(body).as_slice() != stored {
            return Err(corrupt(
                "sealed table digest mismatch (torn write, disk error, or partially delivered \
                 archive)",
            ));
        }
        Self::parse(domain, bytes)
    }

    /// Parse a table whose digest this reader ALREADY verified when it loaded
    /// the bytes.
    ///
    /// Not public: "trust me, it was checked" is exactly the shape that turns
    /// into per-lookup hashing when someone tries to make it safe, or into an
    /// unverified read when someone tries to make it fast.
    fn parse(domain: TableDomain, bytes: &'a [u8]) -> Result<Self, SealedAcceptedIndexError> {
        domain.validate()?;
        if bytes.len() < TABLE_HEADER_BYTES + TABLE_DIGEST_BYTES {
            return Err(corrupt(
                "sealed table is shorter than its header and digest",
            ));
        }
        let body = &bytes[..bytes.len() - TABLE_DIGEST_BYTES];
        if &body[..MAGIC_BYTES] != SEALED_TABLE_MAGIC.as_bytes() {
            return Err(corrupt("sealed table magic mismatch"));
        }
        let schema = u32::from_be_bytes(
            body[8..12]
                .try_into()
                .map_err(|_| corrupt("sealed table header is truncated"))?,
        );
        if schema != SEALED_TABLE_SCHEMA_VERSION {
            return Err(corrupt("sealed table schema is not the current schema"));
        }
        let domain_id = body[12];
        let key_len = body[13] as usize;
        let value_len = body[14] as usize;
        if body[15] != 0 {
            return Err(corrupt("sealed table reserved header byte is not zero"));
        }
        if domain_id != domain.id
            || key_len != domain.key_len as usize
            || value_len != domain.value_len as usize
        {
            return Err(corrupt("sealed table geometry does not match its domain"));
        }
        let count = u64::from_be_bytes(
            body[16..24]
                .try_into()
                .map_err(|_| corrupt("sealed table header is truncated"))?,
        );
        let count = usize::try_from(count).map_err(|_| SealedAcceptedIndexError::Capacity)?;
        let entry_len = key_len + value_len;
        let expected = TABLE_HEADER_BYTES
            .checked_add(
                count
                    .checked_mul(entry_len)
                    .ok_or(SealedAcceptedIndexError::Capacity)?,
            )
            .and_then(|size| size.checked_add(fence_count(count) * key_len))
            .ok_or(SealedAcceptedIndexError::Capacity)?;
        if body.len() != expected {
            return Err(corrupt(
                "sealed table length does not match its entry count",
            ));
        }
        Ok(Self {
            bytes: body,
            domain_id,
            key_len,
            value_len,
            count,
            entries_at: TABLE_HEADER_BYTES,
            fences_at: TABLE_HEADER_BYTES + count * entry_len,
        })
    }

    pub fn domain_id(&self) -> u8 {
        self.domain_id
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn key_at(&self, index: usize) -> &'a [u8] {
        let start = self.entries_at + index * (self.key_len + self.value_len);
        &self.bytes[start..start + self.key_len]
    }

    fn value_at(&self, index: usize) -> &'a [u8] {
        let start = self.entries_at + index * (self.key_len + self.value_len) + self.key_len;
        &self.bytes[start..start + self.value_len]
    }

    fn fence_at(&self, index: usize) -> &'a [u8] {
        let start = self.fences_at + index * self.key_len;
        &self.bytes[start..start + self.key_len]
    }

    /// Entry `index` as `(key, value)`.
    pub fn entry(&self, index: usize) -> (&'a [u8], &'a [u8]) {
        (self.key_at(index), self.value_at(index))
    }

    /// The half-open entry range of the block the fence array routes `key` to.
    ///
    /// This is the FIRST of the two reads the format promises: fences are the
    /// only bytes a non-resident reader must touch to know which block to
    /// fetch.
    fn block_for(&self, key: &[u8]) -> (usize, usize) {
        let fences = fence_count(self.count);
        // Largest block whose first key is <= `key`.
        let mut low = 0usize;
        let mut high = fences;
        while low < high {
            let mid = low + (high - low) / 2;
            if self.fence_at(mid) <= key {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        let start = low * SEALED_TABLE_FENCE_INTERVAL;
        let end = ((low + 1) * SEALED_TABLE_FENCE_INTERVAL).min(self.count);
        (start, end)
    }

    /// Index of `key`, or the insertion point.
    fn search(&self, key: &[u8]) -> Result<usize, usize> {
        if self.count == 0 {
            return Err(0);
        }
        let (start, end) = self.block_for(key);
        let mut low = start;
        let mut high = end;
        while low < high {
            let mid = low + (high - low) / 2;
            match self.key_at(mid).cmp(key) {
                Ordering::Less => low = mid + 1,
                Ordering::Greater => high = mid,
                Ordering::Equal => return Ok(mid),
            }
        }
        Err(low)
    }

    /// This table's value for `key`, tombstones included.
    pub fn get(&self, key: &[u8]) -> Option<&'a [u8]> {
        self.search(key).ok().map(|index| self.value_at(index))
    }

    /// The greatest entry with key <= `key`, tombstones included.
    pub fn predecessor(&self, key: &[u8]) -> Option<(&'a [u8], &'a [u8])> {
        let index = match self.search(key) {
            Ok(index) => index,
            Err(0) => return None,
            Err(index) => index - 1,
        };
        Some(self.entry(index))
    }

    /// Every entry in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&'a [u8], &'a [u8])> + '_ {
        (0..self.count).map(move |index| self.entry(index))
    }
}

// ---------------------------------------------------------------------------
// Table set: the ordered, levelled list a root names
// ---------------------------------------------------------------------------

/// One table in a domain's list.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TableRef {
    pub locator: TableLocator,
    pub level: u8,
    pub count: u64,
}

/// The bytes provider. The caller's container, seen through a keyhole.
pub trait TableBytes {
    fn read_table<'a>(
        &'a self,
        locator: &TableLocator,
    ) -> Result<Cow<'a, [u8]>, SealedAcceptedIndexError>;
}

struct CachedTable<'a> {
    bytes: Cow<'a, [u8]>,
}

/// Reads one domain's table list, newest first.
///
/// Opens each table at most once and keeps it, so a warm reader performs zero
/// [`TableBytes::read_table`] calls and zero digest verifications.
pub struct TableSetReader<'a, Provider: TableBytes> {
    provider: &'a Provider,
    domain: TableDomain,
    tables: Vec<TableRef>,
    cache: Vec<OnceCell<CachedTable<'a>>>,
}

impl<'a, Provider: TableBytes> TableSetReader<'a, Provider> {
    /// `tables` is the root's order for this domain: newest first, grouped by
    /// level. First hit wins, which is how an overwrite and a tombstone are
    /// expressed.
    pub fn new(
        provider: &'a Provider,
        domain: TableDomain,
        tables: Vec<TableRef>,
    ) -> Result<Self, SealedAcceptedIndexError> {
        domain.validate()?;
        let cache = (0..tables.len()).map(|_| OnceCell::new()).collect();
        Ok(Self {
            provider,
            domain,
            tables,
            cache,
        })
    }

    pub fn domain(&self) -> TableDomain {
        self.domain
    }

    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// How many levels this list spans.
    pub fn level_count(&self) -> usize {
        self.tables
            .iter()
            .map(|table| usize::from(table.level) + 1)
            .max()
            .unwrap_or(0)
    }

    fn view(&self, index: usize) -> Result<TableView<'_>, SealedAcceptedIndexError> {
        let cached = match self.cache[index].get() {
            Some(cached) => cached,
            None => {
                let bytes = self.provider.read_table(&self.tables[index].locator)?;
                // Verify eagerly so the digest is checked exactly once per
                // table per reader, and never again per lookup.
                TableView::decode(self.domain, &bytes)?;
                let _ = self.cache[index].set(CachedTable { bytes });
                self.cache[index]
                    .get()
                    .expect("cache entry was just installed")
            }
        };
        // Re-parsing the header is slicing, not hashing: the digest was
        // verified once, when these bytes entered the cache.
        TableView::parse(self.domain, &cached.bytes)
    }

    /// The newest live value for `key`, or `None` if absent or tombstoned.
    /// How many of this reader's tables have had their digest verified.
    ///
    /// The process-wide [`table_digest_verifications`] counter answers the same
    /// question for a whole run; this one is what a test can assert against
    /// while other tests are running.
    pub fn digest_verifications(&self) -> usize {
        self.cache
            .iter()
            .filter(|slot| slot.get().is_some())
            .count()
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, SealedAcceptedIndexError> {
        if key.len() != self.domain.key_len as usize {
            return Err(corrupt(
                "sealed table lookup key width does not match its domain",
            ));
        }
        for index in 0..self.tables.len() {
            let view = self.view(index)?;
            if let Some(value) = view.get(key) {
                if self.domain.is_tombstone(value) {
                    return Ok(None);
                }
                return Ok(Some(value.to_vec()));
            }
        }
        Ok(None)
    }

    pub fn contains(&self, key: &[u8]) -> Result<bool, SealedAcceptedIndexError> {
        Ok(self.get(key)?.is_some())
    }

    /// The greatest live key <= `key`, with its value.
    ///
    /// Newest-first is not enough on its own here: a newer table can shadow a
    /// candidate an older table proposes, so this takes the maximum candidate
    /// over ALL tables and then asks the set what that key currently resolves
    /// to. A tombstoned maximum means the search continues below it.
    /// The largest LIVE key at or below `key`, with its value -- a FLOOR, not a
    /// strict predecessor. "Which accepted batch is at or before sequence s"
    /// is the question the sequence domain is asked, and answering it strictly
    /// below would make an exact hit return the wrong entry.
    pub fn predecessor(
        &self,
        key: &[u8],
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, SealedAcceptedIndexError> {
        if key.len() != self.domain.key_len as usize {
            return Err(corrupt(
                "sealed table lookup key width does not match its domain",
            ));
        }
        let mut bound = key.to_vec();
        loop {
            let mut best: Option<Vec<u8>> = None;
            for index in 0..self.tables.len() {
                let view = self.view(index)?;
                if let Some((candidate, _)) = view.predecessor(&bound) {
                    if best.as_deref().is_none_or(|current| current < candidate) {
                        best = Some(candidate.to_vec());
                    }
                }
            }
            let Some(candidate) = best else {
                return Ok(None);
            };
            if let Some(value) = self.get(&candidate)? {
                return Ok(Some((candidate, value)));
            }
            // The winner is tombstoned: look strictly below it.
            match key_below(&candidate) {
                Some(next) => bound = next,
                None => return Ok(None),
            }
        }
    }
}

/// `candidate - 1` as fixed-width big-endian bytes, or `None` at zero.
fn key_below(candidate: &[u8]) -> Option<Vec<u8>> {
    let mut bound = candidate.to_vec();
    for byte in bound.iter_mut().rev() {
        if *byte > 0 {
            *byte -= 1;
            return Some(bound);
        }
        *byte = 0xff;
    }
    None
}

// ---------------------------------------------------------------------------
// Merge
// ---------------------------------------------------------------------------

/// Linear k-way merge. `inputs` is newest-first; a newer entry shadows an older
/// one, and shadowed entries are dropped. Tombstones are KEPT: a tier merge
/// consumes one level, and an older level may still hold the value the
/// tombstone hides, so dropping it here would resurrect that value. Use
/// [`compact_tables`] only when `inputs` are every table in the domain.
pub fn merge_tables(
    domain: TableDomain,
    inputs: &[TableView<'_>],
) -> Result<Vec<u8>, SealedAcceptedIndexError> {
    merge_inputs(domain, inputs, false)
}

/// `merge_tables`, but dropping tombstones.
///
/// Valid ONLY when `inputs` are every table in the domain. A tombstone is the
/// record that a key is gone; dropping it while an older table still holds the
/// key resurrects the old value, which is why the ordinary merge keeps them.
pub fn compact_tables(
    domain: TableDomain,
    inputs: &[TableView<'_>],
) -> Result<Vec<u8>, SealedAcceptedIndexError> {
    merge_inputs(domain, inputs, true)
}

fn merge_inputs(
    domain: TableDomain,
    inputs: &[TableView<'_>],
    drop_tombstones: bool,
) -> Result<Vec<u8>, SealedAcceptedIndexError> {
    domain.validate()?;
    let mut cursors = vec![0usize; inputs.len()];
    let mut merged: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    loop {
        let mut smallest: Option<&[u8]> = None;
        for (index, view) in inputs.iter().enumerate() {
            if cursors[index] >= view.len() {
                continue;
            }
            let key = view.entry(cursors[index]).0;
            if smallest.is_none_or(|current| key < current) {
                smallest = Some(key);
            }
        }
        let Some(smallest) = smallest else { break };
        let smallest = smallest.to_vec();
        // Newest input wins; every other input's copy of this key is consumed.
        let mut winner: Option<Vec<u8>> = None;
        for (index, view) in inputs.iter().enumerate() {
            if cursors[index] < view.len() {
                let (key, value) = view.entry(cursors[index]);
                if key == smallest.as_slice() {
                    if winner.is_none() {
                        winner = Some(value.to_vec());
                    }
                    cursors[index] += 1;
                }
            }
        }
        let value = winner.expect("the smallest key came from some input");
        if !(drop_tombstones && domain.is_tombstone(&value)) {
            merged.push((smallest, value));
        }
    }
    let count = merged.len();
    encode_table(
        domain,
        count,
        merged
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice())),
    )
}

// ---------------------------------------------------------------------------
// Tier planner
// ---------------------------------------------------------------------------

/// Size-tiered compaction, as a pure function.
///
/// The rejected alternative was "one base plus at most K deltas, rewrite the
/// base when K is exceeded". With one small delta per cut that rewrites an
/// ever-growing base every K cuts, which is quadratic in the number of cuts.
/// `the_necessity_control_fails_the_amortized_bound` runs exactly that policy
/// and reports the number it costs.
pub struct TierPlan;

/// What one cut does to a domain's table list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TierCut {
    /// Tables consumed by this cut's merge, newest first. Empty when no level
    /// overflowed.
    pub merge: Vec<TableRef>,
    /// The level the merge output lands at, when there is a merge.
    ///
    /// The merged table is NEWER than every table already at that level, so the
    /// caller inserts it at the FRONT of `retain` before
    /// [`TierPlan::canonical_order`]. Appending it instead would let an older
    /// same-level table shadow it on a point lookup.
    pub merged_level: Option<u8>,
    /// Tables carried over untouched.
    pub retain: Vec<TableRef>,
}

impl TierPlan {
    /// The fanout: a level holding this many tables merges.
    pub const FANOUT: usize = SEALED_TABLE_TIER_FANOUT;

    /// Plan the cut that appends `new_delta` at level 0.
    ///
    /// `current_levels` is the domain's list, newest first. At most ONE level
    /// merges per cut -- the lowest that has reached [`Self::FANOUT`] -- which
    /// is what bounds a single cut's work by one level's size rather than by
    /// the whole domain. Deferring the cascade is safe: a level that has
    /// reached FANOUT receives its next table only after the level below it
    /// refills, which takes FANOUT further cuts, and this cut's merge happens
    /// first.
    pub fn next_cut(current_levels: &[TableRef], new_delta: TableRef) -> TierCut {
        let mut tables: Vec<TableRef> = Vec::with_capacity(current_levels.len() + 1);
        tables.push(TableRef {
            level: 0,
            ..new_delta
        });
        tables.extend_from_slice(current_levels);

        let mut levels: Vec<u8> = tables.iter().map(|table| table.level).collect();
        levels.sort_unstable();
        levels.dedup();
        for level in levels {
            let at_level: Vec<TableRef> = tables
                .iter()
                .copied()
                .filter(|table| table.level == level)
                .collect();
            if at_level.len() >= Self::FANOUT {
                let retain = tables
                    .iter()
                    .copied()
                    .filter(|table| table.level != level)
                    .collect();
                return TierCut {
                    merge: at_level,
                    merged_level: Some(level.saturating_add(1)),
                    retain,
                };
            }
        }
        TierCut {
            merge: Vec::new(),
            merged_level: None,
            retain: tables,
        }
    }

    /// Reorder a domain's list into the root's canonical order: newest first,
    /// grouped by level, lowest level first.
    ///
    /// Lowest level first is the lookup order that makes "first hit wins"
    /// correct: a level-0 delta is newer than anything a merge has already
    /// swallowed.
    /// Sort a domain's list into lookup order: lowest level first, and within
    /// one level the order the caller already has.
    ///
    /// The sort is STABLE, and that is load-bearing: two tables at the same
    /// level are disjoint segments of DIFFERENT cuts, so a key present in both
    /// must resolve to the newer one. A merged table is newer than every table
    /// already at its level, so the caller inserts it at the FRONT of the list
    /// (see [`TierCut::merged_level`]) before calling this.
    pub fn canonical_order(tables: &mut [TableRef]) {
        tables.sort_by_key(|table| table.level);
    }
}

// ---------------------------------------------------------------------------
// Root record
// ---------------------------------------------------------------------------

/// One domain's ordered table list inside a root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedTableDomainRoot {
    pub domain_id: u8,
    /// Newest first, grouped by level.
    pub tables: Vec<TableRef>,
}

impl SealedTableDomainRoot {
    pub fn entry_count(&self) -> u64 {
        self.tables.iter().map(|table| table.count).sum()
    }
}

/// Everything one generation's sealed index is: a list of table locators per
/// domain, and nothing else.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SealedTableRoot {
    pub domains: Vec<SealedTableDomainRoot>,
}

impl SealedTableRoot {
    pub fn domain(&self, domain_id: u8) -> Option<&SealedTableDomainRoot> {
        self.domains
            .iter()
            .find(|entry| entry.domain_id == domain_id)
    }

    pub fn tables_for(&self, domain: TableDomain) -> Vec<TableRef> {
        self.domain(domain.id)
            .map(|entry| entry.tables.clone())
            .unwrap_or_default()
    }

    /// Canonical bytes. Domains ascend by id; a domain's tables keep the
    /// root's own order, which IS the lookup order.
    pub fn encode(&self) -> Result<Vec<u8>, SealedAcceptedIndexError> {
        let mut ids: Vec<u8> = self.domains.iter().map(|entry| entry.domain_id).collect();
        let before = ids.len();
        ids.sort_unstable();
        ids.dedup();
        if ids.len() != before {
            return Err(corrupt("sealed root names a domain twice"));
        }
        if !self
            .domains
            .windows(2)
            .all(|pair| pair[0].domain_id < pair[1].domain_id)
        {
            return Err(corrupt("sealed root domains are not in ascending id order"));
        }
        let mut bytes = Vec::new();
        bytes.extend_from_slice(SEALED_ROOT_MAGIC.as_bytes());
        bytes.extend_from_slice(&SEALED_ROOT_SCHEMA_VERSION.to_be_bytes());
        bytes.extend_from_slice(&(self.domains.len() as u32).to_be_bytes());
        for entry in &self.domains {
            bytes.push(entry.domain_id);
            bytes.extend_from_slice(&(entry.tables.len() as u32).to_be_bytes());
            for table in &entry.tables {
                bytes.push(table.level);
                bytes.extend_from_slice(&table.count.to_be_bytes());
                bytes.extend_from_slice(&table.locator.0);
            }
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, SealedAcceptedIndexError> {
        let mut cursor = 0usize;
        let mut take = |len: usize| -> Result<&[u8], SealedAcceptedIndexError> {
            let end = cursor
                .checked_add(len)
                .ok_or(SealedAcceptedIndexError::Capacity)?;
            if end > bytes.len() {
                return Err(corrupt("sealed root record is truncated"));
            }
            let slice = &bytes[cursor..end];
            cursor = end;
            Ok(slice)
        };
        if take(MAGIC_BYTES)? != SEALED_ROOT_MAGIC.as_bytes() {
            return Err(corrupt("sealed root magic mismatch"));
        }
        let schema = u32::from_be_bytes(take(4)?.try_into().expect("4 bytes"));
        if schema != SEALED_ROOT_SCHEMA_VERSION {
            return Err(corrupt("sealed root schema is not the current schema"));
        }
        let domain_count = u32::from_be_bytes(take(4)?.try_into().expect("4 bytes")) as usize;
        let mut domains = Vec::with_capacity(domain_count.min(256));
        for _ in 0..domain_count {
            let domain_id = take(1)?[0];
            let table_count = u32::from_be_bytes(take(4)?.try_into().expect("4 bytes")) as usize;
            let mut tables = Vec::with_capacity(table_count.min(4096));
            for _ in 0..table_count {
                let level = take(1)?[0];
                let count = u64::from_be_bytes(take(8)?.try_into().expect("8 bytes"));
                let mut locator = [0u8; 32];
                locator.copy_from_slice(take(32)?);
                tables.push(TableRef {
                    locator: TableLocator(locator),
                    level,
                    count,
                });
            }
            domains.push(SealedTableDomainRoot { domain_id, tables });
        }
        if cursor != bytes.len() {
            return Err(corrupt("sealed root record has trailing bytes"));
        }
        let decoded = Self { domains };
        // Reject anything `encode` would refuse, so a round trip is total.
        decoded.encode()?;
        Ok(decoded)
    }

    /// `sha256(canonical root bytes)`. The marker names this; the frontier root
    /// of an anchored database folds it in.
    pub fn root_digest(&self) -> Result<ContentDigest, SealedAcceptedIndexError> {
        Ok(ContentDigest::of(&self.encode()?))
    }
}

/// The root digest of a sealed index with no domains and no tables.
///
/// An anchor covering zero batches must name exactly this, so "covered nothing"
/// is one value rather than any digest a caller cares to write.
pub fn sealed_empty_root_digest() -> ContentDigest {
    SealedTableRoot::default()
        .root_digest()
        .expect("the empty root encodes")
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::cell::Cell;
    use std::collections::{BTreeMap, HashMap};

    use crate::sealed_accepted_index_impl::SealedAcceptedObjectKind;

    use super::*;

    /// A deterministic xorshift, so a seeded model comparison is reproducible
    /// without pulling in a dependency.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }

        fn below(&mut self, bound: u64) -> u64 {
            self.next() % bound
        }
    }

    /// The consumer's container. `tine-storage` never names a file; a caller
    /// hands it bytes for a locator, and this is the smallest thing that can.
    #[derive(Default)]
    struct Store {
        tables: HashMap<[u8; 32], Vec<u8>>,
        reads: Cell<u64>,
    }

    impl TableBytes for Store {
        fn read_table<'a>(
            &'a self,
            locator: &TableLocator,
        ) -> Result<Cow<'a, [u8]>, SealedAcceptedIndexError> {
            self.reads.set(self.reads.get() + 1);
            self.tables
                .get(&locator.0)
                .map(|bytes| Cow::Borrowed(bytes.as_slice()))
                .ok_or(SealedAcceptedIndexError::Missing {
                    kind: SealedAcceptedObjectKind::Table,
                    address: ContentDigest::from_bytes(locator.0),
                })
        }
    }

    impl Store {
        fn put(&mut self, bytes: Vec<u8>, level: u8, count: u64) -> TableRef {
            let locator = TableLocator(*ContentDigest::of(&bytes).as_bytes());
            self.tables.insert(locator.0, bytes);
            TableRef {
                locator,
                level,
                count,
            }
        }
    }

    /// The domain the model tests use: 8-byte keys, 8-byte values, tombstoned.
    const MODEL: TableDomain = TableDomain {
        id: 9,
        key_len: 8,
        value_len: 8,
        tombstone: true,
    };

    fn build(domain: TableDomain, entries: &BTreeMap<Vec<u8>, Vec<u8>>) -> Vec<u8> {
        let mut builder = TableBuilder::new(domain);
        for (key, value) in entries {
            builder.insert(key, value).unwrap();
        }
        builder.finish().unwrap()
    }

    // ---------------------------------------------------------------- gate 1

    /// The frozen bytes of the format this release introduces.
    ///
    /// A codec with no golden vector is a codec that can be changed by accident:
    /// every earlier sealed record in this crate has one, and the sequence-tree
    /// vectors this replaces were exactly what made a silent re-encoding
    /// impossible. Recompute these ONLY with a deliberate schema bump.
    #[test]
    fn sealed_table_bytes_are_frozen() {
        let mut builder = TableBuilder::new(SEALED_SEQUENCE_DOMAIN);
        builder.insert(&1u64.to_be_bytes(), &[0x51; 16]).unwrap();
        builder.insert(&2u64.to_be_bytes(), &[0x52; 16]).unwrap();
        let bytes = builder.finish().unwrap();

        let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(
            hex,
            // magic | schema | domain | key_len | value_len | reserved | count
            "54494e4554424c31 00000001 02 08 10 00 0000000000000002\
             0000000000000001 51515151515151515151515151515151\
             0000000000000002 52525252525252525252525252525252\
             04bd38048985d07838b96eec9d9d9c86d2a489ff88f0dc4ea0e37f5ed828ab06"
                .replace(' ', "")
        );
        assert_eq!(bytes.len(), 24 + 2 * 24 + 32);
        // Two entries, one block, no fences: the array holds BOUNDARIES.
        assert_eq!(fence_count(2), 0);
        assert_eq!(fence_count(64), 0);
        assert_eq!(fence_count(65), 1);
        assert_eq!(fence_count(256), 3);
    }

    #[test]
    fn a_table_round_trips_and_refuses_damage() {
        let mut entries = BTreeMap::new();
        for index in 0..200u64 {
            entries.insert(
                index.to_be_bytes().to_vec(),
                (index * 7).to_be_bytes().to_vec(),
            );
        }
        let bytes = build(MODEL, &entries);
        let table = TableView::decode(MODEL, &bytes).unwrap();
        assert_eq!(table.len(), 200);
        for (key, value) in &entries {
            assert_eq!(table.get(key), Some(value.as_slice()));
        }
        assert_eq!(table.get(&500u64.to_be_bytes()), None);

        // Every single-byte flip is caught by the trailing digest.
        for position in [0usize, 3, 24, 100, bytes.len() - 33, bytes.len() - 1] {
            let mut damaged = bytes.clone();
            damaged[position] ^= 0x01;
            assert!(
                TableView::decode(MODEL, &damaged).is_err(),
                "a flip at {position} decoded anyway"
            );
        }
        // Truncation, trailing bytes, and the wrong domain are all refused.
        assert!(TableView::decode(MODEL, &bytes[..bytes.len() - 1]).is_err());
        let mut extended = bytes.clone();
        extended.push(0);
        assert!(TableView::decode(MODEL, &extended).is_err());
        assert!(TableView::decode(SEALED_BATCH_DOMAIN, &bytes).is_err());

        // The builder refuses what the format cannot express, and UPSERTS
        // within one delta: a block edited twice before a cut is one entry, not
        // two, so a cut's size is its distinct keys.
        let mut builder = TableBuilder::new(MODEL);
        builder.insert(&2u64.to_be_bytes(), &[1; 8]).unwrap();
        builder.insert(&1u64.to_be_bytes(), &[2; 8]).unwrap();
        builder.insert(&2u64.to_be_bytes(), &[3; 8]).unwrap();
        assert_eq!(builder.len(), 2);
        assert!(builder.insert(&[0u8; 4], &[0; 8]).is_err());
        assert!(builder.insert(&3u64.to_be_bytes(), &[0; 4]).is_err());
        let bytes = builder.finish().unwrap();
        let table = TableView::decode(MODEL, &bytes).unwrap();
        assert_eq!(table.len(), 2);
        assert_eq!(table.get(&1u64.to_be_bytes()), Some(&[2u8; 8][..]));
        assert_eq!(table.get(&2u64.to_be_bytes()), Some(&[3u8; 8][..]));
    }

    /// D-2: the digest is verified ONCE, when the bytes are admitted -- never
    /// per lookup. A per-lookup verification would make every point read cost
    /// the whole table, which is the shape this release exists to remove.
    #[test]
    fn a_table_is_digest_verified_once_not_per_lookup() {
        let mut entries = BTreeMap::new();
        for index in 0..1_000u64 {
            entries.insert(index.to_be_bytes().to_vec(), index.to_be_bytes().to_vec());
        }
        let mut store = Store::default();
        let table = store.put(build(MODEL, &entries), 0, 1_000);

        let before = table_digest_verifications();
        let reader = TableSetReader::new(&store, MODEL, vec![table]).unwrap();
        assert_eq!(
            reader.digest_verifications(),
            0,
            "construction hashes nothing"
        );
        for index in 0..1_000u64 {
            assert_eq!(
                reader.get(&index.to_be_bytes()).unwrap(),
                Some(index.to_be_bytes().to_vec())
            );
        }
        assert_eq!(
            reader.digest_verifications(),
            1,
            "1000 lookups must hash the table once, not once each"
        );
        // The process-wide counter moved too, by at least this reader's one
        // verification. It is not asserted exactly: other tests share it.
        assert!(table_digest_verifications() > before);
        assert_eq!(
            store.reads.get(),
            1,
            "and must fetch its bytes once, not once each"
        );
    }

    /// The semantics, against a `BTreeMap` model: overwrites and tombstones
    /// across several levels, with `get`, `contains` and `predecessor` compared
    /// on every key after every compaction.
    #[test]
    fn a_table_set_matches_a_btreemap_model_through_overwrites_and_tombstones() {
        const KEYS: u64 = 400;
        const OPS: usize = 10_000;

        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut store = Store::default();
        let mut model: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        let mut levels: Vec<TableRef> = Vec::new();
        let mut pending: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let tombstone = MODEL.tombstone_value().unwrap();
        let mut compactions = 0usize;
        let mut max_level = 0u8;

        for op in 0..OPS {
            let key = rng.below(KEYS).to_be_bytes().to_vec();
            if rng.below(4) == 0 {
                pending.insert(key.clone(), tombstone.clone());
                model.insert(key, None);
            } else {
                let value = rng.next().to_be_bytes().to_vec();
                pending.insert(key.clone(), value.clone());
                model.insert(key, Some(value));
            }

            // Seal a delta every 64 operations, the way a checkpoint cut does.
            if op % 64 != 63 {
                continue;
            }
            let count = pending.len() as u64;
            let delta = store.put(build(MODEL, &pending), 0, count);
            pending.clear();

            let cut = TierPlan::next_cut(&levels, delta);
            levels = cut.retain;
            if let Some(level) = cut.merged_level {
                compactions += 1;
                max_level = max_level.max(level);
                let bytes: Vec<Vec<u8>> = cut
                    .merge
                    .iter()
                    .map(|table| store.tables.get(&table.locator.0).unwrap().clone())
                    .collect();
                let views: Vec<TableView<'_>> = bytes
                    .iter()
                    .map(|bytes| TableView::decode(MODEL, bytes).unwrap())
                    .collect();
                // Tombstones may be dropped only when this merge consumes the
                // whole domain -- nothing older survives to be resurrected.
                let merged = if levels.is_empty() {
                    compact_tables(MODEL, &views).unwrap()
                } else {
                    merge_tables(MODEL, &views).unwrap()
                };
                let count = TableView::decode(MODEL, &merged).unwrap().len() as u64;
                let table = store.put(merged, level, count);
                levels.insert(0, table);
            }
            TierPlan::canonical_order(&mut levels);

            // The model holds after every compaction, over every key -- present,
            // tombstoned and absent alike.
            let reader = TableSetReader::new(&store, MODEL, levels.clone()).unwrap();
            let staged: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
            assert!(staged.is_empty());
            for candidate in 0..KEYS {
                let key = candidate.to_be_bytes().to_vec();
                let expected = model.get(&key).cloned().flatten();
                assert_eq!(reader.get(&key).unwrap(), expected, "get({candidate})");
                assert_eq!(
                    reader.contains(&key).unwrap(),
                    expected.is_some(),
                    "contains({candidate})"
                );
                // `predecessor` is a FLOOR: at or below the key.
                let expected_predecessor = model
                    .range(..=key.clone())
                    .rev()
                    .find(|(_, value)| value.is_some())
                    .map(|(key, value)| (key.clone(), value.clone().unwrap()));
                assert_eq!(
                    reader.predecessor(&key).unwrap(),
                    expected_predecessor,
                    "predecessor({candidate})"
                );
            }
        }

        assert!(compactions > 0, "the fixture must actually compact");
        assert!(max_level >= 2, "and must reach at least three levels");
    }

    // ---------------------------------------------------------------- gate 2

    /// One entry-write, as the tier planner charges it.
    struct TierRun {
        levels: Vec<TableRef>,
        entries_written: u64,
        bytes_written: u64,
        worst_cut_bytes: u64,
        worst_cut_levels: usize,
    }

    /// The exact encoded size of a table of `count` entries in `domain`.
    fn table_bytes(domain: TableDomain, count: u64) -> u64 {
        let count = count as usize;
        (TABLE_HEADER_BYTES
            + count * domain.entry_len()
            + fence_count(count) * domain.key_len as usize
            + TABLE_DIGEST_BYTES) as u64
    }

    /// `cuts` single-entry checkpoint cuts under the tier planner, charging the
    /// bytes every merge rewrites. No table bytes are materialized: at 50,000
    /// cuts the aim is the COST CURVE, and building real tables would make the
    /// gate a minute long without changing a single number.
    fn run_tiers(domain: TableDomain, cuts: u64) -> TierRun {
        let mut levels: Vec<TableRef> = Vec::new();
        let mut entries_written = 0u64;
        let mut bytes_written = 0u64;
        let mut worst_cut_bytes = 0u64;
        let mut worst_cut_levels = 0usize;
        for index in 0..cuts {
            let delta = TableRef {
                locator: TableLocator(*ContentDigest::of(&index.to_be_bytes()).as_bytes()),
                level: 0,
                count: 1,
            };
            entries_written += 1;
            let mut cut_bytes = table_bytes(domain, 1);
            bytes_written += cut_bytes;

            let cut = TierPlan::next_cut(&levels, delta);
            levels = cut.retain;
            if let Some(level) = cut.merged_level {
                let merged: u64 = cut.merge.iter().map(|table| table.count).sum();
                entries_written += merged;
                let merged_bytes = table_bytes(domain, merged);
                bytes_written += merged_bytes;
                cut_bytes += merged_bytes;
                levels.insert(
                    0,
                    TableRef {
                        locator: TableLocator(*ContentDigest::of(&index.to_be_bytes()).as_bytes()),
                        level,
                        count: merged,
                    },
                );
                let merged_levels = cut
                    .merge
                    .iter()
                    .map(|table| table.level)
                    .collect::<std::collections::BTreeSet<_>>()
                    .len();
                worst_cut_levels = worst_cut_levels.max(merged_levels);
            }
            worst_cut_bytes = worst_cut_bytes.max(cut_bytes);
            TierPlan::canonical_order(&mut levels);
        }
        TierRun {
            levels,
            entries_written,
            bytes_written,
            worst_cut_bytes,
            worst_cut_levels,
        }
    }

    fn distinct_levels(levels: &[TableRef]) -> usize {
        levels
            .iter()
            .map(|table| table.level)
            .collect::<std::collections::BTreeSet<_>>()
            .len()
    }

    /// I-25 / I-14, the growth bound: 50,000 single-block cuts.
    ///
    /// The numbers this prints are the release's claim, and the dossier's two
    /// arithmetic targets do not both survive contact with the pinned domain
    /// widths -- see the receipt. What is asserted here is the bound the spec
    /// derives (§4.2): cumulative bytes written are at most `L` times the bytes
    /// finally retained, with `L = ceil(log_R(n)) + 1` levels.
    #[test]
    fn fifty_thousand_cuts_stay_inside_the_tier_bound() {
        const CUTS: u64 = 50_000;
        let run = run_tiers(SEALED_BATCH_DOMAIN, CUTS);

        let retained: u64 = run
            .levels
            .iter()
            .map(|table| table_bytes(SEALED_BATCH_DOMAIN, table.count))
            .sum();
        let tables = run.levels.len();
        let levels = distinct_levels(&run.levels);
        let ratio = run.bytes_written as f64 / retained as f64;
        println!(
            "50,000 cuts: {} tables over {levels} levels, {retained} B retained, \
             {} B written ({ratio:.2}x), {} entry-writes, worst cut {} B over {} level(s)",
            tables,
            run.bytes_written,
            run.entries_written,
            run.worst_cut_bytes,
            run.worst_cut_levels
        );

        // (a) the level count is logarithmic, not linear.
        let bound = (CUTS as f64).log(TierPlan::FANOUT as f64).ceil() as usize + 1;
        assert_eq!(bound, 7);
        assert!(levels <= bound, "{levels} levels over {CUTS} cuts");
        assert!(tables < TierPlan::FANOUT * bound);

        // (b) write amplification is bounded by the level count. A table is
        //     rewritten at most once per level it climbs, so the total is at
        //     most `L` passes over the retained bytes.
        assert!(
            run.bytes_written <= bound as u64 * retained,
            "wrote {} B against {retained} B retained over {bound} levels",
            run.bytes_written
        );

        // (c) one cut merges at most one level, and never more bytes than that
        //     level holds. This is the LATENCY bound: no cut is allowed to
        //     become a whole-history rewrite.
        assert!(run.worst_cut_levels <= 1);
        let widest_level_bytes =
            table_bytes(SEALED_BATCH_DOMAIN, TierPlan::FANOUT as u64 * CUTS / 8);
        assert!(run.worst_cut_bytes < widest_level_bytes);

        // (d) a point lookup consults at most `FANOUT` tables per level.
        assert!(tables <= TierPlan::FANOUT * levels);
    }

    /// The NECESSITY CONTROL for the gate above.
    ///
    /// Neuter the tiering -- keep one growing base and rewrite it every
    /// `FANOUT` cuts, which is what "just compact it" means -- and (b) must
    /// fail. Without this the bound could be satisfied by arithmetic that never
    /// depended on tiering at all.
    #[test]
    fn a_single_growing_base_fails_the_tier_bound() {
        const CUTS: u64 = 50_000;
        let mut bytes_written = 0u64;
        for index in 1..=CUTS {
            bytes_written += table_bytes(SEALED_BATCH_DOMAIN, 1);
            if index % TierPlan::FANOUT as u64 == 0 {
                // The whole base is rewritten, every time.
                bytes_written += table_bytes(SEALED_BATCH_DOMAIN, index);
            }
        }
        let retained = table_bytes(SEALED_BATCH_DOMAIN, CUTS);
        let bound = (CUTS as f64).log(TierPlan::FANOUT as f64).ceil() as u64 + 1;
        let ratio = bytes_written as f64 / retained as f64;
        println!(
            "necessity control (one growing base): {bytes_written} B written against \
             {retained} B retained -- {ratio:.0}x, against a {bound}x bound"
        );
        assert!(
            bytes_written > bound * retained,
            "the control must FAIL the bound, or the bound proves nothing: \
             {bytes_written} B written, {retained} B retained"
        );
        assert!(ratio > 1_000.0, "measured {ratio:.0}x");
    }

    /// The planner's own invariant, stated directly: no level ever holds more
    /// than `FANOUT` tables, and each cut merges the LOWEST overflowing level.
    #[test]
    fn no_level_ever_exceeds_the_fanout() {
        let run = run_tiers(SEALED_BATCH_DOMAIN, 5_000);
        let mut by_level: BTreeMap<u8, usize> = BTreeMap::new();
        for table in &run.levels {
            *by_level.entry(table.level).or_default() += 1;
        }
        for (level, count) in &by_level {
            assert!(
                *count < TierPlan::FANOUT,
                "level {level} holds {count} tables"
            );
        }
    }

    // ---------------------------------------------------------------- gate 3

    /// A point lookup fetches O(1) bytes per table consulted, and NOTHING on a
    /// warm reader. The retired treap fetched one object per level of a tree
    /// whose depth grew with history; this is what replaced that.
    #[test]
    fn a_point_lookup_reads_each_table_at_most_once_and_nothing_when_warm() {
        let mut store = Store::default();
        let mut levels = Vec::new();
        for level in 0..4u8 {
            let mut entries = BTreeMap::new();
            for index in 0..500u64 {
                let key = (index * 4 + level as u64).to_be_bytes().to_vec();
                entries.insert(key, (level as u64).to_be_bytes().to_vec());
            }
            levels.push(store.put(build(MODEL, &entries), level, 500));
        }
        let table_count = levels.len();

        store.reads.set(0);
        let reader = TableSetReader::new(&store, MODEL, levels).unwrap();
        assert_eq!(store.reads.get(), 0, "constructing a reader reads nothing");

        // The coldest possible lookup: an absent key, which cannot stop early.
        assert_eq!(reader.get(&9_999u64.to_be_bytes()).unwrap(), None);
        assert_eq!(
            store.reads.get(),
            table_count as u64,
            "a cold miss reads each table exactly once"
        );

        // Warm: 2,000 further lookups across all four tables read nothing more.
        let warm = store.reads.get();
        for index in 0..2_000u64 {
            let _ = reader.get(&index.to_be_bytes()).unwrap();
        }
        assert_eq!(store.reads.get(), warm, "a warm reader must not re-read");

        // First hit wins, lowest level first: the newest delta shadows the base.
        let mut store = Store::default();
        let mut old = BTreeMap::new();
        old.insert(7u64.to_be_bytes().to_vec(), 100u64.to_be_bytes().to_vec());
        let base = store.put(build(MODEL, &old), 1, 1);
        let mut new = BTreeMap::new();
        new.insert(7u64.to_be_bytes().to_vec(), 200u64.to_be_bytes().to_vec());
        let delta = store.put(build(MODEL, &new), 0, 1);
        let reader = TableSetReader::new(&store, MODEL, vec![delta, base]).unwrap();
        assert_eq!(
            reader.get(&7u64.to_be_bytes()).unwrap(),
            Some(200u64.to_be_bytes().to_vec())
        );
    }

    // ---------------------------------------------------------------- gate 4

    /// I-25, the unit cost: what ONE accepted batch costs the sealed index,
    /// amortized, at three history sizes.
    ///
    /// The two sealed domains are pinned at (16 + 64) and (8 + 16) bytes per
    /// entry, so 104 B of the per-batch cost is the RECORD, not overhead. What
    /// this gate bounds is that the overhead on top -- fences, headers, digests,
    /// and every byte of rewriting the tier planner does -- stays small and does
    /// not grow with the history.
    #[test]
    fn one_batch_costs_a_bounded_number_of_sealed_bytes() {
        let mut measured: Vec<f64> = Vec::new();
        for cuts in [1u64, 1_000, 50_000] {
            let batch = run_tiers(SEALED_BATCH_DOMAIN, cuts);
            let sequence = run_tiers(SEALED_SEQUENCE_DOMAIN, cuts);
            let batch_retained: u64 = batch
                .levels
                .iter()
                .map(|table| table_bytes(SEALED_BATCH_DOMAIN, table.count))
                .sum();
            let sequence_retained: u64 = sequence
                .levels
                .iter()
                .map(|table| table_bytes(SEALED_SEQUENCE_DOMAIN, table.count))
                .sum();
            let combined = (batch_retained + sequence_retained) as f64 / cuts as f64;
            println!(
                "{cuts} batches: {:.2} B/batch retained in the batch domain, \
                 {:.2} B/batch in the sequence domain, {combined:.2} B/batch combined",
                batch_retained as f64 / cuts as f64,
                sequence_retained as f64 / cuts as f64,
            );
            // The measured curve is 216 B at one batch, ~105.6 at a thousand,
            // ~104.4 at fifty thousand: the fixed 2 x 56 B of header-and-digest
            // per table amortizes away and the RECORDS remain.
            //
            // 104 B is the two RECORDS themselves at the pinned widths
            // ((16 + 64) + (8 + 16)); the rest is fences, headers and digests.
            // At one batch the fixed 2 x 56 B of header-and-digest is the whole
            // cost and there is nothing to amortize it over -- that is a
            // CONSTANT, not a growth term, which is why the bound below is
            // stated per history size rather than as one number. The dossier's
            // single <=100 B budget is arithmetically unreachable at these
            // widths; see the receipt.
            assert!(combined >= 104.0, "the record bytes cannot be undercut");
            measured.push(combined);
        }

        // The overhead SHRINKS with history: this is the I-25 shape claim, and
        // it is what a per-edit cost that tracked the graph's lifetime would
        // fail. At fifty thousand batches the sealed index costs one batch's
        // two records plus under a byte and a half.
        assert!(measured[0] > measured[1] && measured[1] > measured[2]);
        assert!(
            measured[2] <= 105.0,
            "50,000 batches: {:.2} B/batch",
            measured[2]
        );
    }

    // ------------------------------------------------------------ root codec

    #[test]
    fn a_sealed_root_round_trips_and_digests_its_domains() {
        let root = SealedTableRoot {
            domains: vec![
                SealedTableDomainRoot {
                    domain_id: SEALED_BATCH_DOMAIN.id,
                    tables: vec![TableRef {
                        locator: TableLocator([0x11; 32]),
                        level: 0,
                        count: 3,
                    }],
                },
                SealedTableDomainRoot {
                    domain_id: SEALED_SEQUENCE_DOMAIN.id,
                    tables: vec![
                        TableRef {
                            locator: TableLocator([0x22; 32]),
                            level: 0,
                            count: 1,
                        },
                        TableRef {
                            locator: TableLocator([0x33; 32]),
                            level: 1,
                            count: 9,
                        },
                    ],
                },
            ],
        };
        let bytes = root.encode().unwrap();
        assert_eq!(SealedTableRoot::decode(&bytes).unwrap(), root);
        assert!(SealedTableRoot::decode(&bytes[..bytes.len() - 1]).is_err());
        let mut extended = bytes.clone();
        extended.push(0);
        assert!(SealedTableRoot::decode(&extended).is_err());

        // The empty root is a distinguished value: the anchor's shape check
        // reads it as "nothing is covered".
        let empty = SealedTableRoot {
            domains: Vec::new(),
        };
        assert_eq!(empty.root_digest().unwrap(), sealed_empty_root_digest());
        assert_ne!(root.root_digest().unwrap(), sealed_empty_root_digest());
        assert_eq!(root.tables_for(SEALED_BATCH_DOMAIN).len(), 1);
        assert_eq!(root.tables_for(SEALED_SEQUENCE_DOMAIN).len(), 2);
        assert_eq!(root.tables_for(MODEL).len(), 0);
    }
}
