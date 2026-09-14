use std::{cmp::Ordering, fmt, hash};

use serde::{
    de::{self, DeserializeOwned},
    Deserialize, Deserializer, Serialize, Serializer,
};

use crate::ContentDigest;

pub const SEALED_ACCEPTED_STATUS_SCHEMA_VERSION: u32 = 2;

pub const SEALED_ACCEPTED_CAUSAL_RECORD_SCHEMA_VERSION: u32 = 2;

/// Inclusive upper bound on one authenticated-map key, in bytes.
///
/// This is a **writer bound**: keys of up to this many bytes are already on
/// disk in sealed map nodes and in the SQLite frontier overlay, so lowering it
/// strands stored data. It is exported from [`crate::formats`], not from the
/// [`crate::sealed_accepted_index`] facade, because a reader must agree with a
/// writer about it.
pub const MAX_AUTHENTICATED_MAP_KEY_BYTES: usize = 48;

/// Bounded canonical key bytes supplied by the domain owner.
///
/// The authenticated map is deliberately domain-blind: it stores, orders and
/// authenticates opaque byte strings of 1..=[`MAX_AUTHENTICATED_MAP_KEY_BYTES`]
/// bytes and never parses them. A 16-byte identifier is one such key, so
/// [`From<[u8; 16]>`] keeps every fixed-width identity map (batch, status,
/// causal clock, causal tip) expressible without a second key type.
///
/// The inline buffer is fixed-width for `Copy`, but only the first `length`
/// bytes are ever meaningful. Equality, hashing, ordering, serialization and
/// every digest fold read [`Self::as_slice`], so the padding can neither change
/// a value's identity nor reach disk as a second representation of one key.
///
/// `Ord` is **lexicographic over the meaningful bytes**, which is the order the
/// treap's binary-search invariant and every sorted-unique precondition depend
/// on. It is deliberately not derived: a derived `Ord` on `(length, bytes)`
/// would sort by length first and silently disagree with the byte order both
/// the SQLite `BLOB` comparison and the domain owner use.
#[derive(Clone, Copy)]
pub struct AuthenticatedMapKey {
    length: u8,
    bytes: [u8; MAX_AUTHENTICATED_MAP_KEY_BYTES],
}

impl AuthenticatedMapKey {
    /// Accept exactly the keys a writer may legally have produced.
    pub fn new(bytes: &[u8]) -> Result<Self, SealedAcceptedIndexError> {
        if bytes.is_empty() || bytes.len() > MAX_AUTHENTICATED_MAP_KEY_BYTES {
            return Err(corrupt(format!(
                "authenticated-map key length {} is outside 1..={MAX_AUTHENTICATED_MAP_KEY_BYTES}",
                bytes.len()
            )));
        }
        let mut stored = [0_u8; MAX_AUTHENTICATED_MAP_KEY_BYTES];
        stored[..bytes.len()].copy_from_slice(bytes);
        Ok(Self {
            length: bytes.len() as u8,
            bytes: stored,
        })
    }

    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.length as usize]
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.length as usize
    }

    /// Always false: a key of zero bytes cannot be constructed.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        false
    }
}

impl From<[u8; 16]> for AuthenticatedMapKey {
    fn from(value: [u8; 16]) -> Self {
        let mut bytes = [0_u8; MAX_AUTHENTICATED_MAP_KEY_BYTES];
        bytes[..16].copy_from_slice(&value);
        Self { length: 16, bytes }
    }
}

impl PartialEq for AuthenticatedMapKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for AuthenticatedMapKey {}

impl hash::Hash for AuthenticatedMapKey {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state);
    }
}

impl Ord for AuthenticatedMapKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl PartialOrd for AuthenticatedMapKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for AuthenticatedMapKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "AuthenticatedMapKey({self})")
    }
}

impl fmt::Display for AuthenticatedMapKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.as_slice() {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl Serialize for AuthenticatedMapKey {
    /// One canonical representation: the meaningful bytes, length-prefixed by
    /// the byte-string encoding itself. The padding is never written.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.as_slice())
    }
}

impl<'de> Deserialize<'de> for AuthenticatedMapKey {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct KeyVisitor;

        impl KeyVisitor {
            fn build<E: de::Error>(bytes: &[u8]) -> Result<AuthenticatedMapKey, E> {
                AuthenticatedMapKey::new(bytes).map_err(|_| {
                    E::invalid_length(bytes.len(), &"1..=48 authenticated-map key bytes")
                })
            }
        }

        impl<'de> de::Visitor<'de> for KeyVisitor {
            type Value = AuthenticatedMapKey;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("1..=48 authenticated-map key bytes")
            }

            fn visit_bytes<E: de::Error>(self, value: &[u8]) -> Result<Self::Value, E> {
                Self::build(value)
            }

            fn visit_byte_buf<E: de::Error>(self, value: Vec<u8>) -> Result<Self::Value, E> {
                Self::build(&value)
            }

            fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut bytes = Vec::with_capacity(MAX_AUTHENTICATED_MAP_KEY_BYTES);
                while let Some(byte) = seq.next_element::<u8>()? {
                    if bytes.len() == MAX_AUTHENTICATED_MAP_KEY_BYTES {
                        return Err(de::Error::invalid_length(
                            bytes.len() + 1,
                            &"1..=48 authenticated-map key bytes",
                        ));
                    }
                    bytes.push(byte);
                }
                Self::build(&bytes)
            }
        }

        deserializer.deserialize_bytes(KeyVisitor)
    }
}

/// Append one key to a digest preimage as `length ‖ bytes`.
///
/// Every authenticated-map key is length-framed inside the shared node digest,
/// so the preimage stays injective for arbitrary caller key spaces — including
/// two keys where one is a prefix of the other — without imposing an
/// undocumented prefix-free obligation on library callers. The length always
/// fits one byte because [`MAX_AUTHENTICATED_MAP_KEY_BYTES`] is 48.
fn push_framed_key(bytes: &mut Vec<u8>, key: AuthenticatedMapKey) {
    bytes.push(key.length);
    bytes.extend_from_slice(key.as_slice());
}

const AUTHENTICATED_MAP_EMPTY_DOMAIN: &[u8] = b"tine/oplog/authenticated-map/v1/empty";
const AUTHENTICATED_MAP_PRIORITY_DOMAIN: &[u8] = b"tine/oplog/authenticated-map/v1/priority\0";
const AUTHENTICATED_MAP_NODE_DOMAIN: &[u8] = b"tine/oplog/authenticated-map/v2/node\0";
const ACCEPTED_STATUS_DOMAIN: &[u8] = b"tine/oplog/accepted-status/v2\0";

const CAUSAL_CLOCK_ENTRY_DOMAIN: &[u8] = b"tine/oplog/causal-clock-entry/v1\0";
const ACCEPTED_CAUSAL_RECORD_DOMAIN: &[u8] = b"tine/oplog/accepted-causal-record/v1\0";
const CAUSAL_PEER_TIP_DOMAIN: &[u8] = b"tine/oplog/causal-peer-tip/v2\0";

/// What kind of sealed object an address names.
///
/// The treap and sequence-tree kinds left with the structures that used them:
/// there are no map nodes and no sequence leaves/nodes any more, only records
/// and the sorted tables that point at them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SealedAcceptedObjectKind {
    StatusRecord,
    CausalRecord,
    Table,
}

impl fmt::Display for SealedAcceptedObjectKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::StatusRecord => "accepted-status record",
            Self::CausalRecord => "accepted-causal record",
            Self::Table => "sealed sorted table",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SealedAcceptedIndexError {
    Corrupt(String),
    Missing {
        kind: SealedAcceptedObjectKind,
        address: ContentDigest,
    },
    Store(String),
    Capacity,
    NonContiguousSequence {
        expected: u64,
        actual: u64,
    },
}

impl fmt::Display for SealedAcceptedIndexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Corrupt(message) => write!(formatter, "corrupt sealed accepted index: {message}"),
            Self::Missing { kind, address } => {
                write!(formatter, "missing sealed {kind} object {address}")
            }
            Self::Store(message) => write!(formatter, "sealed accepted index store: {message}"),
            Self::Capacity => formatter.write_str("sealed accepted index capacity exceeded"),
            Self::NonContiguousSequence { expected, actual } => write!(
                formatter,
                "non-contiguous accepted sequence: expected {expected}, found {actual}"
            ),
        }
    }
}

impl std::error::Error for SealedAcceptedIndexError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthenticatedMapLinkV1 {
    pub key: AuthenticatedMapKey,
    pub digest: ContentDigest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthenticatedMapRootV1 {
    pub count: u64,
    pub root: Option<AuthenticatedMapLinkV1>,
}

impl Default for AuthenticatedMapRootV1 {
    fn default() -> Self {
        Self::empty()
    }
}

impl AuthenticatedMapRootV1 {
    pub const fn empty() -> Self {
        Self {
            count: 0,
            root: None,
        }
    }

    pub fn root_digest(self) -> ContentDigest {
        self.root
            .map_or_else(authenticated_map_empty_digest, |root| root.digest)
    }
}

pub fn authenticated_map_empty_digest() -> ContentDigest {
    ContentDigest::of(AUTHENTICATED_MAP_EMPTY_DOMAIN)
}

/// Heap priority for one key.
///
/// The fold stays raw and keeps its v1 domain: its only variable-width input is
/// the last field, so a single key already determines the preimage uniquely and
/// no framing is needed. Keeping it byte-identical means the treap *shape* of
/// every existing 16-byte map is unchanged by this widening; only the node
/// digests are rebuilt.
pub fn authenticated_map_priority(key: AuthenticatedMapKey) -> ContentDigest {
    digest_fold(AUTHENTICATED_MAP_PRIORITY_DOMAIN, &[key.as_slice()])
}

/// Total order used for the treap heap, ties broken by the complete key.
pub fn authenticated_map_priority_order(
    left: AuthenticatedMapKey,
    right: AuthenticatedMapKey,
) -> Ordering {
    authenticated_map_priority(left)
        .as_bytes()
        .cmp(authenticated_map_priority(right).as_bytes())
        .then_with(|| left.cmp(&right))
}

/// The single shared authenticated-map node digest.
///
/// Every implementation — the sealed writer/reader, the SQLite frontier treap,
/// and the Cartesian root builder — must call exactly this function, which is
/// what makes their roots comparable bit for bit.
///
/// Own and child keys are **length-framed** (`length ‖ bytes`); see
/// [`push_framed_key`].
pub fn authenticated_map_node_digest(
    key: AuthenticatedMapKey,
    value_digest: ContentDigest,
    left: Option<(AuthenticatedMapKey, ContentDigest)>,
    right: Option<(AuthenticatedMapKey, ContentDigest)>,
) -> ContentDigest {
    let mut bytes = AUTHENTICATED_MAP_NODE_DOMAIN.to_vec();
    push_framed_key(&mut bytes, key);
    bytes.extend_from_slice(value_digest.as_bytes());
    for child in [left, right] {
        match child {
            Some((child_key, digest)) => {
                bytes.push(1);
                push_framed_key(&mut bytes, child_key);
                bytes.extend_from_slice(digest.as_bytes());
            }
            None => bytes.push(0),
        }
    }
    ContentDigest::of(&bytes)
}

/// Derive the canonical treap root from strictly key-sorted entries.
pub fn authenticated_map_root(
    entries: &[(AuthenticatedMapKey, ContentDigest)],
) -> Result<AuthenticatedMapRootV1, SealedAcceptedIndexError> {
    if entries.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return Err(corrupt("authenticated-map entries are not strictly sorted"));
    }
    Ok(AuthenticatedMapRootV1 {
        count: entries
            .len()
            .try_into()
            .map_err(|_| SealedAcceptedIndexError::Capacity)?,
        root: authenticated_map_subtree(entries),
    })
}

fn authenticated_map_subtree(
    entries: &[(AuthenticatedMapKey, ContentDigest)],
) -> Option<AuthenticatedMapLinkV1> {
    let (root_index, (key, value_digest)) =
        entries
            .iter()
            .enumerate()
            .min_by(|(_, (left, _)), (_, (right, _))| {
                authenticated_map_priority_order(*left, *right)
            })?;
    let left = authenticated_map_subtree(&entries[..root_index]);
    let right = authenticated_map_subtree(&entries[root_index + 1..]);
    Some(AuthenticatedMapLinkV1 {
        key: *key,
        digest: authenticated_map_node_digest(
            *key,
            *value_digest,
            left.map(|child| (child.key, child.digest)),
            right.map(|child| (child.key, child.digest)),
        ),
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptedStatusRecordV2 {
    pub batch_id: [u8; 16],
    pub no_op: bool,
    pub evidence_schema: u32,
    pub exact_evidence_bytes: Vec<u8>,
    pub accepted_causal_record_digest: ContentDigest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct AcceptedStatusWireV2 {
    schema: u32,
    batch_id: [u8; 16],
    no_op: bool,
    evidence_schema: u32,
    exact_evidence_bytes: Vec<u8>,
    accepted_causal_record_digest: [u8; 32],
}

impl AcceptedStatusRecordV2 {
    pub fn value_digest(&self) -> ContentDigest {
        let no_op = [u8::from(self.no_op)];
        let evidence_schema = self.evidence_schema.to_be_bytes();
        let evidence_len = (self.exact_evidence_bytes.len() as u64).to_be_bytes();
        digest_fold(
            ACCEPTED_STATUS_DOMAIN,
            &[
                &self.batch_id,
                &no_op,
                &evidence_schema,
                &evidence_len,
                &self.exact_evidence_bytes,
                self.accepted_causal_record_digest.as_bytes(),
            ],
        )
    }

    pub fn encode(&self) -> Result<Vec<u8>, SealedAcceptedIndexError> {
        if self.evidence_schema == 0 || self.exact_evidence_bytes.is_empty() {
            return Err(corrupt("accepted-status evidence is empty or unversioned"));
        }
        canonical_encode(&AcceptedStatusWireV2 {
            schema: SEALED_ACCEPTED_STATUS_SCHEMA_VERSION,
            batch_id: self.batch_id,
            no_op: self.no_op,
            evidence_schema: self.evidence_schema,
            exact_evidence_bytes: self.exact_evidence_bytes.clone(),
            accepted_causal_record_digest: *self.accepted_causal_record_digest.as_bytes(),
        })
    }

    pub fn decode(
        expected_batch: [u8; 16],
        expected_address: ContentDigest,
        bytes: &[u8],
    ) -> Result<Self, SealedAcceptedIndexError> {
        let wire: AcceptedStatusWireV2 = canonical_decode(bytes, "accepted-status record")?;
        let record = Self {
            batch_id: wire.batch_id,
            no_op: wire.no_op,
            evidence_schema: wire.evidence_schema,
            exact_evidence_bytes: wire.exact_evidence_bytes,
            accepted_causal_record_digest: ContentDigest::from_bytes(
                wire.accepted_causal_record_digest,
            ),
        };
        if wire.schema != SEALED_ACCEPTED_STATUS_SCHEMA_VERSION
            || record.batch_id != expected_batch
            || record.evidence_schema == 0
            || record.exact_evidence_bytes.is_empty()
            || record.value_digest() != expected_address
        {
            return Err(corrupt("accepted-status record binding mismatch"));
        }
        Ok(record)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SealedAcceptedCausalClockEntryV2 {
    pub peer_id: [u8; 16],
    pub counter: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedAcceptedCausalRecordV2 {
    pub batch_id: [u8; 16],
    pub manifest_fingerprint: ContentDigest,
    pub event_binding_digest: ContentDigest,
    pub causal_peer_id: [u8; 16],
    pub causal_counter: u64,
    pub canonical_causal_clock: Vec<SealedAcceptedCausalClockEntryV2>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct CausalClockEntryWireV2 {
    peer_id: [u8; 16],
    counter: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct AcceptedCausalRecordWireV2 {
    schema: u32,
    batch_id: [u8; 16],
    manifest_fingerprint: [u8; 32],
    event_binding_digest: [u8; 32],
    causal_peer_id: [u8; 16],
    causal_counter: u64,
    canonical_causal_clock: Vec<CausalClockEntryWireV2>,
}

impl SealedAcceptedCausalRecordV2 {
    pub fn clock_root(&self) -> Result<AuthenticatedMapRootV1, SealedAcceptedIndexError> {
        validate_causal_clock(self)?;
        let entries = self
            .canonical_causal_clock
            .iter()
            .map(|entry| {
                (
                    AuthenticatedMapKey::from(entry.peer_id),
                    causal_clock_counter_digest(entry.peer_id, entry.counter),
                )
            })
            .collect::<Vec<_>>();
        authenticated_map_root(&entries)
    }

    pub fn address(&self) -> Result<ContentDigest, SealedAcceptedIndexError> {
        let root = self.clock_root()?;
        Ok(accepted_causal_record_digest(
            self.batch_id,
            self.manifest_fingerprint,
            self.event_binding_digest,
            self.causal_peer_id,
            self.causal_counter,
            root.root,
        ))
    }

    pub fn encode(&self) -> Result<Vec<u8>, SealedAcceptedIndexError> {
        validate_causal_clock(self)?;
        canonical_encode(&AcceptedCausalRecordWireV2 {
            schema: SEALED_ACCEPTED_CAUSAL_RECORD_SCHEMA_VERSION,
            batch_id: self.batch_id,
            manifest_fingerprint: *self.manifest_fingerprint.as_bytes(),
            event_binding_digest: *self.event_binding_digest.as_bytes(),
            causal_peer_id: self.causal_peer_id,
            causal_counter: self.causal_counter,
            canonical_causal_clock: self
                .canonical_causal_clock
                .iter()
                .map(|entry| CausalClockEntryWireV2 {
                    peer_id: entry.peer_id,
                    counter: entry.counter,
                })
                .collect(),
        })
    }

    pub fn decode(
        expected_batch: [u8; 16],
        expected_address: ContentDigest,
        bytes: &[u8],
    ) -> Result<Self, SealedAcceptedIndexError> {
        let wire: AcceptedCausalRecordWireV2 = canonical_decode(bytes, "accepted-causal record")?;
        let record = Self {
            batch_id: wire.batch_id,
            manifest_fingerprint: ContentDigest::from_bytes(wire.manifest_fingerprint),
            event_binding_digest: ContentDigest::from_bytes(wire.event_binding_digest),
            causal_peer_id: wire.causal_peer_id,
            causal_counter: wire.causal_counter,
            canonical_causal_clock: wire
                .canonical_causal_clock
                .into_iter()
                .map(|entry| SealedAcceptedCausalClockEntryV2 {
                    peer_id: entry.peer_id,
                    counter: entry.counter,
                })
                .collect(),
        };
        if wire.schema != SEALED_ACCEPTED_CAUSAL_RECORD_SCHEMA_VERSION
            || record.batch_id != expected_batch
            || record.address()? != expected_address
        {
            return Err(corrupt("accepted-causal record binding mismatch"));
        }
        Ok(record)
    }
}

pub fn causal_clock_counter_digest(peer_id: [u8; 16], counter: u64) -> ContentDigest {
    digest_fold(
        CAUSAL_CLOCK_ENTRY_DOMAIN,
        &[&peer_id, &counter.to_be_bytes()],
    )
}

pub fn accepted_causal_record_digest(
    batch_id: [u8; 16],
    manifest_fingerprint: ContentDigest,
    event_binding_digest: ContentDigest,
    causal_peer_id: [u8; 16],
    causal_counter: u64,
    clock_root: Option<AuthenticatedMapLinkV1>,
) -> ContentDigest {
    let mut bytes = ACCEPTED_CAUSAL_RECORD_DOMAIN.to_vec();
    bytes.extend_from_slice(&batch_id);
    bytes.extend_from_slice(manifest_fingerprint.as_bytes());
    bytes.extend_from_slice(event_binding_digest.as_bytes());
    bytes.extend_from_slice(&causal_peer_id);
    bytes.extend_from_slice(&causal_counter.to_be_bytes());
    match clock_root {
        Some(root) => {
            bytes.push(1);
            // The clock root key became variable-width with the map key, and a
            // fixed-width digest follows it, so it is length-framed here for
            // the same injectivity reason as the node digest.
            push_framed_key(&mut bytes, root.key);
            bytes.extend_from_slice(root.digest.as_bytes());
        }
        None => {
            bytes.push(0);
            bytes.extend_from_slice(authenticated_map_empty_digest().as_bytes());
        }
    }
    ContentDigest::of(&bytes)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CausalTipRecordV2 {
    pub peer_id: [u8; 16],
    pub highest_accepted_counter: u64,
    pub batch_id: [u8; 16],
}

impl CausalTipRecordV2 {
    pub fn value_digest(self) -> Result<ContentDigest, SealedAcceptedIndexError> {
        if self.highest_accepted_counter == 0 {
            return Err(corrupt("causal-tip counter is zero"));
        }
        Ok(digest_fold(
            CAUSAL_PEER_TIP_DOMAIN,
            &[
                &self.peer_id,
                &self.highest_accepted_counter.to_be_bytes(),
                &self.batch_id,
            ],
        ))
    }
}

/// Domain fields recovered from one exact canonical accepted-evidence value.
///
/// `tine-storage` deliberately does not own Tine's accepted-evidence codec. The
/// caller-supplied decoder below validates the current bytes without causing this
/// physical crate to depend on the engine crate; the sealed reader then binds
/// the decoded identity to all three authenticated index edges.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AcceptedEvidenceBindingV2 {
    pub batch_id: [u8; 16],
    pub manifest_fingerprint: ContentDigest,
    pub event_binding_digest: ContentDigest,
    pub acceptance_sequence: u64,
}

pub trait SealedAcceptedEvidenceDecoder {
    fn decode_accepted_evidence(
        &self,
        evidence_schema: u32,
        exact_evidence_bytes: &[u8],
    ) -> Result<AcceptedEvidenceBindingV2, SealedAcceptedIndexError>;
}

/// The two records one sealed accepted batch resolves to.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedBatchRecords {
    pub causal: SealedAcceptedCausalRecordV2,
    pub status: AcceptedStatusRecordV2,
}

impl SealedBatchRecords {
    /// The cross-checks the retired `prove_membership` performed, minus the
    /// Merkle path.
    ///
    /// The path is gone on purpose (D-2, spec 4.3): Tine does not
    /// re-authenticate its own previously established private state, and a
    /// per-lookup proof walk is exactly that. What the proof ALSO did, and what
    /// is worth keeping, is binding the two records and the caller's evidence
    /// to one identity, which costs no reads at all. Callers that hold an
    /// evidence decoder should call this; it is not free-standing validation
    /// the reader can do for them, because `tine-storage` deliberately does not
    /// own Tine's accepted-evidence codec.
    pub fn verify<Decoder: SealedAcceptedEvidenceDecoder>(
        &self,
        sequence: u64,
        batch_id: [u8; 16],
        evidence_decoder: &Decoder,
    ) -> Result<(), SealedAcceptedIndexError> {
        if self.causal.batch_id != batch_id || self.status.batch_id != batch_id {
            return Err(corrupt("sealed batch records name another batch"));
        }
        if self.status.accepted_causal_record_digest != self.causal.address()? {
            return Err(corrupt("status/causal record cross-check failed"));
        }
        let evidence = evidence_decoder.decode_accepted_evidence(
            self.status.evidence_schema,
            &self.status.exact_evidence_bytes,
        )?;
        if evidence.batch_id != batch_id
            || evidence.acceptance_sequence != sequence
            || evidence.manifest_fingerprint != self.causal.manifest_fingerprint
            || evidence.event_binding_digest != self.causal.event_binding_digest
        {
            return Err(corrupt(
                "accepted evidence/status/sequence/causal binding mismatch",
            ));
        }
        Ok(())
    }
}

/// The sealed accepted index, as the two POINT LOOKUPS its consumers need.
///
/// It used to be a NODE seam: SQLite descended one authenticated treap whose
/// covered subtrees lived in sealed files, falling through per node. With
/// immutable sorted tables there is no node to fall through to, and there is no
/// shared tree to keep in step either -- the sealed side answers a question
/// instead of exposing a structure.
///
/// Implementations expose only already-sealed immutable state. A missing
/// covered batch is `Ok(None)` -- genuinely absent -- not a cache miss an
/// implementation may reinterpret.
pub trait SealedAcceptedIndexRead {
    /// The causal and status records for `batch_id`, or `None` when the sealed
    /// history does not contain it.
    fn batch(
        &self,
        batch_id: [u8; 16],
    ) -> Result<Option<SealedBatchRecords>, SealedAcceptedIndexError>;

    /// The batch accepted at `sequence`, or `None` when the sealed history does
    /// not cover it.
    fn sequence(&self, sequence: u64) -> Result<Option<[u8; 16]>, SealedAcceptedIndexError>;
}

fn validate_causal_clock(
    record: &SealedAcceptedCausalRecordV2,
) -> Result<(), SealedAcceptedIndexError> {
    if record.causal_counter == 0
        || record.canonical_causal_clock.is_empty()
        || record
            .canonical_causal_clock
            .windows(2)
            .any(|pair| pair[0].peer_id >= pair[1].peer_id)
        || record
            .canonical_causal_clock
            .iter()
            .any(|entry| entry.counter == 0)
        || !record.canonical_causal_clock.iter().any(|entry| {
            entry.peer_id == record.causal_peer_id && entry.counter == record.causal_counter
        })
    {
        return Err(corrupt("accepted-causal record clock is not canonical"));
    }
    Ok(())
}

fn digest_fold(domain: &[u8], fields: &[&[u8]]) -> ContentDigest {
    let length = fields.iter().map(|field| field.len()).sum::<usize>();
    let mut bytes = Vec::with_capacity(domain.len() + length);
    bytes.extend_from_slice(domain);
    for field in fields {
        bytes.extend_from_slice(field);
    }
    ContentDigest::of(&bytes)
}

fn canonical_encode<T: Serialize>(value: &T) -> Result<Vec<u8>, SealedAcceptedIndexError> {
    postcard::to_allocvec(value).map_err(|error| SealedAcceptedIndexError::Store(error.to_string()))
}

fn canonical_decode<T: DeserializeOwned + Serialize>(
    bytes: &[u8],
    what: &str,
) -> Result<T, SealedAcceptedIndexError> {
    let (value, trailing): (T, &[u8]) = postcard::take_from_bytes(bytes)
        .map_err(|error| corrupt(format!("invalid {what}: {error}")))?;
    if !trailing.is_empty() || canonical_encode(&value)? != bytes {
        return Err(corrupt(format!("non-canonical {what}")));
    }
    Ok(value)
}

fn corrupt(message: impl Into<String>) -> SealedAcceptedIndexError {
    SealedAcceptedIndexError::Corrupt(message.into())
}
#[cfg(test)]
mod tests {
    use super::*;

    fn digest(byte: u8) -> ContentDigest {
        ContentDigest::from_bytes([byte; 32])
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    struct TestEvidenceWire {
        schema: u32,
        batch_id: [u8; 16],
        manifest_fingerprint: [u8; 32],
        event_binding_digest: [u8; 32],
        acceptance_sequence: u64,
    }

    struct TestEvidenceDecoder;

    impl SealedAcceptedEvidenceDecoder for TestEvidenceDecoder {
        fn decode_accepted_evidence(
            &self,
            evidence_schema: u32,
            exact_evidence_bytes: &[u8],
        ) -> Result<AcceptedEvidenceBindingV2, SealedAcceptedIndexError> {
            let wire: TestEvidenceWire = canonical_decode(exact_evidence_bytes, "test evidence")?;
            if wire.schema != evidence_schema || !matches!(wire.schema, 1 | 2) {
                return Err(corrupt("unknown test evidence schema"));
            }
            Ok(AcceptedEvidenceBindingV2 {
                batch_id: wire.batch_id,
                manifest_fingerprint: ContentDigest::from_bytes(wire.manifest_fingerprint),
                event_binding_digest: ContentDigest::from_bytes(wire.event_binding_digest),
                acceptance_sequence: wire.acceptance_sequence,
            })
        }
    }

    fn status(batch: [u8; 16], causal: ContentDigest) -> AcceptedStatusRecordV2 {
        AcceptedStatusRecordV2 {
            batch_id: batch,
            no_op: false,
            evidence_schema: 1,
            exact_evidence_bytes: canonical_encode(&TestEvidenceWire {
                schema: 1,
                batch_id: batch,
                manifest_fingerprint: [0x22; 32],
                event_binding_digest: [0x33; 32],
                acceptance_sequence: 1,
            })
            .unwrap(),
            accepted_causal_record_digest: causal,
        }
    }

    fn causal(batch: [u8; 16]) -> SealedAcceptedCausalRecordV2 {
        SealedAcceptedCausalRecordV2 {
            batch_id: batch,
            manifest_fingerprint: digest(0x22),
            event_binding_digest: digest(0x33),
            causal_peer_id: [0x44; 16],
            causal_counter: 7,
            canonical_causal_clock: vec![
                SealedAcceptedCausalClockEntryV2 {
                    peer_id: [0x11; 16],
                    counter: 3,
                },
                SealedAcceptedCausalClockEntryV2 {
                    peer_id: [0x44; 16],
                    counter: 7,
                },
            ],
        }
    }

    /// The cross-check `SealedBatchRecords::verify` exists for: the causal
    /// record, the status record, the decoded evidence, the batch id and the
    /// acceptance sequence must all name the same accepted batch.
    ///
    /// This is the whole of what survived the membership PROOF the sealed treap
    /// used to carry (D-2: private state is not re-authenticated per lookup).
    /// It is a binding check over records the caller already holds, not a
    /// Merkle path, and it is cheap enough to run on every covered read.
    #[test]
    fn sealed_batch_records_cross_check_their_evidence_and_sequence() {
        let batch_id = [0x51; 16];
        let causal = causal(batch_id);
        let records = SealedBatchRecords {
            status: status(batch_id, causal.address().unwrap()),
            causal: causal.clone(),
        };
        records.verify(1, batch_id, &TestEvidenceDecoder).unwrap();

        // Another id, the wrong sequence, a status pointing at a different
        // causal record, and evidence for a different batch all refuse.
        assert!(records.verify(1, [0x52; 16], &TestEvidenceDecoder).is_err());
        assert!(records.verify(2, batch_id, &TestEvidenceDecoder).is_err());

        let mut misbound = records.clone();
        misbound.status.accepted_causal_record_digest = digest(0x99);
        assert!(misbound.verify(1, batch_id, &TestEvidenceDecoder).is_err());

        let mut foreign_evidence = records.clone();
        foreign_evidence.status = status([0x52; 16], causal.address().unwrap());
        foreign_evidence.status.batch_id = batch_id;
        assert!(foreign_evidence
            .verify(1, batch_id, &TestEvidenceDecoder)
            .is_err());

        let mut unknown_schema = records;
        unknown_schema.status.evidence_schema = 7;
        assert!(unknown_schema
            .verify(1, batch_id, &TestEvidenceDecoder)
            .is_err());
    }

    #[test]
    fn canonical_decoders_reject_trailing_and_misbound_bytes() {
        let record = status([0x61; 16], digest(0x72));
        let address = record.value_digest();
        let mut bytes = record.encode().unwrap();
        bytes.push(0);
        assert!(AcceptedStatusRecordV2::decode(record.batch_id, address, &bytes).is_err());

        let record = causal([0x62; 16]);
        let bytes = record.encode().unwrap();
        assert!(SealedAcceptedCausalRecordV2::decode(
            [0x63; 16],
            record.address().unwrap(),
            &bytes,
        )
        .is_err());

        let mut later_dot = causal([0x64; 16]);
        later_dot.canonical_causal_clock[1].counter += 1;
        assert!(later_dot.encode().is_err());
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn key16(fill: u8) -> AuthenticatedMapKey {
        AuthenticatedMapKey::from([fill; 16])
    }

    fn tagged_entity_key(uuid: u8) -> AuthenticatedMapKey {
        let mut bytes = vec![0x01];
        bytes.extend_from_slice(&[uuid; 16]);
        AuthenticatedMapKey::new(&bytes).unwrap()
    }

    fn tagged_membership_key(block: u8, page: u8) -> AuthenticatedMapKey {
        let mut bytes = vec![0x02];
        bytes.extend_from_slice(&[block; 16]);
        bytes.extend_from_slice(&[page; 16]);
        AuthenticatedMapKey::new(&bytes).unwrap()
    }

    /// The domain owner's mixed-width key space: 17-byte entity keys and
    /// 33-byte membership keys interleaved, plus a bare 16-byte identity.
    fn mixed_width_entries() -> Vec<(AuthenticatedMapKey, ContentDigest)> {
        let mut entries = vec![
            (tagged_entity_key(0x11), digest(0x01)),
            (tagged_entity_key(0x22), digest(0x02)),
            (tagged_entity_key(0x33), digest(0x03)),
            (tagged_membership_key(0x11, 0x44), digest(0x04)),
            (tagged_membership_key(0x11, 0x55), digest(0x05)),
            (tagged_membership_key(0x22, 0x44), digest(0x06)),
            (key16(0x00), digest(0x07)),
            (AuthenticatedMapKey::new(&[0x03]).unwrap(), digest(0x08)),
            (
                AuthenticatedMapKey::new(&[0xff; MAX_AUTHENTICATED_MAP_KEY_BYTES]).unwrap(),
                digest(0x09),
            ),
        ];
        entries.sort_unstable_by_key(|(key, _)| *key);
        entries
    }
    #[test]
    fn authenticated_map_key_validates_length_and_hides_its_padding() {
        assert!(AuthenticatedMapKey::new(&[]).is_err());
        assert!(AuthenticatedMapKey::new(&[0; MAX_AUTHENTICATED_MAP_KEY_BYTES + 1]).is_err());
        assert_eq!(AuthenticatedMapKey::new(&[7]).unwrap().len(), 1);
        assert_eq!(
            AuthenticatedMapKey::new(&[7; MAX_AUTHENTICATED_MAP_KEY_BYTES])
                .unwrap()
                .len(),
            MAX_AUTHENTICATED_MAP_KEY_BYTES
        );

        // A short key and a longer key whose padding would make them equal are
        // distinct in every identity channel the map uses.
        let short = AuthenticatedMapKey::new(&[1, 2]).unwrap();
        let padded_looking = AuthenticatedMapKey::new(&[1, 2, 0]).unwrap();
        assert_ne!(short, padded_looking);
        assert!(short < padded_looking);
        assert_ne!(
            authenticated_map_priority(short),
            authenticated_map_priority(padded_looking)
        );

        let hash = |key: AuthenticatedMapKey| {
            use std::hash::{BuildHasher as _, RandomState};
            RandomState::new().hash_one(key)
        };
        assert_ne!(hash(short), hash(padded_looking));
        assert_eq!(
            AuthenticatedMapKey::from([0x5a; 16]),
            AuthenticatedMapKey::new(&[0x5a; 16]).unwrap(),
            "a 16-byte identity is exactly the 16-byte key"
        );
    }

    #[test]
    fn authenticated_map_key_orders_lexicographically_not_by_length() {
        // The discriminating case: a derived `Ord` over `(length, bytes)` would
        // put the one-byte key first. Lexicographic order does not, and the
        // treap's search invariant and every sorted-unique precondition depend
        // on the lexicographic answer.
        let one = AuthenticatedMapKey::new(&[0x02]).unwrap();
        let two = AuthenticatedMapKey::new(&[0x01, 0xff]).unwrap();
        assert!(two < one);
        assert_eq!(two.cmp(&one), [0x01_u8, 0xff][..].cmp(&[0x02][..]));

        // A strict prefix sorts before its extension, matching SQLite BLOB
        // order and the domain owner's own byte order.
        let prefix = AuthenticatedMapKey::new(&[0x01, 0x02]).unwrap();
        let extension = AuthenticatedMapKey::new(&[0x01, 0x02, 0x00]).unwrap();
        assert!(prefix < extension);

        // Class tags keep entity and membership keys in disjoint contiguous
        // ranges, and the shared order agrees with the raw bytes throughout.
        let mut keys: Vec<AuthenticatedMapKey> =
            mixed_width_entries().into_iter().map(|(k, _)| k).collect();
        keys.sort_unstable();
        let mut raw: Vec<Vec<u8>> = keys.iter().map(|key| key.as_slice().to_vec()).collect();
        raw.sort();
        assert_eq!(
            keys.iter()
                .map(|key| key.as_slice().to_vec())
                .collect::<Vec<_>>(),
            raw
        );
    }

    #[test]
    fn authenticated_map_key_serde_is_canonical_and_rejects_malformed_lengths() {
        for key in [
            AuthenticatedMapKey::new(&[0x09]).unwrap(),
            key16(0x42),
            tagged_membership_key(0x11, 0x22),
            AuthenticatedMapKey::new(&[0xab; MAX_AUTHENTICATED_MAP_KEY_BYTES]).unwrap(),
        ] {
            let bytes = canonical_encode(&key).unwrap();
            // One canonical representation: a length prefix and the meaningful
            // bytes only, never the fixed-width padding.
            assert_eq!(bytes.len(), 1 + key.len());
            assert_eq!(bytes[0] as usize, key.len());
            assert_eq!(&bytes[1..], key.as_slice());
            assert_eq!(
                canonical_decode::<AuthenticatedMapKey>(&bytes, "key").unwrap(),
                key
            );
        }

        // Zero-length and over-long encodings are refused at the codec, so no
        // padded or empty second representation can be read back.
        assert!(canonical_decode::<AuthenticatedMapKey>(&[0x00], "key").is_err());
        let mut oversized = vec![(MAX_AUTHENTICATED_MAP_KEY_BYTES + 1) as u8];
        oversized.extend_from_slice(&[0u8; MAX_AUTHENTICATED_MAP_KEY_BYTES + 1]);
        assert!(canonical_decode::<AuthenticatedMapKey>(&oversized, "key").is_err());

        // The stored map NODE this arm used to decode went with the sealed
        // treap; the key-level refusals above are what survived it, and they
        // are the ones the retained `AuthenticatedMapKey` codec owns.
    }

    /// The reason the shared node digest length-frames its keys.
    ///
    /// With a raw fold, a one-byte-longer own key can absorb the first byte of
    /// the value digest, shifting every later field by one and letting a
    /// *different* (key, value, child) triple produce the identical preimage.
    /// The pair below is exactly such a shift; framing separates them.
    #[test]
    fn authenticated_map_node_digest_length_frames_keys_against_shift_collisions() {
        let mut value_a = [0x5a_u8; 32];
        value_a[31] = 1; // becomes the child-present tag byte in the shifted twin
        let child_digest = digest(0x6b);

        let long_key = AuthenticatedMapKey::new(&[0xaa, 0xbb]).unwrap();
        let short_key = AuthenticatedMapKey::new(&[0xaa]).unwrap();

        let mut value_b = [0u8; 32];
        value_b[0] = 0xbb;
        value_b[1..].copy_from_slice(&value_a[..31]);

        let child_a = AuthenticatedMapKey::new(&[0x77]).unwrap();
        let child_b = AuthenticatedMapKey::new(&[0x01, 0x77]).unwrap();

        // Raw preimages: identical byte strings, byte for byte.
        let raw = |key: AuthenticatedMapKey, value: [u8; 32], child: AuthenticatedMapKey| {
            let mut bytes = AUTHENTICATED_MAP_NODE_DOMAIN.to_vec();
            bytes.extend_from_slice(key.as_slice());
            bytes.extend_from_slice(&value);
            bytes.push(1);
            bytes.extend_from_slice(child.as_slice());
            bytes.extend_from_slice(child_digest.as_bytes());
            bytes.push(0);
            bytes
        };
        assert_eq!(
            raw(long_key, value_a, child_a),
            raw(short_key, value_b, child_b),
            "the two nodes are a genuine raw-fold collision"
        );

        // The shared, length-framed digest separates them.
        let framed = |key: AuthenticatedMapKey, value: [u8; 32], child: AuthenticatedMapKey| {
            authenticated_map_node_digest(
                key,
                ContentDigest::from_bytes(value),
                Some((child, child_digest)),
                None,
            )
        };
        assert_ne!(
            framed(long_key, value_a, child_a),
            framed(short_key, value_b, child_b)
        );

        // Prefix-related keys in the same slot also stay distinct.
        assert_ne!(
            authenticated_map_node_digest(
                AuthenticatedMapKey::new(&[0x01, 0x02]).unwrap(),
                digest(0x10),
                None,
                None,
            ),
            authenticated_map_node_digest(
                AuthenticatedMapKey::new(&[0x01, 0x02, 0x00]).unwrap(),
                digest(0x10),
                None,
                None,
            )
        );
    }

    #[test]
    fn v1_and_v2_golden_vectors_are_frozen() {
        let key = key16(0x11);
        let priority = authenticated_map_priority(key).to_string();
        let empty = authenticated_map_empty_digest().to_string();
        let node = authenticated_map_node_digest(key, digest(0x22), None, None).to_string();
        let root = authenticated_map_root(&[
            (key16(0x10), digest(0xa0)),
            (key16(0x20), digest(0xb0)),
            (key16(0x30), digest(0xc0)),
        ])
        .unwrap();

        let causal = causal([0x51; 16]);
        let causal_bytes = hex(&causal.encode().unwrap());
        let causal_address = causal.address().unwrap().to_string();
        let clock_root = causal.clock_root().unwrap();
        let status = status([0x51; 16], causal.address().unwrap());
        let status_bytes = hex(&status.encode().unwrap());
        let status_digest = status.value_digest().to_string();
        assert_eq!(
            priority,
            "b04c72e061f87a6d015f69242d917fc0cddc0699b320805e33e92dabe097e7ad"
        );
        assert_eq!(
            empty,
            "610e8e19cb4d5cf03632e84b4278eac97c00f8b76fcf093fcca732fc5759b622"
        );
        assert_eq!(
            node,
            "7e95dc91b6c2842c6fb1948c49901f4331624e2e420130590e6bce6028e2d64c"
        );
        assert_eq!(
            root.root_digest().to_string(),
            "8f97b0824b8b3674284e302539c276f91ab41e26b4f0b46eac668832c51d31dd"
        );
        assert_eq!(causal_bytes, "02515151515151515151515151515151512222222222222222222222222222222222222222222222222222222222222222333333333333333333333333333333333333333333333333333333333333333344444444444444444444444444444444070211111111111111111111111111111111034444444444444444444444444444444407");
        assert_eq!(
            causal_address,
            "8256b076a35d84e81ff47ae5b044bca6753ae167ddc2dd0413b630bcb49af197"
        );
        assert_eq!(
            clock_root.root_digest().to_string(),
            "0190f2ad40bf5145da6838c56d638d6620c975cfd7271cd4de8b318833af11ea"
        );
        assert_eq!(status_bytes, "0251515151515151515151515151515151000152015151515151515151515151515151515122222222222222222222222222222222222222222222222222222222222222223333333333333333333333333333333333333333333333333333333333333333018256b076a35d84e81ff47ae5b044bca6753ae167ddc2dd0413b630bcb49af197");
        assert_eq!(
            status_digest,
            "5b90c90985efd08eab2a6d661130d320ff6013d996dff26e7f9af03e2916fa66"
        );
        // The sequence-tree leaf and node vectors went with the tree. The
        // sealed sequence index is a sorted table now, and its frozen bytes are
        // `sealed_tables_impl`'s golden vector.
    }
}
