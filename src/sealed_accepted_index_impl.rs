use std::{cmp::Ordering, fmt};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::ContentDigest;

pub const SEALED_ACCEPTED_INDEX_SCHEMA_VERSION: u32 = 2;
pub const SEALED_ACCEPTED_MAP_NODE_SCHEMA_VERSION: u32 = 2;
pub const SEALED_ACCEPTED_STATUS_SCHEMA_VERSION: u32 = 2;
pub const SEALED_ACCEPTED_SEQUENCE_SCHEMA_VERSION: u32 = 2;
pub const SEALED_ACCEPTED_CAUSAL_RECORD_SCHEMA_VERSION: u32 = 2;
pub const SEALED_ACCEPTED_SEQUENCE_FANOUT: usize = 32;
pub const SEALED_ACCEPTED_SEQUENCE_LEAF_CAPACITY: usize = 1;

/// Hard refusal ceiling for a corrupted or adversarially-shaped sealed index.
///
/// This is a reader work bound, not part of the bytes on disk. Healthy
/// deterministic treaps are expected to remain far below it.
pub const MAX_ACCEPTED_INDEX_DEPTH: usize = 256;

const AUTHENTICATED_MAP_EMPTY_DOMAIN: &[u8] = b"tine/oplog/authenticated-map/v1/empty";
const AUTHENTICATED_MAP_PRIORITY_DOMAIN: &[u8] = b"tine/oplog/authenticated-map/v1/priority\0";
const AUTHENTICATED_MAP_NODE_DOMAIN: &[u8] = b"tine/oplog/authenticated-map/v1/node\0";
const ACCEPTED_STATUS_DOMAIN: &[u8] = b"tine/oplog/accepted-status/v2\0";
const ACCEPTED_SEQUENCE_ENTRY_DOMAIN: &[u8] = b"tine/oplog/accepted-sequence/v2/entry\0";
const ACCEPTED_SEQUENCE_LEAF_DOMAIN: &[u8] = b"tine/oplog/accepted-sequence/v2/leaf\0";
const ACCEPTED_SEQUENCE_NODE_DOMAIN: &[u8] = b"tine/oplog/accepted-sequence/v2/node\0";
const CAUSAL_CLOCK_ENTRY_DOMAIN: &[u8] = b"tine/oplog/causal-clock-entry/v1\0";
const ACCEPTED_CAUSAL_RECORD_DOMAIN: &[u8] = b"tine/oplog/accepted-causal-record/v1\0";
const CAUSAL_PEER_TIP_DOMAIN: &[u8] = b"tine/oplog/causal-peer-tip/v2\0";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SealedAcceptedObjectKind {
    MapNode,
    StatusRecord,
    SequenceLeaf,
    SequenceNode,
    CausalRecord,
}

impl fmt::Display for SealedAcceptedObjectKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::MapNode => "authenticated-map node",
            Self::StatusRecord => "accepted-status record",
            Self::SequenceLeaf => "accepted-sequence leaf",
            Self::SequenceNode => "accepted-sequence node",
            Self::CausalRecord => "accepted-causal record",
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

/// Content-addressed object access used by both the clean engine and SQLite.
///
/// Implementations must verify exact-existing publication according to their
/// physical store contract. The sealed-index layer independently validates
/// every canonical payload and logical address on read.
pub trait SealedAcceptedIndexObjectStore {
    fn read_sealed_accepted_object(
        &self,
        kind: SealedAcceptedObjectKind,
        address: ContentDigest,
    ) -> Result<Option<Vec<u8>>, SealedAcceptedIndexError>;

    fn publish_sealed_accepted_object(
        &mut self,
        kind: SealedAcceptedObjectKind,
        address: ContentDigest,
        bytes: &[u8],
    ) -> Result<(), SealedAcceptedIndexError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct MapLinkWire {
    key: [u8; 16],
    digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthenticatedMapLinkV1 {
    pub key: [u8; 16],
    pub digest: ContentDigest,
}

impl From<AuthenticatedMapLinkV1> for MapLinkWire {
    fn from(value: AuthenticatedMapLinkV1) -> Self {
        Self {
            key: value.key,
            digest: *value.digest.as_bytes(),
        }
    }
}

impl From<MapLinkWire> for AuthenticatedMapLinkV1 {
    fn from(value: MapLinkWire) -> Self {
        Self {
            key: value.key,
            digest: ContentDigest::from_bytes(value.digest),
        }
    }
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedAuthenticatedMapNodeV2 {
    pub key: [u8; 16],
    pub value_digest: ContentDigest,
    pub left: Option<AuthenticatedMapLinkV1>,
    pub right: Option<AuthenticatedMapLinkV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct MapNodeWireV2 {
    schema: u32,
    key: [u8; 16],
    value_digest: [u8; 32],
    left: Option<MapLinkWire>,
    right: Option<MapLinkWire>,
}

impl SealedAuthenticatedMapNodeV2 {
    pub fn logical_digest(&self) -> ContentDigest {
        authenticated_map_node_digest(
            self.key,
            self.value_digest,
            self.left.map(|child| (child.key, child.digest)),
            self.right.map(|child| (child.key, child.digest)),
        )
    }

    pub fn encode(&self) -> Result<Vec<u8>, SealedAcceptedIndexError> {
        canonical_encode(&MapNodeWireV2 {
            schema: SEALED_ACCEPTED_MAP_NODE_SCHEMA_VERSION,
            key: self.key,
            value_digest: *self.value_digest.as_bytes(),
            left: self.left.map(Into::into),
            right: self.right.map(Into::into),
        })
    }

    pub fn decode(
        expected: AuthenticatedMapLinkV1,
        bytes: &[u8],
    ) -> Result<Self, SealedAcceptedIndexError> {
        let wire: MapNodeWireV2 = canonical_decode(bytes, "authenticated-map node")?;
        if wire.schema != SEALED_ACCEPTED_MAP_NODE_SCHEMA_VERSION || wire.key != expected.key {
            return Err(corrupt("authenticated-map node schema/key mismatch"));
        }
        let node = Self {
            key: wire.key,
            value_digest: ContentDigest::from_bytes(wire.value_digest),
            left: wire.left.map(Into::into),
            right: wire.right.map(Into::into),
        };
        if !valid_map_children(node.key, node.left.as_ref(), node.right.as_ref())
            || node.logical_digest() != expected.digest
        {
            return Err(corrupt("authenticated-map node binding mismatch"));
        }
        Ok(node)
    }
}

pub fn authenticated_map_empty_digest() -> ContentDigest {
    ContentDigest::of(AUTHENTICATED_MAP_EMPTY_DOMAIN)
}

pub fn authenticated_map_priority(key: [u8; 16]) -> ContentDigest {
    digest_fold(AUTHENTICATED_MAP_PRIORITY_DOMAIN, &[&key])
}

pub fn authenticated_map_priority_order(left: [u8; 16], right: [u8; 16]) -> Ordering {
    authenticated_map_priority(left)
        .as_bytes()
        .cmp(authenticated_map_priority(right).as_bytes())
        .then_with(|| left.cmp(&right))
}

pub fn authenticated_map_node_digest(
    key: [u8; 16],
    value_digest: ContentDigest,
    left: Option<([u8; 16], ContentDigest)>,
    right: Option<([u8; 16], ContentDigest)>,
) -> ContentDigest {
    let mut bytes = AUTHENTICATED_MAP_NODE_DOMAIN.to_vec();
    bytes.extend_from_slice(&key);
    bytes.extend_from_slice(value_digest.as_bytes());
    for child in [left, right] {
        match child {
            Some((child_key, digest)) => {
                bytes.push(1);
                bytes.extend_from_slice(&child_key);
                bytes.extend_from_slice(digest.as_bytes());
            }
            None => bytes.push(0),
        }
    }
    ContentDigest::of(&bytes)
}

/// Derive the exact V1 canonical treap root from strictly key-sorted entries.
pub fn authenticated_map_root(
    entries: &[([u8; 16], ContentDigest)],
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
    entries: &[([u8; 16], ContentDigest)],
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
pub struct AcceptedSequenceEntryV2 {
    pub sequence: u64,
    pub batch_id: [u8; 16],
    pub accepted_status_value_digest: ContentDigest,
}

impl AcceptedSequenceEntryV2 {
    pub fn entry_digest(self) -> ContentDigest {
        digest_fold(
            ACCEPTED_SEQUENCE_ENTRY_DOMAIN,
            &[
                &self.sequence.to_be_bytes(),
                &self.batch_id,
                self.accepted_status_value_digest.as_bytes(),
            ],
        )
    }

    pub fn encode_leaf(self) -> Result<Vec<u8>, SealedAcceptedIndexError> {
        canonical_encode(&AcceptedSequenceLeafWireV2 {
            schema: SEALED_ACCEPTED_SEQUENCE_SCHEMA_VERSION,
            sequence_be: self.sequence.to_be_bytes(),
            batch_id: self.batch_id,
            accepted_status_value_digest: *self.accepted_status_value_digest.as_bytes(),
        })
    }

    pub fn leaf_digest(self) -> Result<ContentDigest, SealedAcceptedIndexError> {
        Ok(digest_fold(
            ACCEPTED_SEQUENCE_LEAF_DOMAIN,
            &[&self.encode_leaf()?],
        ))
    }

    fn decode_leaf(
        expected_sequence: u64,
        expected_address: ContentDigest,
        bytes: &[u8],
    ) -> Result<Self, SealedAcceptedIndexError> {
        let wire: AcceptedSequenceLeafWireV2 = canonical_decode(bytes, "accepted-sequence leaf")?;
        let sequence = u64::from_be_bytes(wire.sequence_be);
        let entry = Self {
            sequence,
            batch_id: wire.batch_id,
            accepted_status_value_digest: ContentDigest::from_bytes(
                wire.accepted_status_value_digest,
            ),
        };
        if wire.schema != SEALED_ACCEPTED_SEQUENCE_SCHEMA_VERSION
            || sequence != expected_sequence
            || entry.leaf_digest()? != expected_address
        {
            return Err(corrupt("accepted-sequence leaf binding mismatch"));
        }
        Ok(entry)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct AcceptedSequenceLeafWireV2 {
    schema: u32,
    sequence_be: [u8; 8],
    batch_id: [u8; 16],
    accepted_status_value_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AcceptedSequenceChildV2 {
    pub first: u64,
    pub last: u64,
    pub digest: ContentDigest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptedSequenceNodeV2 {
    pub height: u8,
    pub first_leaf: u64,
    pub children: Vec<AcceptedSequenceChildV2>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct AcceptedSequenceChildWireV2 {
    first_be: [u8; 8],
    last_be: [u8; 8],
    digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct AcceptedSequenceNodeWireV2 {
    schema: u32,
    height: u8,
    first_leaf_be: [u8; 8],
    children: Vec<AcceptedSequenceChildWireV2>,
}

impl AcceptedSequenceNodeV2 {
    pub fn encode(&self) -> Result<Vec<u8>, SealedAcceptedIndexError> {
        validate_sequence_node(self)?;
        canonical_encode(&AcceptedSequenceNodeWireV2 {
            schema: SEALED_ACCEPTED_SEQUENCE_SCHEMA_VERSION,
            height: self.height,
            first_leaf_be: self.first_leaf.to_be_bytes(),
            children: self
                .children
                .iter()
                .map(|child| AcceptedSequenceChildWireV2 {
                    first_be: child.first.to_be_bytes(),
                    last_be: child.last.to_be_bytes(),
                    digest: *child.digest.as_bytes(),
                })
                .collect(),
        })
    }

    pub fn digest(&self) -> Result<ContentDigest, SealedAcceptedIndexError> {
        Ok(digest_fold(
            ACCEPTED_SEQUENCE_NODE_DOMAIN,
            &[&self.encode()?],
        ))
    }

    fn decode(
        expected_height: u8,
        expected_first: u64,
        expected_address: ContentDigest,
        bytes: &[u8],
    ) -> Result<Self, SealedAcceptedIndexError> {
        let wire: AcceptedSequenceNodeWireV2 = canonical_decode(bytes, "accepted-sequence node")?;
        let node = Self {
            height: wire.height,
            first_leaf: u64::from_be_bytes(wire.first_leaf_be),
            children: wire
                .children
                .into_iter()
                .map(|child| AcceptedSequenceChildV2 {
                    first: u64::from_be_bytes(child.first_be),
                    last: u64::from_be_bytes(child.last_be),
                    digest: ContentDigest::from_bytes(child.digest),
                })
                .collect(),
        };
        if wire.schema != SEALED_ACCEPTED_SEQUENCE_SCHEMA_VERSION
            || node.height != expected_height
            || node.first_leaf != expected_first
            || node.digest()? != expected_address
        {
            return Err(corrupt("accepted-sequence node binding mismatch"));
        }
        validate_sequence_node(&node)?;
        Ok(node)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AcceptedSequenceRootV2 {
    pub len: u64,
    pub height: u8,
    pub root_digest: Option<ContentDigest>,
}

impl Default for AcceptedSequenceRootV2 {
    fn default() -> Self {
        Self::empty()
    }
}

impl AcceptedSequenceRootV2 {
    pub const fn empty() -> Self {
        Self {
            len: 0,
            height: 0,
            root_digest: None,
        }
    }

    pub fn encode(self) -> Result<Vec<u8>, SealedAcceptedIndexError> {
        validate_sequence_root(self)?;
        canonical_encode(&AcceptedSequenceRootWireV2 {
            schema: SEALED_ACCEPTED_SEQUENCE_SCHEMA_VERSION,
            len: self.len,
            height: self.height,
            root_digest: self.root_digest.map(|digest| *digest.as_bytes()),
        })
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, SealedAcceptedIndexError> {
        let wire: AcceptedSequenceRootWireV2 = canonical_decode(bytes, "accepted-sequence root")?;
        let root = Self {
            len: wire.len,
            height: wire.height,
            root_digest: wire.root_digest.map(ContentDigest::from_bytes),
        };
        if wire.schema != SEALED_ACCEPTED_SEQUENCE_SCHEMA_VERSION {
            return Err(corrupt("accepted-sequence root schema mismatch"));
        }
        validate_sequence_root(root)?;
        Ok(root)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct AcceptedSequenceRootWireV2 {
    schema: u32,
    len: u64,
    height: u8,
    root_digest: Option<[u8; 32]>,
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
                    entry.peer_id,
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
            bytes.extend_from_slice(&root.key);
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SealedAcceptedIndexRootsV2 {
    pub batch_map: AuthenticatedMapRootV1,
    pub status_map: AuthenticatedMapRootV1,
    pub sequence: AcceptedSequenceRootV2,
}

impl SealedAcceptedIndexRootsV2 {
    pub fn validate_counts(self) -> Result<(), SealedAcceptedIndexError> {
        if self.batch_map.count != self.status_map.count
            || self.batch_map.count != self.sequence.len
        {
            return Err(corrupt("sealed accepted-index counts differ"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedAcceptedMembershipProofV2 {
    pub sequence: AcceptedSequenceEntryV2,
    pub status: AcceptedStatusRecordV2,
    pub causal: SealedAcceptedCausalRecordV2,
}

/// Domain fields recovered from one exact canonical accepted-evidence value.
///
/// `tine-storage` deliberately does not own Tine's V1/V2 evidence codecs. The
/// caller-supplied decoder below validates those bytes without causing this
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

pub struct SealedAcceptedIndexReader<'a, Store> {
    store: &'a Store,
}

impl<'a, Store: SealedAcceptedIndexObjectStore> SealedAcceptedIndexReader<'a, Store> {
    pub const fn new(store: &'a Store) -> Self {
        Self { store }
    }

    pub fn map_value(
        &self,
        root: AuthenticatedMapRootV1,
        key: [u8; 16],
    ) -> Result<Option<ContentDigest>, SealedAcceptedIndexError> {
        validate_map_root(root)?;
        let mut current = root.root;
        for _ in 0..MAX_ACCEPTED_INDEX_DEPTH {
            let Some(link) = current else { return Ok(None) };
            let node = self.read_map_node(link)?;
            match key.cmp(&node.key) {
                Ordering::Equal => return Ok(Some(node.value_digest)),
                Ordering::Less => current = node.left,
                Ordering::Greater => current = node.right,
            }
        }
        Err(SealedAcceptedIndexError::Capacity)
    }

    pub fn status(
        &self,
        root: AuthenticatedMapRootV1,
        batch_id: [u8; 16],
    ) -> Result<Option<AcceptedStatusRecordV2>, SealedAcceptedIndexError> {
        let Some(address) = self.map_value(root, batch_id)? else {
            return Ok(None);
        };
        let bytes = self.required(SealedAcceptedObjectKind::StatusRecord, address)?;
        Ok(Some(AcceptedStatusRecordV2::decode(
            batch_id, address, &bytes,
        )?))
    }

    pub fn causal(
        &self,
        batch_id: [u8; 16],
        address: ContentDigest,
    ) -> Result<SealedAcceptedCausalRecordV2, SealedAcceptedIndexError> {
        let bytes = self.required(SealedAcceptedObjectKind::CausalRecord, address)?;
        SealedAcceptedCausalRecordV2::decode(batch_id, address, &bytes)
    }

    pub fn sequence_entry(
        &self,
        root: AcceptedSequenceRootV2,
        sequence: u64,
    ) -> Result<Option<AcceptedSequenceEntryV2>, SealedAcceptedIndexError> {
        validate_sequence_root(root)?;
        if sequence == 0 || sequence > root.len {
            return Ok(None);
        }
        let mut address = root.root_digest.expect("validated nonempty sequence root");
        let mut height = root.height;
        let mut first = 1_u64;
        let mut depth = 0usize;
        while height > 0 {
            if depth >= MAX_ACCEPTED_INDEX_DEPTH {
                return Err(SealedAcceptedIndexError::Capacity);
            }
            let bytes = self.required(SealedAcceptedObjectKind::SequenceNode, address)?;
            let node = AcceptedSequenceNodeV2::decode(height, first, address, &bytes)?;
            let child = node
                .children
                .iter()
                .find(|child| child.first <= sequence && sequence <= child.last)
                .ok_or_else(|| corrupt("accepted-sequence node does not cover requested entry"))?;
            address = child.digest;
            first = child.first;
            height -= 1;
            depth += 1;
        }
        let bytes = self.required(SealedAcceptedObjectKind::SequenceLeaf, address)?;
        Ok(Some(AcceptedSequenceEntryV2::decode_leaf(
            sequence, address, &bytes,
        )?))
    }

    pub fn prove_membership<Decoder: SealedAcceptedEvidenceDecoder>(
        &self,
        roots: SealedAcceptedIndexRootsV2,
        sequence: u64,
        batch_id: [u8; 16],
        evidence_decoder: &Decoder,
    ) -> Result<Option<SealedAcceptedMembershipProofV2>, SealedAcceptedIndexError> {
        roots.validate_counts()?;
        let Some(sequence_entry) = self.sequence_entry(roots.sequence, sequence)? else {
            return Ok(None);
        };
        if sequence_entry.batch_id != batch_id {
            return Ok(None);
        }
        let Some(status) = self.status(roots.status_map, batch_id)? else {
            return Err(corrupt("sequence names a missing accepted-status record"));
        };
        if sequence_entry.accepted_status_value_digest != status.value_digest() {
            return Err(corrupt("sequence/status digest cross-check failed"));
        }
        let Some(causal_address) = self.map_value(roots.batch_map, batch_id)? else {
            return Err(corrupt(
                "accepted-status record names a missing batch-map entry",
            ));
        };
        if causal_address != status.accepted_causal_record_digest {
            return Err(corrupt("status/batch-map causal digest cross-check failed"));
        }
        let causal = self.causal(batch_id, causal_address)?;
        let evidence = evidence_decoder
            .decode_accepted_evidence(status.evidence_schema, &status.exact_evidence_bytes)?;
        if evidence.batch_id != batch_id
            || evidence.acceptance_sequence != sequence
            || evidence.manifest_fingerprint != causal.manifest_fingerprint
            || evidence.event_binding_digest != causal.event_binding_digest
        {
            return Err(corrupt(
                "accepted evidence/status/sequence/causal binding mismatch",
            ));
        }
        Ok(Some(SealedAcceptedMembershipProofV2 {
            sequence: sequence_entry,
            status,
            causal,
        }))
    }

    fn read_map_node(
        &self,
        link: AuthenticatedMapLinkV1,
    ) -> Result<SealedAuthenticatedMapNodeV2, SealedAcceptedIndexError> {
        let bytes = self.required(SealedAcceptedObjectKind::MapNode, link.digest)?;
        SealedAuthenticatedMapNodeV2::decode(link, &bytes)
    }

    fn required(
        &self,
        kind: SealedAcceptedObjectKind,
        address: ContentDigest,
    ) -> Result<Vec<u8>, SealedAcceptedIndexError> {
        self.store
            .read_sealed_accepted_object(kind, address)?
            .ok_or(SealedAcceptedIndexError::Missing { kind, address })
    }
}

pub struct SealedAcceptedIndexWriter<'a, Store> {
    store: &'a mut Store,
}

impl<'a, Store: SealedAcceptedIndexObjectStore> SealedAcceptedIndexWriter<'a, Store> {
    pub fn new(store: &'a mut Store) -> Self {
        Self { store }
    }

    pub fn publish_status(
        &mut self,
        record: &AcceptedStatusRecordV2,
    ) -> Result<ContentDigest, SealedAcceptedIndexError> {
        let bytes = record.encode()?;
        let address = record.value_digest();
        self.store.publish_sealed_accepted_object(
            SealedAcceptedObjectKind::StatusRecord,
            address,
            &bytes,
        )?;
        Ok(address)
    }

    pub fn publish_causal(
        &mut self,
        record: &SealedAcceptedCausalRecordV2,
    ) -> Result<ContentDigest, SealedAcceptedIndexError> {
        let bytes = record.encode()?;
        let address = record.address()?;
        self.store.publish_sealed_accepted_object(
            SealedAcceptedObjectKind::CausalRecord,
            address,
            &bytes,
        )?;
        Ok(address)
    }

    pub fn upsert_map(
        &mut self,
        root: AuthenticatedMapRootV1,
        key: [u8; 16],
        value_digest: ContentDigest,
    ) -> Result<AuthenticatedMapRootV1, SealedAcceptedIndexError> {
        validate_map_root(root)?;
        let (link, inserted) = self.upsert_map_child(root.root, key, value_digest, 0)?;
        Ok(AuthenticatedMapRootV1 {
            count: if inserted {
                root.count
                    .checked_add(1)
                    .ok_or(SealedAcceptedIndexError::Capacity)?
            } else {
                root.count
            },
            root: Some(link),
        })
    }

    pub fn append_sequence(
        &mut self,
        root: AcceptedSequenceRootV2,
        entry: AcceptedSequenceEntryV2,
    ) -> Result<AcceptedSequenceRootV2, SealedAcceptedIndexError> {
        validate_sequence_root(root)?;
        let expected = root
            .len
            .checked_add(1)
            .ok_or(SealedAcceptedIndexError::Capacity)?;
        if entry.sequence != expected {
            return Err(SealedAcceptedIndexError::NonContiguousSequence {
                expected,
                actual: entry.sequence,
            });
        }
        let leaf_bytes = entry.encode_leaf()?;
        let leaf_digest = entry.leaf_digest()?;
        self.store.publish_sealed_accepted_object(
            SealedAcceptedObjectKind::SequenceLeaf,
            leaf_digest,
            &leaf_bytes,
        )?;

        let next_len = root
            .len
            .checked_add(1)
            .ok_or(SealedAcceptedIndexError::Capacity)?;
        if root.len == 0 {
            return Ok(AcceptedSequenceRootV2 {
                len: 1,
                height: 0,
                root_digest: Some(leaf_digest),
            });
        }

        let old_capacity = sequence_capacity(root.height)?;
        let (height, digest) = if root.len == old_capacity {
            let right = self.build_sequence_path(root.height, entry.sequence, leaf_digest)?;
            let node = AcceptedSequenceNodeV2 {
                height: root
                    .height
                    .checked_add(1)
                    .ok_or(SealedAcceptedIndexError::Capacity)?,
                first_leaf: 1,
                children: vec![
                    AcceptedSequenceChildV2 {
                        first: 1,
                        last: root.len,
                        digest: root.root_digest.expect("validated sequence root"),
                    },
                    AcceptedSequenceChildV2 {
                        first: entry.sequence,
                        last: entry.sequence,
                        digest: right,
                    },
                ],
            };
            let digest = self.publish_sequence_node(&node)?;
            (node.height, digest)
        } else {
            let digest = self.append_sequence_path(
                root.height,
                1,
                root.root_digest.expect("validated sequence root"),
                entry.sequence,
                leaf_digest,
                0,
            )?;
            (root.height, digest)
        };
        let next = AcceptedSequenceRootV2 {
            len: next_len,
            height,
            root_digest: Some(digest),
        };
        validate_sequence_root(next)?;
        Ok(next)
    }

    fn upsert_map_child(
        &mut self,
        current: Option<AuthenticatedMapLinkV1>,
        key: [u8; 16],
        value_digest: ContentDigest,
        depth: usize,
    ) -> Result<(AuthenticatedMapLinkV1, bool), SealedAcceptedIndexError> {
        ensure_index_depth(depth)?;
        let Some(current) = current else {
            return Ok((
                self.publish_map_node(&SealedAuthenticatedMapNodeV2 {
                    key,
                    value_digest,
                    left: None,
                    right: None,
                })?,
                true,
            ));
        };
        let mut node = self.read_map_node(current)?;
        let inserted;
        match key.cmp(&node.key) {
            Ordering::Equal => {
                node.value_digest = value_digest;
                inserted = false;
            }
            Ordering::Less => {
                let (left, was_inserted) =
                    self.upsert_map_child(node.left.take(), key, value_digest, depth + 1)?;
                node.left = Some(left);
                inserted = was_inserted;
                if authenticated_map_priority_order(left.key, node.key).is_lt() {
                    return Ok((self.rotate_map_right(node)?, inserted));
                }
            }
            Ordering::Greater => {
                let (right, was_inserted) =
                    self.upsert_map_child(node.right.take(), key, value_digest, depth + 1)?;
                node.right = Some(right);
                inserted = was_inserted;
                if authenticated_map_priority_order(right.key, node.key).is_lt() {
                    return Ok((self.rotate_map_left(node)?, inserted));
                }
            }
        }
        Ok((self.publish_map_node(&node)?, inserted))
    }

    fn rotate_map_right(
        &mut self,
        mut node: SealedAuthenticatedMapNodeV2,
    ) -> Result<AuthenticatedMapLinkV1, SealedAcceptedIndexError> {
        let left = node
            .left
            .take()
            .ok_or_else(|| corrupt("right rotation has no left child"))?;
        let mut left_node = self.read_map_node(left)?;
        node.left = left_node.right.take();
        left_node.right = Some(self.publish_map_node(&node)?);
        self.publish_map_node(&left_node)
    }

    fn rotate_map_left(
        &mut self,
        mut node: SealedAuthenticatedMapNodeV2,
    ) -> Result<AuthenticatedMapLinkV1, SealedAcceptedIndexError> {
        let right = node
            .right
            .take()
            .ok_or_else(|| corrupt("left rotation has no right child"))?;
        let mut right_node = self.read_map_node(right)?;
        node.right = right_node.left.take();
        right_node.left = Some(self.publish_map_node(&node)?);
        self.publish_map_node(&right_node)
    }

    fn read_map_node(
        &self,
        link: AuthenticatedMapLinkV1,
    ) -> Result<SealedAuthenticatedMapNodeV2, SealedAcceptedIndexError> {
        let bytes = self
            .store
            .read_sealed_accepted_object(SealedAcceptedObjectKind::MapNode, link.digest)?
            .ok_or(SealedAcceptedIndexError::Missing {
                kind: SealedAcceptedObjectKind::MapNode,
                address: link.digest,
            })?;
        SealedAuthenticatedMapNodeV2::decode(link, &bytes)
    }

    fn publish_map_node(
        &mut self,
        node: &SealedAuthenticatedMapNodeV2,
    ) -> Result<AuthenticatedMapLinkV1, SealedAcceptedIndexError> {
        let address = node.logical_digest();
        let bytes = node.encode()?;
        self.store.publish_sealed_accepted_object(
            SealedAcceptedObjectKind::MapNode,
            address,
            &bytes,
        )?;
        Ok(AuthenticatedMapLinkV1 {
            key: node.key,
            digest: address,
        })
    }

    fn build_sequence_path(
        &mut self,
        height: u8,
        sequence: u64,
        leaf_digest: ContentDigest,
    ) -> Result<ContentDigest, SealedAcceptedIndexError> {
        if height == 0 {
            return Ok(leaf_digest);
        }
        let child = self.build_sequence_path(height - 1, sequence, leaf_digest)?;
        self.publish_sequence_node(&AcceptedSequenceNodeV2 {
            height,
            first_leaf: sequence,
            children: vec![AcceptedSequenceChildV2 {
                first: sequence,
                last: sequence,
                digest: child,
            }],
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn append_sequence_path(
        &mut self,
        height: u8,
        first: u64,
        address: ContentDigest,
        sequence: u64,
        leaf_digest: ContentDigest,
        depth: usize,
    ) -> Result<ContentDigest, SealedAcceptedIndexError> {
        ensure_index_depth(depth)?;
        if height == 0 {
            return Err(SealedAcceptedIndexError::Capacity);
        }
        let bytes = self
            .store
            .read_sealed_accepted_object(SealedAcceptedObjectKind::SequenceNode, address)?
            .ok_or(SealedAcceptedIndexError::Missing {
                kind: SealedAcceptedObjectKind::SequenceNode,
                address,
            })?;
        let mut node = AcceptedSequenceNodeV2::decode(height, first, address, &bytes)?;
        let child_capacity = sequence_capacity(height - 1)?;
        let relative = sequence
            .checked_sub(first)
            .ok_or_else(|| corrupt("sequence append precedes subtree"))?;
        let child_index: usize = (relative / child_capacity)
            .try_into()
            .map_err(|_| SealedAcceptedIndexError::Capacity)?;
        if child_index > node.children.len() || child_index >= SEALED_ACCEPTED_SEQUENCE_FANOUT {
            return Err(corrupt("accepted-sequence append is not left-packed"));
        }
        if child_index == node.children.len() {
            let child = self.build_sequence_path(height - 1, sequence, leaf_digest)?;
            node.children.push(AcceptedSequenceChildV2 {
                first: sequence,
                last: sequence,
                digest: child,
            });
        } else {
            let existing = node.children[child_index];
            if sequence != existing.last.saturating_add(1) {
                return Err(corrupt("accepted-sequence child append is not contiguous"));
            }
            let digest = if height == 1 {
                if existing.first != existing.last {
                    return Err(corrupt("accepted-sequence leaf child has a range"));
                }
                leaf_digest
            } else {
                self.append_sequence_path(
                    height - 1,
                    existing.first,
                    existing.digest,
                    sequence,
                    leaf_digest,
                    depth + 1,
                )?
            };
            node.children[child_index] = AcceptedSequenceChildV2 {
                first: existing.first,
                last: sequence,
                digest,
            };
        }
        self.publish_sequence_node(&node)
    }

    fn publish_sequence_node(
        &mut self,
        node: &AcceptedSequenceNodeV2,
    ) -> Result<ContentDigest, SealedAcceptedIndexError> {
        let bytes = node.encode()?;
        let digest = node.digest()?;
        self.store.publish_sealed_accepted_object(
            SealedAcceptedObjectKind::SequenceNode,
            digest,
            &bytes,
        )?;
        Ok(digest)
    }
}

fn validate_map_root(root: AuthenticatedMapRootV1) -> Result<(), SealedAcceptedIndexError> {
    if (root.count == 0) != root.root.is_none()
        || (root.count == 0 && root.root_digest() != authenticated_map_empty_digest())
    {
        return Err(corrupt("authenticated-map root count/binding mismatch"));
    }
    Ok(())
}

fn valid_map_children(
    key: [u8; 16],
    left: Option<&AuthenticatedMapLinkV1>,
    right: Option<&AuthenticatedMapLinkV1>,
) -> bool {
    left.is_none_or(|child| {
        child.key < key && authenticated_map_priority_order(key, child.key).is_lt()
    }) && right.is_none_or(|child| {
        child.key > key && authenticated_map_priority_order(key, child.key).is_lt()
    })
}

fn validate_sequence_root(root: AcceptedSequenceRootV2) -> Result<(), SealedAcceptedIndexError> {
    if (root.len == 0) != root.root_digest.is_none() || (root.len == 0 && root.height != 0) {
        return Err(corrupt("accepted-sequence root count/binding mismatch"));
    }
    if root.len > 0 {
        let capacity = sequence_capacity(root.height)?;
        if root.len > capacity
            || (root.height > 0 && root.len <= sequence_capacity(root.height - 1)?)
        {
            return Err(corrupt("accepted-sequence root height is not minimal"));
        }
    }
    Ok(())
}

fn validate_sequence_node(node: &AcceptedSequenceNodeV2) -> Result<(), SealedAcceptedIndexError> {
    if node.height == 0
        || node.first_leaf == 0
        || node.children.is_empty()
        || node.children.len() > SEALED_ACCEPTED_SEQUENCE_FANOUT
        || node.children[0].first != node.first_leaf
    {
        return Err(corrupt("accepted-sequence node shape is invalid"));
    }
    let child_capacity = sequence_capacity(node.height - 1)?;
    for (index, child) in node.children.iter().enumerate() {
        let expected_first = node
            .first_leaf
            .checked_add(
                u64::try_from(index)
                    .map_err(|_| SealedAcceptedIndexError::Capacity)?
                    .checked_mul(child_capacity)
                    .ok_or(SealedAcceptedIndexError::Capacity)?,
            )
            .ok_or(SealedAcceptedIndexError::Capacity)?;
        if child.first != expected_first || child.last < child.first {
            return Err(corrupt("accepted-sequence children are not left-packed"));
        }
        let used = child
            .last
            .checked_sub(child.first)
            .and_then(|span| span.checked_add(1))
            .ok_or(SealedAcceptedIndexError::Capacity)?;
        if used > child_capacity || (index + 1 < node.children.len() && used != child_capacity) {
            return Err(corrupt("accepted-sequence child range is not canonical"));
        }
    }
    Ok(())
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

fn sequence_capacity(height: u8) -> Result<u64, SealedAcceptedIndexError> {
    let mut capacity = SEALED_ACCEPTED_SEQUENCE_LEAF_CAPACITY as u64;
    for _ in 0..height {
        capacity = capacity
            .checked_mul(SEALED_ACCEPTED_SEQUENCE_FANOUT as u64)
            .ok_or(SealedAcceptedIndexError::Capacity)?;
    }
    Ok(capacity)
}

fn ensure_index_depth(depth: usize) -> Result<(), SealedAcceptedIndexError> {
    if depth >= MAX_ACCEPTED_INDEX_DEPTH {
        Err(SealedAcceptedIndexError::Capacity)
    } else {
        Ok(())
    }
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

    #[derive(Clone, Default)]
    struct MemoryStore {
        objects: Vec<(SealedAcceptedObjectKind, ContentDigest, Vec<u8>)>,
    }

    impl SealedAcceptedIndexObjectStore for MemoryStore {
        fn read_sealed_accepted_object(
            &self,
            kind: SealedAcceptedObjectKind,
            address: ContentDigest,
        ) -> Result<Option<Vec<u8>>, SealedAcceptedIndexError> {
            Ok(self
                .objects
                .iter()
                .find(|(stored_kind, stored_address, _)| {
                    *stored_kind == kind && *stored_address == address
                })
                .map(|(_, _, bytes)| bytes.clone()))
        }

        fn publish_sealed_accepted_object(
            &mut self,
            kind: SealedAcceptedObjectKind,
            address: ContentDigest,
            bytes: &[u8],
        ) -> Result<(), SealedAcceptedIndexError> {
            if let Some((_, _, existing)) =
                self.objects
                    .iter()
                    .find(|(stored_kind, stored_address, _)| {
                        *stored_kind == kind && *stored_address == address
                    })
            {
                if existing != bytes {
                    return Err(corrupt("same object address has different bytes"));
                }
                return Ok(());
            }
            self.objects.push((kind, address, bytes.to_vec()));
            Ok(())
        }
    }

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

    #[test]
    fn persistent_map_upsert_is_order_independent_and_point_readable() {
        let entries = [
            ([0x10; 16], digest(0xa0)),
            ([0x20; 16], digest(0xb0)),
            ([0x30; 16], digest(0xc0)),
        ];
        let expected = authenticated_map_root(&entries).unwrap();

        for order in [[0, 1, 2], [2, 0, 1], [1, 2, 0]] {
            let mut store = MemoryStore::default();
            let mut root = AuthenticatedMapRootV1::empty();
            {
                let mut writer = SealedAcceptedIndexWriter::new(&mut store);
                for index in order {
                    root = writer
                        .upsert_map(root, entries[index].0, entries[index].1)
                        .unwrap();
                }
            }
            assert_eq!(root, expected);
            let reader = SealedAcceptedIndexReader::new(&store);
            for (key, value) in entries {
                assert_eq!(reader.map_value(root, key).unwrap(), Some(value));
            }
            assert_eq!(reader.map_value(root, [0xff; 16]).unwrap(), None);
        }
    }

    #[test]
    fn sequence_append_crosses_fanout_and_height_boundaries() {
        let mut store = MemoryStore::default();
        let mut root = AcceptedSequenceRootV2::empty();
        {
            let mut writer = SealedAcceptedIndexWriter::new(&mut store);
            for sequence in 1..=1025_u64 {
                root = writer
                    .append_sequence(
                        root,
                        AcceptedSequenceEntryV2 {
                            sequence,
                            batch_id: [(sequence % 251) as u8; 16],
                            accepted_status_value_digest: digest((sequence % 253) as u8),
                        },
                    )
                    .unwrap();
                let expected_height = match root.len {
                    0 | 1 => 0,
                    2..=32 => 1,
                    33..=1024 => 2,
                    _ => 3,
                };
                assert_eq!(root.height, expected_height);
            }
        }
        assert_eq!(root.len, 1025);
        let reader = SealedAcceptedIndexReader::new(&store);
        for sequence in [1, 2, 32, 33, 34, 1024, 1025] {
            let entry = reader.sequence_entry(root, sequence).unwrap().unwrap();
            assert_eq!(entry.sequence, sequence);
            assert_eq!(entry.batch_id, [(sequence % 251) as u8; 16]);
        }
        assert_eq!(reader.sequence_entry(root, 0).unwrap(), None);
        assert_eq!(reader.sequence_entry(root, 1026).unwrap(), None);
    }

    #[test]
    fn one_based_sequence_roots_are_frozen_at_growth_boundaries() {
        let mut store = MemoryStore::default();
        let mut root = AcceptedSequenceRootV2::empty();
        let mut roots = Vec::new();
        {
            let mut writer = SealedAcceptedIndexWriter::new(&mut store);
            for sequence in 1..=1025_u64 {
                root = writer
                    .append_sequence(
                        root,
                        AcceptedSequenceEntryV2 {
                            sequence,
                            batch_id: [(sequence % 251) as u8; 16],
                            accepted_status_value_digest: digest((sequence % 253) as u8),
                        },
                    )
                    .unwrap();
                if matches!(sequence, 1 | 32 | 33 | 1024 | 1025) {
                    roots.push((sequence, root.height, root.root_digest.unwrap().to_string()));
                }
            }
        }
        assert_eq!(
            roots,
            vec![
                (
                    1,
                    0,
                    "26a54cac813394adfb132def56ba1054f46ce1314e4bd1a57d003de08c07bdb1".into()
                ),
                (
                    32,
                    1,
                    "2fe1c7e764443227a6acc0641e8d5a6de6b17a8450d145874aa1ae1f85dfdd6c".into()
                ),
                (
                    33,
                    2,
                    "c15156bcb38c1317cb34eac5ef1f8033b6c63f82777ca933190e9d93bd563cf4".into()
                ),
                (
                    1024,
                    2,
                    "bc937691a47d2ef02488b65437b094632a4e5fb5816f1790e200018a02663250".into()
                ),
                (
                    1025,
                    3,
                    "3d6bc3cb03eef88484a27a64dbf134c795843c7b0ab162de69facba4ca079d67".into()
                ),
            ]
        );
    }

    #[test]
    fn full_membership_proof_cross_checks_sequence_status_batch_map_and_causal_record() {
        let batch = [0x51; 16];
        let causal = causal(batch);
        let mut store = MemoryStore::default();
        let (causal_address, status_record, status_address, batch_root, status_root, sequence_root);
        {
            let mut writer = SealedAcceptedIndexWriter::new(&mut store);
            causal_address = writer.publish_causal(&causal).unwrap();
            status_record = status(batch, causal_address);
            status_address = writer.publish_status(&status_record).unwrap();
            batch_root = writer
                .upsert_map(AuthenticatedMapRootV1::empty(), batch, causal_address)
                .unwrap();
            status_root = writer
                .upsert_map(AuthenticatedMapRootV1::empty(), batch, status_address)
                .unwrap();
            sequence_root = writer
                .append_sequence(
                    AcceptedSequenceRootV2::empty(),
                    AcceptedSequenceEntryV2 {
                        sequence: 1,
                        batch_id: batch,
                        accepted_status_value_digest: status_address,
                    },
                )
                .unwrap();
        }
        let proof = SealedAcceptedIndexReader::new(&store)
            .prove_membership(
                SealedAcceptedIndexRootsV2 {
                    batch_map: batch_root,
                    status_map: status_root,
                    sequence: sequence_root,
                },
                1,
                batch,
                &TestEvidenceDecoder,
            )
            .unwrap()
            .unwrap();
        assert_eq!(proof.status, status_record);
        assert_eq!(proof.causal, causal);

        let wrong_status_root = {
            let mut writer = SealedAcceptedIndexWriter::new(&mut store);
            writer.upsert_map(status_root, batch, digest(0xfe)).unwrap()
        };
        assert!(SealedAcceptedIndexReader::new(&store)
            .prove_membership(
                SealedAcceptedIndexRootsV2 {
                    batch_map: batch_root,
                    status_map: wrong_status_root,
                    sequence: sequence_root,
                },
                1,
                batch,
                &TestEvidenceDecoder,
            )
            .is_err());

        let evidence_variants = [
            vec![1, 2, 3],
            canonical_encode(&TestEvidenceWire {
                schema: 3,
                batch_id: batch,
                manifest_fingerprint: [0x22; 32],
                event_binding_digest: [0x33; 32],
                acceptance_sequence: 1,
            })
            .unwrap(),
            canonical_encode(&TestEvidenceWire {
                schema: 1,
                batch_id: [0x52; 16],
                manifest_fingerprint: [0x22; 32],
                event_binding_digest: [0x33; 32],
                acceptance_sequence: 1,
            })
            .unwrap(),
            canonical_encode(&TestEvidenceWire {
                schema: 1,
                batch_id: batch,
                manifest_fingerprint: [0x23; 32],
                event_binding_digest: [0x33; 32],
                acceptance_sequence: 1,
            })
            .unwrap(),
            canonical_encode(&TestEvidenceWire {
                schema: 1,
                batch_id: batch,
                manifest_fingerprint: [0x22; 32],
                event_binding_digest: [0x34; 32],
                acceptance_sequence: 1,
            })
            .unwrap(),
            canonical_encode(&TestEvidenceWire {
                schema: 1,
                batch_id: batch,
                manifest_fingerprint: [0x22; 32],
                event_binding_digest: [0x33; 32],
                acceptance_sequence: 2,
            })
            .unwrap(),
        ];
        for (index, exact_evidence_bytes) in evidence_variants.into_iter().enumerate() {
            let mut bad = status_record.clone();
            bad.exact_evidence_bytes = exact_evidence_bytes;
            if index == 1 {
                bad.evidence_schema = 3;
            }
            let (bad_status_root, bad_sequence_root) = {
                let mut writer = SealedAcceptedIndexWriter::new(&mut store);
                let address = writer.publish_status(&bad).unwrap();
                let status_root = writer
                    .upsert_map(AuthenticatedMapRootV1::empty(), batch, address)
                    .unwrap();
                let sequence_root = writer
                    .append_sequence(
                        AcceptedSequenceRootV2::empty(),
                        AcceptedSequenceEntryV2 {
                            sequence: 1,
                            batch_id: batch,
                            accepted_status_value_digest: address,
                        },
                    )
                    .unwrap();
                (status_root, sequence_root)
            };
            assert!(SealedAcceptedIndexReader::new(&store)
                .prove_membership(
                    SealedAcceptedIndexRootsV2 {
                        batch_map: batch_root,
                        status_map: bad_status_root,
                        sequence: bad_sequence_root,
                    },
                    1,
                    batch,
                    &TestEvidenceDecoder,
                )
                .is_err());
        }

        let sequence_status_mismatch = {
            let mut writer = SealedAcceptedIndexWriter::new(&mut store);
            writer
                .append_sequence(
                    AcceptedSequenceRootV2::empty(),
                    AcceptedSequenceEntryV2 {
                        sequence: 1,
                        batch_id: batch,
                        accepted_status_value_digest: digest(0xfd),
                    },
                )
                .unwrap()
        };
        assert!(SealedAcceptedIndexReader::new(&store)
            .prove_membership(
                SealedAcceptedIndexRootsV2 {
                    batch_map: batch_root,
                    status_map: status_root,
                    sequence: sequence_status_mismatch,
                },
                1,
                batch,
                &TestEvidenceDecoder,
            )
            .is_err());

        let mut other_causal = causal.clone();
        other_causal.event_binding_digest = digest(0x35);
        let wrong_batch_root = {
            let mut writer = SealedAcceptedIndexWriter::new(&mut store);
            let address = writer.publish_causal(&other_causal).unwrap();
            writer
                .upsert_map(AuthenticatedMapRootV1::empty(), batch, address)
                .unwrap()
        };
        assert!(SealedAcceptedIndexReader::new(&store)
            .prove_membership(
                SealedAcceptedIndexRootsV2 {
                    batch_map: wrong_batch_root,
                    status_map: status_root,
                    sequence: sequence_root,
                },
                1,
                batch,
                &TestEvidenceDecoder,
            )
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

        assert!(ensure_index_depth(MAX_ACCEPTED_INDEX_DEPTH - 1).is_ok());
        assert_eq!(
            ensure_index_depth(MAX_ACCEPTED_INDEX_DEPTH),
            Err(SealedAcceptedIndexError::Capacity)
        );
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn v1_and_v2_golden_vectors_are_frozen() {
        let key = [0x11; 16];
        let priority = authenticated_map_priority(key).to_string();
        let empty = authenticated_map_empty_digest().to_string();
        let node = authenticated_map_node_digest(key, digest(0x22), None, None).to_string();
        let root = authenticated_map_root(&[
            ([0x10; 16], digest(0xa0)),
            ([0x20; 16], digest(0xb0)),
            ([0x30; 16], digest(0xc0)),
        ])
        .unwrap();

        let causal = causal([0x51; 16]);
        let causal_bytes = hex(&causal.encode().unwrap());
        let causal_address = causal.address().unwrap().to_string();
        let clock_root = causal.clock_root().unwrap();
        let status = status([0x51; 16], causal.address().unwrap());
        let status_bytes = hex(&status.encode().unwrap());
        let status_digest = status.value_digest().to_string();
        let leaf = AcceptedSequenceEntryV2 {
            sequence: 0x0102_0304_0506_0708,
            batch_id: [0x51; 16],
            accepted_status_value_digest: status.value_digest(),
        };
        let leaf_bytes = hex(&leaf.encode_leaf().unwrap());
        let leaf_digest = leaf.leaf_digest().unwrap().to_string();
        let node_bytes = hex(&AcceptedSequenceNodeV2 {
            height: 1,
            first_leaf: 1,
            children: vec![AcceptedSequenceChildV2 {
                first: 1,
                last: 1,
                digest: leaf.leaf_digest().unwrap(),
            }],
        }
        .encode()
        .unwrap());

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
            "e565466183c18e6d795f150dc3294acaed8028c54880f1135ab51cf2bb1fbf23"
        );
        assert_eq!(
            root.root_digest().to_string(),
            "57a8918c46769c518761d22c6d8a6087d57e0897a06f62e6ee52a2c434fbed8d"
        );
        assert_eq!(causal_bytes, "02515151515151515151515151515151512222222222222222222222222222222222222222222222222222222222222222333333333333333333333333333333333333333333333333333333333333333344444444444444444444444444444444070211111111111111111111111111111111034444444444444444444444444444444407");
        assert_eq!(
            causal_address,
            "7f4986b2491f46879adadfd66a4f7c3f516006c123868ad7ecff6d5791b80756"
        );
        assert_eq!(
            clock_root.root_digest().to_string(),
            "effa60f99d8c9c9560c9f6d176ce457070460af5152e813d6affdd6cb48496d2"
        );
        assert_eq!(status_bytes, "0251515151515151515151515151515151000152015151515151515151515151515151515122222222222222222222222222222222222222222222222222222222222222223333333333333333333333333333333333333333333333333333333333333333017f4986b2491f46879adadfd66a4f7c3f516006c123868ad7ecff6d5791b80756");
        assert_eq!(
            status_digest,
            "6bdd768fa218e9f43ad0cf93530f2a8fb951f77403d989e78bfd8cb8a6b3a2c1"
        );
        assert_eq!(leaf_bytes, "020102030405060708515151515151515151515151515151516bdd768fa218e9f43ad0cf93530f2a8fb951f77403d989e78bfd8cb8a6b3a2c1");
        assert_eq!(
            leaf_digest,
            "e75ae7327f31bc853a826ce6386c6786f0452da204cecb25f4f0ef7c883bac74"
        );
        assert_eq!(node_bytes, "020100000000000000010100000000000000010000000000000001e75ae7327f31bc853a826ce6386c6786f0452da204cecb25f4f0ef7c883bac74");
    }
}
