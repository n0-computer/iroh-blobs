//! Example: store plaintext locally, serve ciphertext on demand.
//!
//! A common shape for end-to-end encrypted storage: a device holds the
//! plaintext (or the file it came from) and wants peers to fetch the
//! *encrypted* form, which is what gets replicated. The
//! ciphertext itself never needs to be stored on the serving device
//! allowing local readers plaintext access.
//!
//! This example wires that up with virtual blobs:
//!
//! 1. Encrypt a plaintext into an RFC 8188 `aes128gcm` payload (a key pair of
//!    salt + input keying material is generated per blob; the salt rides in
//!    the payload header). The bytes of this ciphertext are never written to
//!    disk - only its bao outboard and root hash are installed as a
//!    *virtual* entry.
//! 2. Register an on-demand provider that holds the key + plaintext and
//!    re-encrypts only the records a peer asks for. `aes128gcm` records are
//!    independently computable, so random access costs a couple of AES-GCM
//!    operations per seek, regardless of blob size.
//! 3. A second node fetches the ciphertext over iroh, bao-verifies it against
//!    the ciphertext hash, stores it locally, and decrypts it.
//!
//! The demo codec below is a compact standalone implementation of RFC 8188's
//! `aes128gcm` framing. Use a well-audited real-world codec in production.
//!
//! Run with `cargo run --example encrypted_virtual_blob`.

use std::collections::HashMap;
use std::sync::Arc;

use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::Aes128Gcm;
use anyhow::{bail, ensure, Result};
use bao_tree::io::mixed::ReadBytesAt;
use hkdf::Hkdf;
use iroh::address_lookup::MemoryLookup;
use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh_blobs::api::Store;
use iroh_blobs::store::mem::MemStore;
use iroh_blobs::store::virtual_blob::{DynVirtualSource, Provider, VirtualProviders};
use iroh_blobs::store::IROH_BLOCK_SIZE;
use iroh_blobs::{ALPN, BlobsProtocol, Hash};
use sha2::Sha256;

const PROVIDER_NAME: &str = "example-encrypted-blob-v1";
const RECORD_SIZE: u64 = 1024; // small on purpose: many records for the demo
const RECORD_OVERHEAD: u64 = 17; // 1 delimiter octet + 16 octet GCM tag
const PAYLOAD_MAX: u64 = RECORD_SIZE - RECORD_OVERHEAD;
const HEADER_LEN: u64 = 21; // RFC 8188: salt (16) + rs (4) + idlen (1); no key id
const SALT_LEN: usize = 16;

/// A generated blob key: 32-octet input-keying material plus the per-blob
/// salt that is transmitted in the plaintext header.
///
/// Never reuse (ikm, salt) across two different plaintexts: aes128gcm derives
/// its CEKs and nonces from them, and GCM is catastrophic if a (CEK, nonce)
/// pair repeats.
#[derive(Clone)]
struct BlobKey {
    ikm: [u8; 32],
    salt: [u8; SALT_LEN],
}

impl BlobKey {
    fn generate() -> Self {
        use rand::RngExt;
        let mut ikm = [0u8; 32];
        rand::rng().fill(&mut ikm);
        let mut salt = [0u8; SALT_LEN];
        rand::rng().fill(&mut salt);
        Self { ikm, salt }
    }
}

/// The per-blob cipher: CEK and nonce base are HKDF-SHA256 expansions of the
/// input-keying material under the salt (RFC 8188 §2.2/§2.3).
struct DemoCipher {
    salt: [u8; SALT_LEN],
    aead: Aes128Gcm,
    nonce_base: [u8; 12],
}

impl DemoCipher {
    fn new(ikm: &[u8], salt: &[u8; SALT_LEN]) -> Self {
        let hk = Hkdf::<Sha256>::new(Some(salt), ikm);
        let mut cek = [0u8; 16];
        hk.expand(b"Content-Encoding: aes128gcm\x00", &mut cek)
            .expect("cek length valid");
        let mut nonce_base = [0u8; 12];
        hk.expand(b"Content-Encoding: nonce\x00", &mut nonce_base)
            .expect("nonce length valid");
        Self {
            salt: *salt,
            aead: Aes128Gcm::new_from_slice(&cek).expect("cek length valid"),
            nonce_base,
        }
    }

    /// RFC 8188 §2.3: nonce = nonce_base XOR 96-bit record sequence number.
    fn nonce(&self, seq: u64) -> [u8; 12] {
        let mut n = self.nonce_base;
        for (i, b) in seq.to_be_bytes().iter().enumerate() {
            n[4 + i] ^= b;
        }
        n
    }

    fn encrypt_record(&self, seq: u64, last: bool, mut plaintext: Vec<u8>) -> Vec<u8> {
        plaintext.push(if last {
            0x02 // final record delimiter
        } else {
            0x01
        });
        self.aead
            .encrypt_in_place(aes_gcm::Nonce::from_slice(&self.nonce(seq)), &[], &mut plaintext)
            .expect("aes-gcm encryption cannot fail");
        plaintext
    }

    fn decrypt_record(&self, seq: u64, record: &[u8]) -> Result<Vec<u8>> {
        let mut buf = record.to_vec();
        self.aead
            .decrypt_in_place(aes_gcm::Nonce::from_slice(&self.nonce(seq)), &[], &mut buf)
            .map_err(|e| anyhow::anyhow!("record auth failed: {e:?}"))?;
        buf.pop();
        Ok(buf)
    }
}

/// Build the RFC 8188 body: header (salt, record size, empty key id) followed
/// by all records. The final record carries delimiter 0x02 and may be short.
fn encrypt_bytes(key: &BlobKey, plaintext: &[u8]) -> Vec<u8> {
    let cipher = DemoCipher::new(&key.ikm, &key.salt);
    let chunk_max = PAYLOAD_MAX as usize;
    let mut out = Vec::with_capacity(plaintext.len() + 64);
    out.extend_from_slice(&cipher.salt);
    out.extend_from_slice(&RECORD_SIZE.to_be_bytes()[4..]); // rs: 32-bit BE
    out.push(0); // idlen: no key id
    if plaintext.is_empty() {
        out.extend_from_slice(&cipher.encrypt_record(0, true, Vec::new()));
        return out;
    }
    let n_records = plaintext.len().div_ceil(chunk_max);
    for (seq, chunk) in plaintext.chunks(chunk_max).enumerate() {
        out.extend_from_slice(&cipher.encrypt_record(
            seq as u64,
            seq == n_records - 1,
            chunk.to_vec(),
        ));
    }
    out
}

/// Decrypt a full RFC 8188 body: parse the header, then every record.
fn decrypt_bytes(key: &BlobKey, ciphertext: &[u8]) -> Result<Vec<u8>> {
    ensure!(
        ciphertext.len() >= HEADER_LEN as usize,
        "ciphertext shorter than header"
    );
    let rs = u32::from_be_bytes(ciphertext[16..20].try_into()?) as u64;
    let idlen = ciphertext[20] as usize;
    let cipher = DemoCipher::new(&key.ikm, &ciphertext[..SALT_LEN].try_into()?);
    let records = &ciphertext[HEADER_LEN as usize + idlen..];
    let mut out = Vec::new();
    let mut seq = 0u64;
    let mut offset = 0usize;
    loop {
        let take = (records.len() - offset).min(rs as usize);
        ensure!(take > RECORD_OVERHEAD as usize, "truncated record");
        let payload = cipher.decrypt_record(seq, &records[offset..offset + take])?;
        out.extend_from_slice(&payload);
        let last = offset + take == records.len();
        if last {
            break;
        }
        seq += 1;
        offset += take;
        ensure!(offset < records.len(), "ciphertext ends without final record");
    }
    Ok(out)
}

/// On-demand encryptor over stored plaintext.
///
/// The salt needed to reproduce wire-identical records travels along with the
/// key and plaintext (it also appears in the header, so it does not need to
/// be secret).
struct OnDemandEncryptor {
    key: BlobKey,
    plain: bytes::Bytes,
}

impl OnDemandEncryptor {
    fn ciphertext_len(&self) -> u64 {
        HEADER_LEN + self.plain.len().div_ceil(PAYLOAD_MAX as usize) as u64 * RECORD_SIZE
    }
}

impl ReadBytesAt for OnDemandEncryptor {
    fn read_bytes_at(&self, offset: u64, size: usize) -> std::io::Result<bytes::Bytes> {
        let end = (offset + size as u64).min(self.ciphertext_len());
        let cipher = DemoCipher::new(&self.key.ikm, &self.key.salt);
        let payload_max = PAYLOAD_MAX as usize;
        let n_records = self.plain.len().div_ceil(payload_max).max(1);
        let mut out = Vec::with_capacity(size);
        let mut pos = offset;
        if pos < HEADER_LEN {
            let mut header = Vec::with_capacity(HEADER_LEN as usize);
            header.extend_from_slice(&cipher.salt);
            header.extend_from_slice(&RECORD_SIZE.to_be_bytes()[4..]); // rs: 32-bit BE
            header.push(0); // idlen: no key id
            let take = (HEADER_LEN - pos).min(end - pos) as usize;
            out.extend_from_slice(&header[pos as usize..pos as usize + take]);
            pos += take as u64;
        }
        while pos < end {
            let rel = pos - HEADER_LEN;
            let idx = (rel / RECORD_SIZE) as usize;
            let within = (rel % RECORD_SIZE) as usize;
            let payload_start = idx * payload_max;
            let payload_end = (payload_start + payload_max).min(self.plain.len());
            let chunk = self.plain[payload_start..payload_end].to_vec();
            let record = cipher.encrypt_record(idx as u64, idx == n_records - 1, chunk);
            let take = (record.len() - within).min((end - pos) as usize);
            out.extend_from_slice(&record[within..within + take]);
            pos += take as u64;
        }
        Ok(out.into())
    }
}

/// Maps ciphertext hashes to the (key + plaintext) that materialize them.
/// How an application finds the key for a hash is up to its own key store.
#[derive(Default)]
struct EncryptedProvider {
    entries: HashMap<Hash, (BlobKey, bytes::Bytes)>,
}

impl Provider for EncryptedProvider {
    fn reader_for(&self, hash: &Hash) -> Option<DynVirtualSource> {
        let (key, plain) = self.entries.get(hash)?;
        Some(Arc::new(OnDemandEncryptor {
            key: key.clone(),
            plain: plain.clone(),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // -- The encrypt side (e.g. the device holding the original files). -----
    let (store, virtuals) = MemStore::new_with_virtuals(Default::default());
    let store = Store::from(store);

    let key = BlobKey::generate();
    // A demo plaintext spanning many 1024-byte records.
    let plaintext: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();

    // 1. Encrypt once. The plaintext is stored as a normal local blob; the
    //    ciphertext is not stored at all.
    let ciphertext = encrypt_bytes(&key, &plaintext);
    let p_hash = Hash::new(&plaintext);
    let c_hash = Hash::new(&ciphertext);
    println!(
        "plaintext  {p_hash} ({} bytes, stored locally)",
        plaintext.len()
    );
    println!(
        "ciphertext {c_hash} ({} bytes, never stored)",
        ciphertext.len()
    );

    // 2. Install the ciphertext as a virtual entry: bao outboard + size +
    //    provider name. Demo-scale, so the outboard is computed from the
    //    buffer; for large media, stream the ciphertext through
    //    `store.blobs().build_outboard(..)` instead, which computes the
    //    (hash, outboard) pair incrementally without holding the bytes.
    let outboard = bao_tree::io::outboard::PreOrderMemOutboard::create(&ciphertext, IROH_BLOCK_SIZE);
    store
        .blobs()
        .add_virtual_with_outboard(
            c_hash,
            ciphertext.len() as u64,
            outboard.data.clone(),
            PROVIDER_NAME,
        )
        .await?;
    println!("installed virtual entry for {c_hash}");

    // 3. Register the on-demand provider. Only the key + plaintext stay
    //    local; the store can now serve C to any peer.
    let provider = Arc::new(EncryptedProvider {
        entries: HashMap::from([(c_hash, (key.clone(), plaintext.clone().into()))]),
    });
    virtuals.register(PROVIDER_NAME, provider)?;

    // -- A second node (relay or backup target) fetches the ciphertext. -----
    let (store_b, _virtuals_b) = MemStore::new_with_virtuals(Default::default());
    let store_b = Store::from(store_b);
    let lookup_a = MemoryLookup::new();
    let lookup_b = MemoryLookup::new();
    let endpoint_a = iroh::Endpoint::builder(presets::Minimal)
        .relay_mode(iroh::RelayMode::Disabled)
        .address_lookup(lookup_a.clone())
        .bind()
        .await?;
    let endpoint_b = iroh::Endpoint::builder(presets::Minimal)
        .relay_mode(iroh::RelayMode::Disabled)
        .address_lookup(lookup_b.clone())
        .bind()
        .await?;
    let blobs_a = BlobsProtocol::new(&store, None);
    let router_a = Router::builder(endpoint_a.clone())
        .accept(ALPN, blobs_a)
        .spawn();
    lookup_b.add_endpoint_info(endpoint_a.addr());

    let conn = endpoint_b.connect(endpoint_a.addr(), ALPN).await?;
    // The receiver stores C as a real, bao-verified blob - it can later
    // re-serve or re-upload it without ever seeing P.
    store_b.remote().fetch(conn, c_hash).await?;
    let got_ct = store_b.get_bytes(c_hash).await?;
    ensure!(
        got_ct.as_ref() == ciphertext.as_slice(),
        "receiver must see byte-identical ciphertext"
    );
    println!("fetched + bao-verified {c_hash} over iroh");

    // 4. Decrypt on the peer: header salt + records -> plaintext.
    let roundtripped = decrypt_bytes(&key, &got_ct)?;
    ensure!(roundtripped == plaintext, "decrypted plaintext mismatch");
    println!(
        "decrypted back to the original plaintext ({} bytes)",
        plaintext.len()
    );


    // Negative case: a virtual entry whose provider is not registered serves
    // as not-found to peers, even though the entry itself exists. (Node B's
    // fetched copy of the plaintext above is now stored content on B - it is
    // served like any other stored blob.)
    let orphan = encrypt_bytes(&BlobKey::generate(), b"nobody registered this");
    let orphan_hash = Hash::new(&orphan);
    let orphan_outboard =
        bao_tree::io::outboard::PreOrderMemOutboard::create(&orphan, IROH_BLOCK_SIZE);
    store
        .blobs()
        .add_virtual_with_outboard(
            orphan_hash,
            orphan.len() as u64,
            orphan_outboard.data.clone(),
            PROVIDER_NAME,
        )
        .await?;
    let conn2 = endpoint_b.connect(endpoint_a.addr(), ALPN).await?;
    match store_b.remote().fetch(conn2, orphan_hash).await {
        Ok(_) => bail!("expected fetch to fail without a registered provider"),
        Err(cause) => println!("no provider registered -> fetch fails as expected ({cause:?})"),
    }

    router_a.shutdown().await?;
    endpoint_a.close().await;
    endpoint_b.close().await;
    println!("demo complete");
    Ok(())
}
