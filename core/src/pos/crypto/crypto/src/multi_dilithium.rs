// Copyright 2021 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use crate::{
    dilithium::{
        DilithiumPrivateKey, DilithiumPublicKey, DilithiumSignature,
    },
    hash::{CryptoHash, CryptoHasher},
    traits::*,
    CryptoMaterialError, PrivateKey, PublicKey, Signature, SigningKey, Uniform,
    ValidCryptoMaterial, ValidCryptoMaterialStringExt, VerifyingKey,
};
use anyhow::{anyhow, Result};
pub use bls_signatures::{
    aggregate, hash as bls_hash, PrivateKey as RawPrivateKey,
    PublicKey as RawPublicKey, Serialize as DilithiumSerialize,
    Signature as RawSignature,
};
use core::convert::TryFrom;
use std::convert::TryInto;
use diem_crypto_derive::{
    DeserializeKey, SerializeKey, SilentDebug, SilentDisplay,
};
use mirai_annotations::*;
use rand::Rng;
use serde::Serialize;
use std::fmt;
use digest::Digest;
use rdil::sign::Message;
use rdil::utils::config::{CRYPTO_BYTES, CRYPTO_PUBLICKEYBYTES};
use sha3::Sha3_256;
use crate::multi_bls::MultiBLSPrivateKey;

const MAX_NUM_OF_KEYS: usize = 300;

#[cfg(feature = "assert-private-keys-not-cloneable")]
static_assertions::assert_not_impl_any!(MultiDilithiumPrivateKey: Clone);

/// Dummy multi private key. Not used.
#[derive(DeserializeKey, Eq, PartialEq, SerializeKey)]
pub struct MultiDilithiumPrivateKey {
    private_key: DilithiumPrivateKey,
}

/// Vector of public keys in the multi-key Dilithium structure.
#[derive(Clone, DeserializeKey, Eq, PartialEq, SerializeKey)]
pub struct MultiDilithiumPublicKey {
    public_keys: Vec<DilithiumPublicKey>,
}

#[cfg(mirai)]
use crate::tags::ValidatedPublicKeyTag;
#[cfg(not(mirai))]
struct ValidatedPublicKeyTag {}

/// Multi Dilithium signature wrapper
#[derive(DeserializeKey, Clone, SerializeKey, PartialEq)]
pub struct MultiDilithiumSignature {
    signatures: Vec<(DilithiumSignature, u32)>,
}

impl PrivateKey for MultiDilithiumPrivateKey {
    type PublicKeyMaterial = MultiDilithiumPublicKey;
}

impl SigningKey for MultiDilithiumPrivateKey {
    type SignatureMaterial = MultiDilithiumSignature;
    type VerifyingKeyMaterial = MultiDilithiumPublicKey;

    fn sign<T: CryptoHash + Serialize>(&self, _message: &T) -> Self::SignatureMaterial {
        unreachable!()
    }

    #[cfg(any(test, feature = "fuzzing"))]
    fn sign_arbitrary_message(&self, _message: &[u8]) -> Self::SignatureMaterial {
        unreachable!()
    }
}

impl Length for MultiDilithiumPrivateKey {
    fn length(&self) -> usize {
        unreachable!()
    }
}

impl ValidCryptoMaterial for MultiDilithiumPrivateKey {
    fn to_bytes(&self) -> Vec<u8> {
        unreachable!()
    }
}

impl Genesis for MultiDilithiumPrivateKey {
    fn genesis() -> Self {
        unreachable!()
    }
}

impl TryFrom<&[u8]> for MultiDilithiumPrivateKey {
    type Error = CryptoMaterialError;

    fn try_from(_value: &[u8]) -> std::result::Result<Self, Self::Error> {
        unreachable!()
    }
}

impl MultiDilithiumPublicKey {
    /// Construct a new MultiDilithiumPublicKey.
    pub fn new(public_keys: Vec<DilithiumPublicKey>) -> Self {
        MultiDilithiumPublicKey { public_keys }
    }

    /// Getter public_keys
    pub fn public_keys(&self) -> &Vec<DilithiumPublicKey> { &self.public_keys }

    /// Serialize a MultiDilithiumPublicKey.
    pub fn to_bytes(&self) -> Vec<u8> { to_bytes(&self.public_keys) }
}

impl From<&MultiDilithiumPrivateKey> for MultiDilithiumPublicKey {
    fn from(private_key: &MultiDilithiumPrivateKey) -> Self {
        let public_keys = vec![private_key.private_key.public_key()];
        MultiDilithiumPublicKey { public_keys }
    }
}

//////////////////////
// PublicKey Traits //
//////////////////////

/// Convenient method to create a MultiDilithiumPublicKey from a single
/// DilithiumPublicKey.
impl From<DilithiumPublicKey> for MultiDilithiumPublicKey {
    fn from(ed_public_key: DilithiumPublicKey) -> Self {
        MultiDilithiumPublicKey {
            public_keys: vec![ed_public_key],
        }
    }
}

/// We deduce PublicKey from this.
impl PublicKey for MultiDilithiumPublicKey {
    type PrivateKeyMaterial = MultiDilithiumPrivateKey;
}

#[allow(clippy::derive_hash_xor_eq)]
impl std::hash::Hash for MultiDilithiumPublicKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let encoded_pubkey = self.to_bytes();
        state.write(&encoded_pubkey);
    }
}

impl TryFrom<&[u8]> for MultiDilithiumPublicKey {
    type Error = CryptoMaterialError;

    /// Deserialize a MultiDilithiumPublicKey. This method will also check for key
    /// and threshold validity, and will only deserialize keys that are safe
    /// against small subgroup attacks.
    fn try_from(
        bytes: &[u8],
    ) -> std::result::Result<MultiDilithiumPublicKey, CryptoMaterialError> {
        if bytes.is_empty() {
            return Err(CryptoMaterialError::WrongLengthError);
        }
        let public_keys: Result<Vec<DilithiumPublicKey>, _> = bytes
            .chunks_exact(CRYPTO_PUBLICKEYBYTES)
            .map(DilithiumPublicKey::try_from)
            .collect();
        public_keys.map(|public_keys| {
            let public_key = MultiDilithiumPublicKey { public_keys };
            add_tag!(&public_key, ValidatedPublicKeyTag);
            public_key
        })
    }
}

/// We deduce VerifyingKey from pointing to the signature material
/// we get the ability to do `pubkey.validate(msg, signature)`
impl VerifyingKey for MultiDilithiumPublicKey {
    type SignatureMaterial = MultiDilithiumSignature;
    type SigningKeyMaterial = MultiDilithiumPrivateKey;
}

impl fmt::Display for MultiDilithiumPublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.to_bytes()))
    }
}

impl fmt::Debug for MultiDilithiumPublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MultiDilithiumPublicKey({})", self)
    }
}

impl Length for MultiDilithiumPublicKey {
    fn length(&self) -> usize { self.public_keys.len() * CRYPTO_PUBLICKEYBYTES }
}

impl ValidCryptoMaterial for MultiDilithiumPublicKey {
    fn to_bytes(&self) -> Vec<u8> { self.to_bytes() }
}

impl MultiDilithiumSignature {
    /// This method will also sort signatures based on index.
    pub fn new(
        signatures: Vec<(DilithiumSignature, usize)>,
    ) -> std::result::Result<Self, CryptoMaterialError> {
        Ok(Self {signatures: signatures.into_iter().map(|e| (e.0, e.1 as u32)).collect()})
    }

    /// to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        self.signatures.iter()
            .flat_map(|(sig, index)| [ValidCryptoMaterial::to_bytes(sig), index.to_be_bytes().to_vec()].concat())
            .collect()
    }
}

//////////////////////
// Signature Traits //
//////////////////////

impl Eq for MultiDilithiumSignature {}

impl TryFrom<&[u8]> for MultiDilithiumSignature {
    type Error = CryptoMaterialError;

    /// Deserialize a MultiDilithiumSignature. This method will also check for
    /// malleable signatures and bitmap validity.
    fn try_from(
        bytes: &[u8],
    ) -> std::result::Result<MultiDilithiumSignature, CryptoMaterialError> {
        let signatures: Result<Vec<(DilithiumSignature, u32)>, _> = bytes
            .chunks_exact(CRYPTO_BYTES + 256 + 4)
            .map(|data| {
                DilithiumSignature::try_from(&data[..CRYPTO_BYTES + 256]).map(|sig| (sig, u32::from_be_bytes(data[CRYPTO_BYTES + 256..].try_into().unwrap())))
            })
            .collect();
        signatures.map(|signatures| {
            let sig  = MultiDilithiumSignature{ signatures };
            add_tag!(&sig, ValidatedPublicKeyTag);
            sig
        })
    }
}

#[allow(clippy::derive_hash_xor_eq)]
impl std::hash::Hash for MultiDilithiumSignature {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let encoded_signature = self.to_bytes();
        state.write(&encoded_signature);
    }
}

impl fmt::Display for MultiDilithiumSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.to_bytes()[..]))
    }
}

impl fmt::Debug for MultiDilithiumSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MultiDilithiumSignature({})", self)
    }
}

impl ValidCryptoMaterial for MultiDilithiumSignature {
    fn to_bytes(&self) -> Vec<u8> { self.to_bytes() }
}

impl Signature for MultiDilithiumSignature {
    type SigningKeyMaterial = MultiDilithiumPrivateKey;
    type VerifyingKeyMaterial = MultiDilithiumPublicKey;

    fn verify<T: CryptoHash + Serialize>(
        &self, message: &T, public_key: &MultiDilithiumPublicKey,
    ) -> Result<()> {
        // Public keys should be validated to be safe against small subgroup
        // attacks, etc.
        precondition!(has_tag!(public_key, ValidatedPublicKeyTag));
        let mut bytes = <T as CryptoHash>::Hasher::seed().to_vec();
        bcs::serialize_into(&mut bytes, &message)
            .map_err(|_| CryptoMaterialError::SerializationError)?;
        let data = Sha3_256::digest(&bytes).to_vec();
        Self::verify_arbitrary_msg(self, &data, public_key)
    }

    /// Checks that `self` is valid for an arbitrary &[u8] `message` using
    /// `public_key`. Outside of this crate, this particular function should
    /// only be used for native signature verification in Move.
    fn verify_arbitrary_msg(
        &self, message: &[u8], public_key: &MultiDilithiumPublicKey,
    ) -> Result<()> {
        precondition!(has_tag!(public_key, ValidatedPublicKeyTag));
        for (sig, index) in &self.signatures {
            let index = *index as usize;
            if index >= public_key.public_keys.len() {
                return Err(anyhow!(
                    "{}",
                    CryptoMaterialError::BitVecError(
                        "Signature index is out of range".to_string()
                    )
                ))
            }
            sig.verify_arbitrary_msg(&message, &public_key.public_keys[index])?;
        }
        Ok(())
    }
}

impl From<DilithiumSignature> for MultiDilithiumSignature {
    fn from(signature: DilithiumSignature) -> Self {
        MultiDilithiumSignature {
            signatures: vec![(signature, 0)],
        }
    }
}

//////////////////////
// Helper functions //
//////////////////////

// Helper function required to MultiDilithium keys to_bytes to add the threshold.
fn to_bytes<T: ValidCryptoMaterial>(keys: &[T]) -> Vec<u8> {
    let bytes: Vec<u8> = keys
        .iter()
        .flat_map(ValidCryptoMaterial::to_bytes)
        .collect();
    bytes
}
