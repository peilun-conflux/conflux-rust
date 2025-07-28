// Copyright 2021 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use crate::{
    hash::{CryptoHash, CryptoHasher},
    traits::*,
    CryptoMaterialError, PrivateKey, PublicKey, Signature, SigningKey, Uniform,
    ValidCryptoMaterial, ValidCryptoMaterialStringExt, VerifyingKey,
};
use anyhow::{anyhow, Result};
use rdil::sign::{SecretKey as RawPrivateKey, PublicKey as RawPublicKey, SignedMessage as RawSignature, Message as RawMessage, sign as rdil_sign, Message, verify_sign as rdil_verify_sign};
use diem_crypto_derive::{
    DeserializeKey, SerializeKey, SilentDebug, SilentDisplay,
};
use diem_logger::prelude::*;
use mirai_annotations::*;
use serde::{Deserialize, Deserializer, Serialize};
use std::convert::TryFrom;

#[cfg(mirai)]
use crate::tags::ValidatedPublicKeyTag;
use std::fmt::{self, Formatter};

#[cfg(not(mirai))]
struct ValidatedPublicKeyTag {}

/// Dilithium signature private key
#[derive(DeserializeKey, SerializeKey, SilentDebug, SilentDisplay)]
pub struct DilithiumPrivateKey(RawPrivateKey);

#[cfg(feature = "assert-private-keys-not-cloneable")]
static_assertions::assert_not_impl_any!(DilithiumPrivateKey: Clone);

#[cfg(any(test, feature = "cloneable-private-keys"))]
impl Clone for DilithiumPrivateKey {
    fn clone(&self) -> Self {
        DilithiumPrivateKey(self.0.clone())
    }
}

/// Dilithium signature public key
#[derive(DeserializeKey, Clone, SerializeKey)]
pub struct DilithiumPublicKey(RawPublicKey);

// TODO(lpl): Signature aggregation.
/// Dilithium signature wrapper
#[derive(DeserializeKey, Clone, SerializeKey)]
pub struct DilithiumSignature(RawSignature);

impl DilithiumPrivateKey {
    ///
    pub fn raw_key(self) -> RawPrivateKey { self.0 }
}

impl PartialEq<Self> for DilithiumPrivateKey {
    fn eq(&self, other: &Self) -> bool { self.to_bytes() == other.to_bytes() }
}

impl Eq for DilithiumPrivateKey {}

impl SigningKey for DilithiumPrivateKey {
    type SignatureMaterial = DilithiumSignature;
    type VerifyingKeyMaterial = DilithiumPublicKey;

    fn sign<T: CryptoHash + Serialize>(
        &self, message: &T,
    ) -> Self::SignatureMaterial {
        let mut bytes = <T::Hasher as CryptoHasher>::seed().to_vec();
        bcs::serialize_into(&mut bytes, &message)
            .map_err(|_| CryptoMaterialError::SerializationError)
            .expect("Serialization of signable material should not fail.");
        DilithiumSignature(rdil_sign(&Message{data: bytes}, &self.0).expect("Signing failed"))
    }

    #[cfg(any(test, feature = "fuzzing"))]
    fn sign_arbitrary_message(
        &self, message: &[u8],
    ) -> Self::SignatureMaterial {
        DilithiumSignature(rdil_sign(&Message{data: message.to_vec()}, &self.0).expect("Signing failed"))
    }
}

impl From<RawPublicKey> for DilithiumPublicKey {
    fn from(raw: RawPublicKey) -> Self { DilithiumPublicKey(raw) }
}

impl VerifyingKey for DilithiumPublicKey {
    type SignatureMaterial = DilithiumSignature;
    type SigningKeyMaterial = DilithiumPrivateKey;
}

impl PrivateKey for DilithiumPrivateKey {
    type PublicKeyMaterial = DilithiumPublicKey;
}

impl DilithiumPublicKey {
    /// return raw public key
    pub fn raw(self) -> RawPublicKey { self.0 }
}

impl DilithiumSignature {
    /// return an all-zero signature (for test only)
    #[cfg(any(test, feature = "fuzzing"))]
    pub fn dummy_signature() -> Self {
        let bytes = [0u8; CRYPTO_BYTES];
        Self::try_from(&bytes[..]).unwrap()
    }

    /// return raw signature
    pub fn raw(self) -> RawSignature { self.0 }
}

impl Signature for DilithiumSignature {
    type SigningKeyMaterial = DilithiumPrivateKey;
    type VerifyingKeyMaterial = DilithiumPublicKey;

    fn verify<T: CryptoHash + Serialize>(
        &self, message: &T, public_key: &Self::VerifyingKeyMaterial,
    ) -> Result<()> {
        let mut bytes = <T::Hasher as CryptoHasher>::seed().to_vec();
        bcs::serialize_into(&mut bytes, &message)
            .map_err(|_| CryptoMaterialError::SerializationError)?;
        self.verify_arbitrary_msg(&bytes, public_key)
    }

    fn verify_arbitrary_msg(
        &self, message: &[u8], public_key: &Self::VerifyingKeyMaterial,
    ) -> Result<()> {
        precondition!(has_tag!(public_key, ValidatedPublicKeyTag));
        match rdil_verify_sign(
            &Message{data: message.to_vec()},
            &self.0,
            &public_key.0,
        ) {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow!("Invalid Dilithium signature: e={:?}", e)),
        }
    }
}

impl PublicKey for DilithiumPublicKey {
    type PrivateKeyMaterial = DilithiumPrivateKey;
}

impl From<&DilithiumPrivateKey> for DilithiumPublicKey {
    fn from(private_key: &DilithiumPrivateKey) -> Self {
        todo!()
    }
}

impl From<&RawPrivateKey> for DilithiumPrivateKey {
    fn from(raw_private_key: &RawPrivateKey) -> Self {
        DilithiumPrivateKey(raw_private_key.clone())
    }
}

impl From<RawPrivateKey> for DilithiumPrivateKey {
    fn from(raw_private_key: RawPrivateKey) -> Self {
        DilithiumPrivateKey(raw_private_key)
    }
}

impl From<&RawSignature> for DilithiumSignature {
    fn from(raw_signature: &RawSignature) -> Self {
        DilithiumSignature(raw_signature.clone())
    }
}

impl From<RawSignature> for DilithiumSignature {
    fn from(raw_signature: RawSignature) -> Self { DilithiumSignature(raw_signature) }
}

impl TryFrom<&[u8]> for DilithiumPrivateKey {
    type Error = CryptoMaterialError;

    /// Deserialize an DilithiumPrivateKey. This method will also check for key
    /// validity.
    fn try_from(
        bytes: &[u8],
    ) -> std::result::Result<DilithiumPrivateKey, CryptoMaterialError> {
        match RawPrivateKey::from_bytes(bytes) {
            Ok(sig) => Ok(Self(sig)),
            Err(_) => Err(CryptoMaterialError::DeserializationError),
        }
    }
}

impl TryFrom<&[u8]> for DilithiumPublicKey {
    type Error = CryptoMaterialError;

    /// Deserialize an DilithiumPrivateKey. This method will also check for key
    /// validity.
    fn try_from(
        bytes: &[u8],
    ) -> std::result::Result<DilithiumPublicKey, CryptoMaterialError> {
        match RawPublicKey::from_bytes(bytes) {
            Ok(sig) => Ok(Self(sig)),
            Err(e) => {
                diem_debug!(
                    "DilithiumPublicKey debug error: bytes={:?}, err={:?}",
                    bytes,
                    e
                );
                Err(CryptoMaterialError::DeserializationError)
            }
        }
    }
}

impl TryFrom<&[u8]> for DilithiumSignature {
    type Error = CryptoMaterialError;

    /// Deserialize an DilithiumPrivateKey. This method will also check for key
    /// validity.
    fn try_from(
        bytes: &[u8],
    ) -> std::result::Result<DilithiumSignature, CryptoMaterialError> {
        // TODO(lpl): Check malleability?
        match RawSignature::from_bytes(bytes) {
            Ok(sig) => Ok(Self(sig)),
            Err(_) => Err(CryptoMaterialError::DeserializationError),
        }
    }
}

impl std::hash::Hash for DilithiumPublicKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let encoded_pubkey = self.to_bytes();
        state.write(&encoded_pubkey);
    }
}

impl PartialEq for DilithiumPublicKey {
    fn eq(&self, other: &DilithiumPublicKey) -> bool {
        self.to_bytes() == other.to_bytes()
    }
}

impl std::hash::Hash for DilithiumSignature {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let encoded_pubkey = ValidCryptoMaterial::to_bytes(self);
        state.write(&encoded_pubkey);
    }
}

impl PartialEq for DilithiumSignature {
    fn eq(&self, other: &DilithiumSignature) -> bool {
        self.to_bytes() == other.to_bytes()
    }
}

impl Eq for DilithiumPublicKey {}

impl Eq for DilithiumSignature {}

impl ValidCryptoMaterial for DilithiumPrivateKey {
    fn to_bytes(&self) -> Vec<u8> { self.0.as_bytes() }
}

impl Genesis for DilithiumPrivateKey {
    fn genesis() -> Self {
        let mut buf = [0u8; CRYPTO_SECRETKEYBYTES];
        buf[CRYPTO_SECRETKEYBYTES - 1] = 1;
        Self::try_from(buf.as_ref()).unwrap()
    }
}

impl ValidCryptoMaterial for DilithiumPublicKey {
    fn to_bytes(&self) -> Vec<u8> { self.0.as_bytes() }
}

impl ValidCryptoMaterial for DilithiumSignature {
    fn to_bytes(&self) -> Vec<u8> { self.0.as_bytes() }
}

impl fmt::Display for DilithiumPublicKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_encoded_string().map_err(|_| fmt::Error)?)
    }
}

impl Uniform for DilithiumPrivateKey {
    fn generate<R>(rng: &mut R) -> Self
    where R: ::rand::RngCore + ::rand::CryptoRng {
        DilithiumPrivateKey(RawPrivateKey::generate(rng))
    }
}

impl fmt::Debug for DilithiumPublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DilithiumPublicKey({})", self)
    }
}

impl fmt::Display for DilithiumSignature {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_encoded_string().map_err(|_| fmt::Error)?)
    }
}

impl fmt::Debug for DilithiumSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DilithiumSignature({})", self)
    }
}

/// Used to deserialize keys in local storage whose validity has been checked
/// before.
#[derive(SerializeKey, DeserializeKey)]
pub struct DilithiumPublicKeyUnchecked(RawPublicKey);
/// Used to deserialize keys in local storage whose validity has been checked
/// before.
#[derive(SerializeKey, DeserializeKey)]
pub struct DilithiumSignatureUnchecked(RawSignature);

impl TryFrom<&[u8]> for DilithiumPublicKeyUnchecked {
    type Error = CryptoMaterialError;

    /// Deserialize an DilithiumPrivateKey. This method will also check for key
    /// validity.
    fn try_from(
        bytes: &[u8],
    ) -> std::result::Result<DilithiumPublicKeyUnchecked, CryptoMaterialError> {
        match RawPublicKey::from_bytes_unchecked(bytes) {
            Ok(sig) => Ok(Self(sig)),
            Err(e) => {
                diem_debug!(
                    "DilithiumPublicKey debug error: bytes={:?}, err={:?}",
                    bytes,
                    e
                );
                Err(CryptoMaterialError::DeserializationError)
            }
        }
    }
}

impl TryFrom<&[u8]> for DilithiumSignatureUnchecked {
    type Error = CryptoMaterialError;

    /// Deserialize an DilithiumPrivateKey. This method will also check for key
    /// validity.
    fn try_from(
        bytes: &[u8],
    ) -> std::result::Result<DilithiumSignatureUnchecked, CryptoMaterialError> {
        // TODO(lpl): Check malleability?
        match RawSignature::from_bytes_unchecked(bytes) {
            Ok(sig) => Ok(Self(sig)),
            Err(_) => Err(CryptoMaterialError::DeserializationError),
        }
    }
}

impl ValidCryptoMaterial for DilithiumPublicKeyUnchecked {
    fn to_bytes(&self) -> Vec<u8> { self.0.as_bytes() }
}

impl ValidCryptoMaterial for DilithiumSignatureUnchecked {
    fn to_bytes(&self) -> Vec<u8> { self.0.as_bytes() }
}

impl From<DilithiumPublicKeyUnchecked> for DilithiumPublicKey {
    fn from(unchecked: DilithiumPublicKeyUnchecked) -> Self { Self(unchecked.0) }
}

impl From<DilithiumSignatureUnchecked> for DilithiumSignature {
    fn from(unchecked: DilithiumSignatureUnchecked) -> Self { Self(unchecked.0) }
}

/// Deserialize public key from local storage.
pub fn deserialize_dilithium_public_key_unchecked<'de, D>(
    deserializer: D,
) -> Result<DilithiumPublicKey, D::Error>
where D: Deserializer<'de> {
    DilithiumPublicKeyUnchecked::deserialize(deserializer).map(Into::into)
}

#[cfg(any(test, feature = "fuzzing"))]
use crate::test_utils::{self, KeyPair};

/// Produces a uniformly random dilithium keypair from a seed
#[cfg(any(test, feature = "fuzzing"))]
pub fn keypair_strategy(
) -> impl Strategy<Value = KeyPair<DilithiumPrivateKey, DilithiumPublicKey>> {
    test_utils::uniform_keypair_strategy::<DilithiumPrivateKey, DilithiumPublicKey>()
}

#[cfg(any(test, feature = "fuzzing"))]
use proptest::prelude::*;
use rdil::utils::config::{CRYPTO_BYTES, CRYPTO_SECRETKEYBYTES};

#[cfg(any(test, feature = "fuzzing"))]
impl proptest::arbitrary::Arbitrary for DilithiumPublicKey {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        crate::test_utils::uniform_keypair_strategy::<
            DilithiumPrivateKey,
            DilithiumPublicKey,
        >()
        .prop_map(|v| v.public_key)
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use crate as diem_crypto;
    use crate::{
        dilithium::{DilithiumPrivateKey, DilithiumSignature},
        SigningKey, Uniform, ValidCryptoMaterial,
    };
    use diem_crypto_derive::{BCSCryptoHash, CryptoHasher};
    use serde::{Deserialize, Serialize};
    use std::{convert::TryFrom, time::Instant};

    #[derive(Debug, CryptoHasher, BCSCryptoHash, Serialize, Deserialize)]
    pub struct TestDiemCrypto(pub String);
    #[test]
    fn test_dilithium_sig_decode() {
        let sk = DilithiumPrivateKey::generate(&mut rand::thread_rng());
        let sig = sk.sign(&TestDiemCrypto("".to_string()));
        let sig_bytes = sig.to_bytes();
        let start = Instant::now();
        let _decoded = DilithiumSignature::try_from(sig_bytes.as_slice()).unwrap();
        println!("Time elapsed: {} us", start.elapsed().as_micros());
    }
}
