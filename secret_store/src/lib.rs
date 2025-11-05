// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

extern crate cfxkey as keylib;
extern crate parking_lot;
extern crate rust_dilithium2 as rdil;

use keylib::KeyPair;
use malloc_size_of_derive::MallocSizeOf as DeriveMallocSizeOf;
use parking_lot::RwLock;
use std::{collections::HashMap, sync::Arc};
use malloc_size_of::{MallocSizeOf, MallocSizeOfOps};
use rustc_hex::ToHex;

pub struct StoreInner {
    account_vec: Vec<KeyPair>,
    post_quantom_account_vec: Vec<(rdil::sign::SecretKey, rdil::sign::PublicKey)>,
    secret_map: HashMap<String, usize>,
}

impl StoreInner {
    pub fn new() -> Self {
        StoreInner {
            account_vec: Vec::new(),
            post_quantom_account_vec: Vec::new(),
            secret_map: HashMap::new(),
        }
    }

    pub fn insert(&mut self, kp: KeyPair) -> bool {
        let secret_string = kp.secret().to_hex();
        if self.secret_map.contains_key(&secret_string) {
            return false;
        }

        let index = self.count();
        self.secret_map.insert(secret_string, index);
        self.account_vec.push(kp);
        true
    }

    pub fn insert_post_quantum(&mut self, sk: rdil::sign::SecretKey, pk: rdil::sign::PublicKey) -> bool {
        let secret_string = sk.as_bytes().to_hex();
        if self.secret_map.contains_key(&secret_string) {
            return false;
        }

        let index = self.count();
        self.secret_map.insert(secret_string, index);
        self.post_quantom_account_vec.push((sk, pk));
        true
    }

    pub fn count(&self) -> usize { self.account_vec.len() }

    pub fn post_quantom_count(&self) -> usize { self.post_quantom_account_vec.len() }

    pub fn get_keypair(&self, index: usize) -> KeyPair {
        self.account_vec[index].clone()
    }

    pub fn get_post_quantom_key(&self, index: usize) -> (rdil::sign::SecretKey, rdil::sign::PublicKey) {self.post_quantom_account_vec[index].clone()}

    pub fn remove_keypair(&mut self, index: usize) {
        let secret_string = self.account_vec[index].secret().to_hex();
        self.secret_map.remove(&secret_string);
        self.account_vec.remove(index);
    }
}

pub struct SecretStore {
    store: RwLock<StoreInner>,
}

pub type SharedSecretStore = Arc<SecretStore>;

impl SecretStore {
    pub fn new() -> Self {
        SecretStore {
            store: RwLock::new(StoreInner::new()),
        }
    }

    pub fn insert(&self, kp: KeyPair) -> bool { self.store.write().insert(kp) }

    pub fn insert_post_quantum(&self, sk: rdil::sign::SecretKey, pk: rdil::sign::PublicKey) -> bool {self.store.write().insert_post_quantum(sk, pk)}

    pub fn count(&self) -> usize { self.store.read().count() }

    pub fn post_quantom_count(&self) -> usize { self.store.read().post_quantom_count() }

    pub fn get_keypair(&self, index: usize) -> KeyPair {
        self.store.read().get_keypair(index)
    }

    pub fn get_post_quantom_key(&self, index: usize) -> (rdil::sign::SecretKey, rdil::sign::PublicKey) {self.store.read().get_post_quantom_key(index)}

    pub fn remove_keypair(&self, index: usize) {
        self.store.write().remove_keypair(index);
    }
}

// Work around the missing MallocSizeOf of rdil.
impl MallocSizeOf for SecretStore {
    fn size_of(&self, _ops: &mut MallocSizeOfOps) -> usize {
        0
    }
}