// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::hash_map::{DefaultHasher, RandomState};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;

type InnerLockTable<K> = HashMap<K, Arc<tokio::sync::Mutex<()>>>;
// MutexTable supports mutual exclusion on keys such as TransactionDigest or ObjectDigest
pub struct MutexTable<K: Hash> {
    random_state: RandomState,
    lock_table: Arc<Vec<RwLock<InnerLockTable<K>>>>,
    _k: std::marker::PhantomData<K>,
    _cleaner: JoinHandle<()>,
}

#[derive(Debug)]
pub enum TryAcquireLockError {
    LockTableLocked,
    LockEntryLocked,
}

impl fmt::Display for TryAcquireLockError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "operation would block")
    }
}

impl Error for TryAcquireLockError {}
// Opaque struct to hide tokio::sync::MutexGuard.
pub struct LockGuard(tokio::sync::OwnedMutexGuard<()>);

impl<K: Hash + std::cmp::Eq + Send + Sync + 'static> MutexTable<K> {
    pub fn new_with_cleanup_duration(
        num_shards: usize,
        shard_size: usize,
        duration: Duration,
    ) -> Self {
        let lock_table: Arc<Vec<RwLock<InnerLockTable<K>>>> = Arc::new(
            (0..num_shards)
                .into_iter()
                .map(|_| RwLock::new(HashMap::with_capacity(shard_size)))
                .collect(),
        );
        let cloned = lock_table.clone();
        Self {
            random_state: RandomState::new(),
            lock_table,
            _k: std::marker::PhantomData {},
            _cleaner: tokio::spawn(async move {
                loop {
                    Self::cleanup(cloned.clone());
                    tokio::time::sleep(duration).await;
                }
            }),
        }
    }
    pub fn new(num_shards: usize, shard_size: usize) -> Self {
        Self::new_with_cleanup_duration(num_shards, shard_size, Duration::from_secs(60))
    }
    pub fn cleanup(lock_table: Arc<Vec<RwLock<InnerLockTable<K>>>>) {
        for shard in lock_table.iter() {
            let map = shard.try_write();
            if map.is_err() {
                continue;
            }
            map.unwrap().retain(|_k, v| {
                let mutex_guard = v.try_lock();
                mutex_guard.is_err()
            });
        }
    }
    fn get_lock_idx(&self, key: &K) -> usize {
        let mut hasher = if !cfg!(test) {
            self.random_state.build_hasher()
        } else {
            // be deterministic for tests
            DefaultHasher::new()
        };

        key.hash(&mut hasher);
        // unwrap ok - converting u64 -> usize
        let hash: usize = hasher.finish().try_into().unwrap();
        hash % self.lock_table.len()
    }

    pub async fn acquire_locks<I>(&self, object_iter: I) -> Vec<LockGuard>
    where
        I: Iterator<Item = K>,
    {
        let mut objects: Vec<K> = object_iter.into_iter().collect();
        objects.sort_by(|a, b| self.get_lock_idx(a).cmp(&self.get_lock_idx(b)));
        objects.dedup();

        let mut guards = Vec::with_capacity(objects.len());
        for object in objects.into_iter() {
            guards.push(self.acquire_lock(object).await);
        }
        guards
    }

    pub async fn acquire_lock(&self, k: K) -> LockGuard {
        let lock_idx = self.get_lock_idx(&k);
        let map = self.lock_table[lock_idx].read().await;
        if let Some(element) = map.get(&k) {
            LockGuard(element.clone().lock_owned().await)
        } else {
            // element doesn't exist
            drop(map);
            let mut map = self.lock_table[lock_idx].write().await;
            let element = map.entry(k).or_insert_with(|| Arc::new(Mutex::new(())));
            LockGuard(element.clone().lock_owned().await)
        }
    }

    pub fn try_acquire_lock(&self, k: K) -> Result<LockGuard, TryAcquireLockError> {
        let lock_idx = self.get_lock_idx(&k);
        let res = self.lock_table[lock_idx].try_read();
        if res.is_err() {
            return Err(TryAcquireLockError::LockTableLocked);
        }
        let map = res.unwrap();
        if let Some(element) = map.get(&k) {
            let lock = element.clone().try_lock_owned();
            if lock.is_err() {
                return Err(TryAcquireLockError::LockEntryLocked);
            }
            Ok(LockGuard(lock.unwrap()))
        } else {
            // element doesn't exist
            drop(map);
            let res = self.lock_table[lock_idx].try_write();
            if res.is_err() {
                return Err(TryAcquireLockError::LockTableLocked);
            }
            let mut map = res.unwrap();
            let element = map.entry(k).or_insert_with(|| Arc::new(Mutex::new(())));
            let lock = element.clone().try_lock_owned();
            Ok(LockGuard(lock.unwrap()))
        }
    }
}

#[tokio::test]
async fn test_mutex_table() {
    let mutex_table = MutexTable::<String>::new(1, 128);
    let john1 = mutex_table.try_acquire_lock("john".to_string());
    assert!(john1.is_ok());
    let john2 = mutex_table.try_acquire_lock("john".to_string());
    assert!(john2.is_err());
    drop(john1);
    let john2 = mutex_table.try_acquire_lock("john".to_string());
    assert!(john2.is_ok());
    let jane = mutex_table.try_acquire_lock("jane".to_string());
    assert!(jane.is_ok());
    MutexTable::cleanup(mutex_table.lock_table.clone());
    let map = mutex_table.lock_table.get(0).as_ref().unwrap().try_read();
    assert!(map.is_ok());
    assert_eq!(map.unwrap().len(), 2);
    drop(john2);
    MutexTable::cleanup(mutex_table.lock_table.clone());
    let map = mutex_table.lock_table.get(0).as_ref().unwrap().try_read();
    assert!(map.is_ok());
    assert_eq!(map.unwrap().len(), 1);
    drop(jane);
    MutexTable::cleanup(mutex_table.lock_table.clone());
    let map = mutex_table.lock_table.get(0).as_ref().unwrap().try_read();
    assert!(map.is_ok());
    assert!(map.unwrap().is_empty());
}
