use super::*;

#[cfg(test)]
std::thread_local! {
    pub(super) static DFS_METADATA_TABLE_LOCKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(super) fn record_metadata_table_lock() {
    #[cfg(test)]
    DFS_METADATA_TABLE_LOCKS.with(|count| count.set(count.get() + 1));
}

/// Shared metadata utility; guards never cross RPCs or inode state waits.
pub(super) struct ShardedMap<K, V> {
    shards: [RwLock<HashMap<K, V>>; DFS_HANDLE_SHARDS],
}

impl<K: Eq + Hash, V> Default for ShardedMap<K, V> {
    fn default() -> Self {
        Self::from_entries(std::iter::empty())
    }
}

impl<K: Eq + Hash, V> ShardedMap<K, V> {
    fn shard(key: &K) -> usize {
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hash);
        hash.finish() as usize % DFS_HANDLE_SHARDS
    }

    pub(super) fn from_entries(entries: impl IntoIterator<Item = (K, V)>) -> Self {
        let mut maps = std::array::from_fn::<_, DFS_HANDLE_SHARDS, _>(|_| HashMap::new());
        for (key, value) in entries {
            maps[Self::shard(&key)].insert(key, value);
        }
        Self {
            shards: maps.map(RwLock::new),
        }
    }

    pub(super) fn read(
        &self,
        key: &K,
    ) -> std::sync::LockResult<std::sync::RwLockReadGuard<'_, HashMap<K, V>>> {
        record_metadata_table_lock();
        self.shards[Self::shard(key)].read()
    }

    pub(super) fn write(
        &self,
        key: &K,
    ) -> std::sync::LockResult<std::sync::RwLockWriteGuard<'_, HashMap<K, V>>> {
        record_metadata_table_lock();
        self.shards[Self::shard(key)].write()
    }

    pub(super) fn snapshot(&self) -> Result<Vec<(K, V)>>
    where
        K: Clone,
        V: Clone,
    {
        let mut entries = Vec::new();
        for shard in &self.shards {
            record_metadata_table_lock();
            let shard = shard
                .read()
                .map_err(|_| unavailable("DFS metadata shard is poisoned"))?;
            entries.extend(
                shard
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone())),
            );
        }
        Ok(entries)
    }

    pub(super) fn len(&self) -> Result<usize> {
        let mut count = 0;
        for shard in &self.shards {
            count += shard
                .read()
                .map_err(|_| unavailable("DFS metadata shard is poisoned"))?
                .len();
        }
        Ok(count)
    }

    pub(super) fn insert(&self, key: K, value: V) -> Result<Option<V>> {
        Ok(self
            .write(&key)
            .map_err(|_| unavailable("DFS metadata shard is poisoned"))?
            .insert(key, value))
    }

    pub(super) fn values_snapshot(&self) -> Result<Vec<V>>
    where
        K: Clone,
        V: Clone,
    {
        Ok(self
            .snapshot()?
            .into_iter()
            .map(|(_, value)| value)
            .collect())
    }

    pub(super) fn keys_snapshot(&self) -> Result<Vec<K>>
    where
        K: Clone,
        V: Clone,
    {
        Ok(self.snapshot()?.into_iter().map(|(key, _)| key).collect())
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.len().unwrap() == 0
    }
}
