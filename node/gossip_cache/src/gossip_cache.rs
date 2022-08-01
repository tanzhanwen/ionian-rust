use crate::Config;
use hashlink::{LinkedHashMap, LinkedHashSet};
use network::types::SignedAnnounceFile;
use parking_lot::Mutex;
use rand::seq::IteratorRandom;
use std::ops::{Add, DerefMut};
use std::time::Instant;

struct CachedFiles {
    expired_at: Instant,
    /// TODO(ionian-dev): using `LinkedHashSet` as an entry is probably an overkill
    files: LinkedHashSet<SignedAnnounceFile>,
}

#[derive(Default)]
pub struct GossipCache {
    config: Config,
    cache: Mutex<LinkedHashMap<u64, CachedFiles>>,
}

impl GossipCache {
    fn garbage_collect(&self, cache: &mut LinkedHashMap<u64, CachedFiles>) {
        let now = Instant::now();

        while let Some((_, files)) = cache.front() {
            if files.expired_at > now {
                return;
            }

            if let Some((tx_seq, _)) = cache.pop_front() {
                debug!(%tx_seq, "Garbage collected for file");
            }
        }
    }

    fn expiration_time(&self) -> Instant {
        Instant::now().add(self.config.entry_expiration_time)
    }

    pub fn insert(&self, announcement: SignedAnnounceFile) {
        let mut cache = self.cache.lock();

        self.garbage_collect(cache.deref_mut());

        let entry = cache
            .entry(announcement.tx_seq)
            .or_insert_with(|| CachedFiles {
                expired_at: self.expiration_time(),
                files: Default::default(),
            });

        entry.files.insert(announcement);

        if entry.files.len() > self.config.max_entries_per_file {
            entry.files.pop_front();
        }

        if cache.len() > self.config.max_files {
            cache.pop_front();
        }
    }

    pub fn get_one(&self, tx_seq: u64) -> Option<SignedAnnounceFile> {
        let mut cache = self.cache.lock();

        self.garbage_collect(cache.deref_mut());

        let cached = cache.to_back(&tx_seq)?;
        cached.expired_at = self.expiration_time();
        cached.files.iter().choose(&mut rand::thread_rng()).cloned()
    }

    pub fn get_all(&self, tx_seq: u64) -> Vec<SignedAnnounceFile> {
        let mut cache = self.cache.lock();

        self.garbage_collect(cache.deref_mut());

        let cached = match cache.to_back(&tx_seq) {
            Some(entries) => entries,
            None => return vec![],
        };

        cached.expired_at = self.expiration_time();
        cached.files.iter().cloned().collect()
    }
}
