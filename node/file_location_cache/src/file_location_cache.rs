use crate::Config;
use network::types::SignedAnnounceFile;
use network::PeerId;
use parking_lot::Mutex;
use priority_queue::PriorityQueue;
use rand::seq::IteratorRandom;
use shared_types::{timestamp_now, TxID};
use std::cmp::Reverse;
use std::collections::HashMap;

/// Caches limited announcements of specified file from different peers.
struct AnnouncementCache {
    /// Maximum number of announcements in cache.
    capacity: usize,

    /// Timeout in seconds to expire the cached announcement.
    /// This is because file may be removed from the announced
    /// storage node.
    timeout_secs: u32,

    /// All cached announcements that mapped from peer id to announcement.
    /// Note, only cache the latest announcement for each peer.
    items: HashMap<PeerId, SignedAnnounceFile>,

    /// All announcements are prioritized by timestamp.
    /// The top element is the oldest announcement.
    priorities: PriorityQueue<PeerId, Reverse<u32>>,
}

impl AnnouncementCache {
    fn new(capacity: usize, timeout_secs: u32) -> Self {
        assert!(capacity > 0);

        AnnouncementCache {
            capacity,
            timeout_secs,
            items: Default::default(),
            priorities: Default::default(),
        }
    }

    /// Returns the priority of the oldest announcement if any.
    fn peek_priority(&self) -> Option<Reverse<u32>> {
        let (_, &Reverse(ts)) = self.priorities.peek()?;
        Some(Reverse(ts))
    }

    /// Removes the oldest announcement if any.
    fn pop(&mut self) -> Option<SignedAnnounceFile> {
        let (peer_id, _) = self.priorities.pop()?;
        self.items.remove(&peer_id)
    }

    fn len(&self) -> usize {
        self.items.len()
    }

    /// Garbage collects expired announcements.
    fn garbage_collect(&mut self) -> usize {
        let mut collected = 0;
        let now = timestamp_now();

        while let Some((_, &Reverse(ts))) = self.priorities.peek() {
            if ts + self.timeout_secs > now {
                break;
            }

            self.pop();
            collected += 1;
        }

        collected
    }

    /// Insert the specified `announcement` into cache.
    fn insert(&mut self, announcement: SignedAnnounceFile) {
        self.garbage_collect();

        let peer_id = announcement.peer_id.clone().into();

        if let Some(existing) = self.items.get(&peer_id) {
            // ignore older announcement
            if announcement.timestamp <= existing.timestamp {
                return;
            }
        }

        // insert or update
        self.priorities
            .push(peer_id, Reverse(announcement.timestamp));
        self.items.insert(peer_id, announcement);

        // remove oldest one if capacity exceeded
        if self.items.len() > self.capacity {
            self.pop();
        }
    }

    /// Randomly pick an announcement if any.
    fn random(&mut self) -> (Option<SignedAnnounceFile>, usize) {
        let collected = self.garbage_collect();

        // all announcements garbage collected
        if self.items.is_empty() {
            return (None, collected);
        }

        let choosed = self
            .items
            .iter()
            .choose(&mut rand::thread_rng())
            .map(|(_, item)| item.clone());

        (choosed, collected)
    }

    /// Returns all announcements.
    fn all(&mut self) -> (Vec<SignedAnnounceFile>, usize) {
        let collected = self.garbage_collect();
        let result = self.items.iter().map(|(_, item)| item.clone()).collect();
        (result, collected)
    }
}

/// Caches announcements for different files.
struct FileCache {
    /// Cache configuration.
    config: Config,

    /// Total number of announcements cached for all files.
    total_announcements: usize,

    /// All cached files that mapped from `tx_id` to `AnnouncementCache`.
    files: HashMap<TxID, AnnouncementCache>,

    /// All files are prioritized by timestamp.
    /// The top element is the `AnnouncementCache` that has the oldest announcement.
    priorities: PriorityQueue<TxID, Reverse<u32>>,
}

impl FileCache {
    fn new(config: Config) -> Self {
        FileCache {
            config,
            total_announcements: 0,
            files: Default::default(),
            priorities: Default::default(),
        }
    }

    /// Insert the specified `announcement` into cache.
    fn insert(&mut self, announcement: SignedAnnounceFile) {
        let tx_id = announcement.tx_id;

        let item = self.files.entry(tx_id).or_insert_with(|| {
            AnnouncementCache::new(
                self.config.max_entries_per_file,
                self.config.entry_expiration_time_secs,
            )
        });

        self.total_announcements
            .checked_sub(item.len())
            .expect("total announcements overflow");

        item.insert(announcement);

        if let Some(priority) = item.peek_priority() {
            self.priorities.push(tx_id, priority);
        }

        self.total_announcements += item.len();
        if self.total_announcements > self.config.max_entries_total {
            self.pop();
        }
    }

    /// Removes the oldest file announcement.
    fn pop(&mut self) -> Option<SignedAnnounceFile> {
        let (&tx_id, _) = self.priorities.peek()?;
        let item = self.files.get_mut(&tx_id)?;

        let result = item.pop();

        if item.len() == 0 {
            self.files.remove(&tx_id);
            self.priorities.remove(&tx_id);
        }

        result
    }

    /// Randomly pick a announcement of specified file by `tx_id`.
    fn random(&mut self, tx_id: TxID) -> Option<SignedAnnounceFile> {
        let item = self.files.get_mut(&tx_id)?;
        let (result, collected) = item.random();
        self.update_after_gc(tx_id, collected);
        result
    }

    fn update_after_gc(&mut self, tx_id: TxID, collected: usize) {
        if collected == 0 {
            return;
        }

        self.total_announcements
            .checked_sub(collected)
            .expect("total announcements overflow");

        let item = match self.files.get_mut(&tx_id) {
            Some(v) => v,
            None => return,
        };

        if item.len() == 0 {
            // remove entry if empty
            self.files.remove(&tx_id);
            self.priorities.remove(&tx_id);
        } else {
            // update priority if garbage collected
            let new_priority = item.peek_priority().expect("should peek if not empty");
            self.priorities.change_priority(&tx_id, new_priority);
        }
    }

    /// Returns all the announcements of specified file by `tx_id`.
    fn all(&mut self, tx_id: TxID) -> Option<Vec<SignedAnnounceFile>> {
        let item = self.files.get_mut(&tx_id)?;
        let (result, collected) = item.all();
        self.update_after_gc(tx_id, collected);
        Some(result)
    }
}

pub struct FileLocationCache {
    cache: Mutex<FileCache>,
}

impl Default for FileLocationCache {
    fn default() -> Self {
        FileLocationCache {
            cache: Mutex::new(FileCache::new(Default::default())),
        }
    }
}

impl FileLocationCache {
    pub fn insert(&self, announcement: SignedAnnounceFile) {
        self.cache.lock().insert(announcement);
    }

    pub fn get_one(&self, tx_id: TxID) -> Option<SignedAnnounceFile> {
        self.cache.lock().random(tx_id)
    }

    pub fn get_all(&self, tx_id: TxID) -> Vec<SignedAnnounceFile> {
        self.cache.lock().all(tx_id).unwrap_or_default()
    }
}
