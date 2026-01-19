use rmpv::Value as MpValue;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::utils::extract_index;

#[derive(Debug, Clone)]
pub struct Event {
    pub seq: u64,
    pub ts: f64,
    pub store: String,
    pub topic: String,
    pub payload_json: Arc<JsonValue>,
    pub index_json: Arc<JsonValue>,
    pub payload_mp: Arc<MpValue>,
    pub index_mp: Arc<MpValue>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TopicMeta {
    pub created_at: f64,
    pub last_ts: f64,
    pub count_total: u64,
}

#[derive(Debug)]
pub struct Store {
    pub maxlen: usize,
    #[allow(dead_code)]
    pub topic_max: usize,
    pub next_seq: AtomicU64,
    pub topics: DashMap<String, Arc<RwLock<VecDeque<Event>>>>,
    pub meta: DashMap<String, TopicMeta>,
    // Read cache: lock-free recent events for fast get_recent
    pub read_cache: DashMap<String, Vec<Event>>,
}

impl Store {
    pub fn new(maxlen: usize, topic_max: usize) -> Self {
        Self {
            maxlen,
            topic_max,
            next_seq: AtomicU64::new(1),
            topics: DashMap::new(),
            meta: DashMap::new(),
            read_cache: DashMap::new(),
        }
    }

    pub fn publish(&self, store: &str, topic: &str, payload: JsonValue) -> Event {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);

        let idx = extract_index(&payload, ts);
        let payload_json = Arc::new(payload);
        let index_json = Arc::new(idx);

        // Cache msgpack values for RPC response hot paths.
        let payload_mp = Arc::new(rmpv::ext::to_value(payload_json.as_ref()).unwrap_or(MpValue::Nil));
        let index_mp = Arc::new(rmpv::ext::to_value(index_json.as_ref()).unwrap_or(MpValue::Nil));

        let ev = Event {
            seq,
            ts,
            store: store.to_string(),
            topic: topic.to_string(),
            payload_json,
            index_json,
            payload_mp,
            index_mp,
        };

        // Update or create metadata
        self.meta.entry(topic.to_string()).or_insert_with(|| TopicMeta {
            created_at: ts,
            last_ts: ts,
            count_total: 0,
        });

        // Get or create topic queue
        let queue = self.topics.entry(topic.to_string()).or_insert_with(|| {
            Arc::new(RwLock::new(VecDeque::with_capacity(self.maxlen.min(4096))))
        });
        
        // Write to queue
        {
            let mut q = queue.write();
            q.push_back(ev.clone());
            while q.len() > self.maxlen {
                q.pop_front();
            }
        }

        // Update metadata
        if let Some(mut m) = self.meta.get_mut(topic) {
            m.last_ts = ts;
            m.count_total = m.count_total.saturating_add(1);
        }
        
        // Update read cache (lock-free)
        self.update_read_cache(topic);
        
        ev
    }

    pub fn replace_topic(&self, store: &str, topic: &str, items: Vec<JsonValue>) -> Vec<Event> {
        let mut out = Vec::with_capacity(items.len());
        
        let queue = self.topics.entry(topic.to_string()).or_insert_with(|| {
            Arc::new(RwLock::new(VecDeque::with_capacity(self.maxlen.min(4096))))
        });
        queue.write().clear();

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        self.meta.insert(
            topic.to_string(),
            TopicMeta {
                created_at: ts,
                last_ts: ts,
                count_total: 0,
            },
        );
        for p in items {
            let ev = self.publish(store, topic, p);
            out.push(ev);
        }
        out
    }

    pub fn get_recent(&self, _store: &str, topic: &str, limit: usize) -> Vec<Event> {
        // Fast path: try read cache first (lock-free)
        if let Some(cache) = self.read_cache.get(topic) {
            let n = limit.min(cache.len());
            let start = cache.len().saturating_sub(n);
            return cache.iter().skip(start).cloned().collect();
        }
        
        // Slow path: read from queue with lock
        let queue = match self.topics.get(topic) {
            Some(q) => q,
            None => return vec![],
        };
        let q = queue.read();
        let n = limit.min(q.len());
        let start = q.len().saturating_sub(n);
        q.iter().skip(start).cloned().collect()
    }
    
    fn update_read_cache(&self, topic: &str) {
        // Update read cache asynchronously (best-effort, no blocking)
        if let Some(queue) = self.topics.get(topic) {
            if let Some(q) = queue.try_read() {
                let cache: Vec<Event> = q.iter().cloned().collect();
                self.read_cache.insert(topic.to_string(), cache);
            }
        }
    }
}

#[derive(Debug)]
pub struct MpState {
    #[allow(dead_code)]
    pub maxlen: usize,
    #[allow(dead_code)]
    pub topic_max: usize,
    pub stores: DashMap<String, Store>,
}

impl MpState {
    pub fn new(maxlen: usize, topic_max: usize) -> Self {
        let stores = DashMap::new();
        for name in ["messages", "events", "lifecycle", "runs", "export", "memory"] {
            stores.insert(name.to_string(), Store::new(maxlen, topic_max));
        }
        Self {
            maxlen,
            topic_max,
            stores,
        }
    }

    pub fn store(&self, name: &str) -> Option<dashmap::mapref::one::Ref<String, Store>> {
        self.stores.get(name)
    }
}

#[derive(Debug, Clone)]
pub struct PubMsg {
    pub topic: Vec<u8>,
    pub body: Vec<u8>,
}
