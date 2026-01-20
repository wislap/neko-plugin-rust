use rmpv::Value as MpValue;
use serde_json::Value as JsonValue;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use serde::Serialize;

use crate::utils::extract_index;

#[derive(Debug, Clone, Serialize)]
pub struct StoreMetrics {
    pub total_events: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub total_publishes: u64,
    pub total_queries: u64,
}

#[derive(Debug, Clone)]
pub struct Event {
    pub seq: u64,
    pub ts: f64,
    pub store: Arc<str>,
    pub topic: Arc<str>,
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
    pub topics: DashMap<String, Arc<RwLock<VecDeque<Arc<Event>>>>>,
    pub meta: DashMap<String, TopicMeta>,
    // Read cache: lock-free recent events for fast get_recent
    pub read_cache: DashMap<String, Vec<Arc<Event>>>,
    // Metrics
    pub metrics_total_publishes: AtomicU64,
    pub metrics_total_queries: AtomicU64,
    pub metrics_cache_hits: AtomicU64,
    pub metrics_cache_misses: AtomicU64,
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
            metrics_total_publishes: AtomicU64::new(0),
            metrics_total_queries: AtomicU64::new(0),
            metrics_cache_hits: AtomicU64::new(0),
            metrics_cache_misses: AtomicU64::new(0),
        }
    }
    
    pub fn get_metrics(&self) -> StoreMetrics {
        let total_events = self.next_seq.load(Ordering::Relaxed).saturating_sub(1);
        StoreMetrics {
            total_events,
            cache_hits: self.metrics_cache_hits.load(Ordering::Relaxed),
            cache_misses: self.metrics_cache_misses.load(Ordering::Relaxed),
            total_publishes: self.metrics_total_publishes.load(Ordering::Relaxed),
            total_queries: self.metrics_total_queries.load(Ordering::Relaxed),
        }
    }

    #[inline]
    pub fn publish(&self, store: &str, topic: &str, payload: JsonValue) -> Arc<Event> {
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

        let ev = Arc::new(Event {
            seq,
            ts,
            store: Arc::from(store),
            topic: Arc::from(topic),
            payload_json,
            index_json,
            payload_mp,
            index_mp,
        });

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
            q.push_back(Arc::clone(&ev));
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
        
        // Update metrics
        self.metrics_total_publishes.fetch_add(1, Ordering::Relaxed);
        
        ev
    }

    pub fn replace_topic(&self, store: &str, topic: &str, items: Vec<JsonValue>) -> Vec<Arc<Event>> {
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

    #[inline]
    pub fn get_recent(&self, _store: &str, topic: &str, limit: usize) -> Vec<Arc<Event>> {
        // Fast path: try read cache first (lock-free)
        if let Some(cache) = self.read_cache.get(topic) {
            self.metrics_cache_hits.fetch_add(1, Ordering::Relaxed);
            let n = limit.min(cache.len());
            let start = cache.len().saturating_sub(n);
            return cache[start..].to_vec();
        }
        
        self.metrics_cache_misses.fetch_add(1, Ordering::Relaxed);
        
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
    
    #[inline]
    fn update_read_cache(&self, topic: &str) {
        // Update read cache asynchronously (best-effort, no blocking)
        if let Some(queue) = self.topics.get(topic) {
            if let Some(q) = queue.try_read() {
                let cache: Vec<Arc<Event>> = q.iter().cloned().collect();
                self.read_cache.insert(topic.to_string(), cache);
            }
        }
    }

    #[inline]
    pub fn get_since(&self, _store: &str, topic: Option<&str>, after_seq: u64, limit: usize) -> Vec<Arc<Event>> {
        self.metrics_total_queries.fetch_add(1, Ordering::Relaxed);
        
        let topics_to_scan: Vec<String> = match topic {
            Some(t) if !t.is_empty() && t != "*" => vec![t.to_string()],
            _ => self.topics.iter().map(|entry| entry.key().clone()).collect(),
        };
        
        let mut snapshots: Vec<Arc<Event>> = Vec::new();
        for topic_name in topics_to_scan {
            if let Some(queue_ref) = self.topics.get(&topic_name) {
                let q = queue_ref.read();
                for ev in q.iter() {
                    if ev.seq > after_seq {
                        snapshots.push(Arc::clone(ev));
                    }
                }
            }
        }
        
        // Sort by seq ascending
        snapshots.sort_by_key(|ev| ev.seq);
        
        // Apply limit
        if snapshots.len() > limit {
            snapshots.truncate(limit);
        }
        
        snapshots
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
        
        // Bus-specific configurations for optimal memory usage
        // messages: high-frequency read/write, needs full capacity
        stores.insert("messages".to_string(), Store::new(maxlen, topic_max));
        
        // events: medium-frequency writes, moderate capacity
        let events_maxlen = (maxlen / 2).max(10000);
        let events_topic_max = (topic_max / 2).max(1000);
        stores.insert("events".to_string(), Store::new(events_maxlen, events_topic_max));
        
        // lifecycle: low-frequency critical events, small capacity
        let lifecycle_maxlen = (maxlen / 20).max(1000);
        let lifecycle_topic_max = (topic_max / 4).max(500);
        stores.insert("lifecycle".to_string(), Store::new(lifecycle_maxlen, lifecycle_topic_max));
        
        // runs: low-frequency large objects, very small capacity
        let runs_maxlen = (maxlen / 40).max(500);
        let runs_topic_max = (topic_max / 10).max(200);
        stores.insert("runs".to_string(), Store::new(runs_maxlen, runs_topic_max));
        
        // export: temporary buffer, moderate capacity
        let export_maxlen = (maxlen / 4).max(5000);
        let export_topic_max = (topic_max / 4).max(500);
        stores.insert("export".to_string(), Store::new(export_maxlen, export_topic_max));
        
        // memory: context storage, moderate capacity
        let memory_maxlen = (maxlen / 10).max(2000);
        let memory_topic_max = (topic_max / 2).max(1000);
        stores.insert("memory".to_string(), Store::new(memory_maxlen, memory_topic_max));
        
        Self {
            maxlen,
            topic_max,
            stores,
        }
    }

    pub fn store(&self, name: &str) -> Option<dashmap::mapref::one::Ref<'_, String, Store>> {
        self.stores.get(name)
    }
}

#[derive(Debug, Clone)]
pub struct PubMsg {
    pub topic: Vec<u8>,
    pub body: Vec<u8>,
}
