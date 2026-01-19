use crossbeam::queue::ArrayQueue;
use std::sync::Arc;

/// Buffer pool for reusing Vec<u8> allocations
pub struct BufferPool {
    pool: Arc<ArrayQueue<Vec<u8>>>,
    default_capacity: usize,
}

impl BufferPool {
    pub fn new(pool_size: usize, default_capacity: usize) -> Self {
        let pool = Arc::new(ArrayQueue::new(pool_size));
        
        // Pre-allocate buffers
        for _ in 0..pool_size.min(100) {
            let _ = pool.push(Vec::with_capacity(default_capacity));
        }
        
        Self {
            pool,
            default_capacity,
        }
    }
    
    /// Get a buffer from the pool or allocate a new one
    pub fn get(&self) -> Vec<u8> {
        self.pool
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(self.default_capacity))
    }
    
    /// Return a buffer to the pool
    pub fn put(&self, mut buf: Vec<u8>) {
        buf.clear();
        // Only return buffers that aren't too large (prevent memory bloat)
        if buf.capacity() <= self.default_capacity * 4 {
            let _ = self.pool.push(buf);
        }
    }
    
    /// Clone the pool reference for use in multiple threads
    pub fn clone_ref(&self) -> Self {
        Self {
            pool: Arc::clone(&self.pool),
            default_capacity: self.default_capacity,
        }
    }
}

impl Clone for BufferPool {
    fn clone(&self) -> Self {
        self.clone_ref()
    }
}

/// Envelope pool for reusing Vec<Vec<u8>> allocations (ZMQ envelopes)
pub struct EnvelopePool {
    pool: Arc<ArrayQueue<Vec<Vec<u8>>>>,
}

impl EnvelopePool {
    pub fn new(pool_size: usize) -> Self {
        let pool = Arc::new(ArrayQueue::new(pool_size));
        
        // Pre-allocate envelopes
        for _ in 0..pool_size.min(100) {
            let _ = pool.push(Vec::with_capacity(4));
        }
        
        Self { pool }
    }
    
    pub fn get(&self) -> Vec<Vec<u8>> {
        self.pool
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(4))
    }
    
    pub fn put(&self, mut env: Vec<Vec<u8>>) {
        env.clear();
        if env.capacity() <= 8 {
            let _ = self.pool.push(env);
        }
    }
    
    pub fn clone_ref(&self) -> Self {
        Self {
            pool: Arc::clone(&self.pool),
        }
    }
}

impl Clone for EnvelopePool {
    fn clone(&self) -> Self {
        self.clone_ref()
    }
}
