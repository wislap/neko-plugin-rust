use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(name = "neko-message-plane")]
pub struct Cli {
    #[arg(long, default_value = "tcp://127.0.0.1:38865")]
    pub rpc_endpoint: String,

    #[arg(long, default_value = "tcp://127.0.0.1:38867")]
    pub ingest_endpoint: String,

    #[arg(long, default_value = "tcp://127.0.0.1:38866")]
    pub pub_endpoint: String,

    #[arg(long, default_value_t = 20000)]
    pub store_maxlen: usize,

    #[arg(long, default_value_t = 2000)]
    pub topic_max: usize,

    #[arg(long, default_value_t = 128)]
    pub topic_name_max_len: usize,

    #[arg(long, default_value_t = 262144)]
    pub payload_max_bytes: usize,

    #[arg(long, default_value = "strict")]
    pub validate_mode: String,

    #[arg(long, default_value_t = true)]
    pub validate_payload_bytes: bool,

    #[arg(long, default_value_t = true)]
    pub pub_enabled: bool,

    #[arg(long, default_value_t = 1000)]
    pub get_recent_max_limit: usize,

    #[arg(long, default_value_t = 0)]
    pub workers: usize,
}

pub fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

impl Cli {
    /// Apply environment variable overrides to CLI defaults
    pub fn apply_env_overrides(&mut self) {
        if self.rpc_endpoint == "tcp://127.0.0.1:38865" {
            self.rpc_endpoint = env_or("NEKO_MESSAGE_PLANE_ZMQ_RPC_ENDPOINT", "tcp://127.0.0.1:38865");
        }
        if self.ingest_endpoint == "tcp://127.0.0.1:38867" {
            self.ingest_endpoint = env_or("NEKO_MESSAGE_PLANE_ZMQ_INGEST_ENDPOINT", "tcp://127.0.0.1:38867");
        }
        if self.pub_endpoint == "tcp://127.0.0.1:38866" {
            self.pub_endpoint = env_or("NEKO_MESSAGE_PLANE_ZMQ_PUB_ENDPOINT", "tcp://127.0.0.1:38866");
        }
        if self.store_maxlen == 20000 {
            self.store_maxlen = std::env::var("NEKO_MESSAGE_PLANE_STORE_MAXLEN")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(20000);
        }
        if self.topic_max == 2000 {
            self.topic_max = std::env::var("NEKO_MESSAGE_PLANE_TOPIC_MAX")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(2000);
        }
        if self.topic_name_max_len == 128 {
            self.topic_name_max_len = std::env::var("NEKO_MESSAGE_PLANE_TOPIC_NAME_MAX_LEN")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(128);
        }
        if self.payload_max_bytes == 262144 {
            self.payload_max_bytes = std::env::var("NEKO_MESSAGE_PLANE_PAYLOAD_MAX_BYTES")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(256 * 1024);
        }
        if self.validate_mode == "strict" {
            self.validate_mode = std::env::var("NEKO_MESSAGE_PLANE_VALIDATE_MODE")
                .unwrap_or_else(|_| "strict".to_string())
                .to_lowercase();
        }
        if self.get_recent_max_limit == 1000 {
            self.get_recent_max_limit = std::env::var("NEKO_MESSAGE_PLANE_GET_RECENT_MAX_LIMIT")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(1000);
        }
        if self.validate_payload_bytes {
            self.validate_payload_bytes = std::env::var("NEKO_MESSAGE_PLANE_VALIDATE_PAYLOAD_BYTES")
                .ok()
                .map(|s| matches!(s.to_lowercase().as_str(), "true" | "1" | "yes" | "on"))
                .unwrap_or(true);
        }
        if self.pub_enabled {
            self.pub_enabled = std::env::var("NEKO_MESSAGE_PLANE_PUB_ENABLED")
                .ok()
                .map(|s| matches!(s.to_lowercase().as_str(), "true" | "1" | "yes" | "on"))
                .unwrap_or(true);
        }
        if self.workers == 0 {
            self.workers = std::env::var("NEKO_MESSAGE_PLANE_WORKERS")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(0);
        }
    }
    
    /// Get effective worker count (0 means auto-detect CPU cores)
    pub fn get_workers(&self) -> usize {
        if self.workers == 0 {
            num_cpus::get().max(4)
        } else {
            self.workers
        }
    }

    /// Export config values to environment variables for use by handlers
    pub fn export_to_env(&self) {
        std::env::set_var("NEKO_MESSAGE_PLANE_VALIDATE_MODE", &self.validate_mode);
        std::env::set_var("NEKO_MESSAGE_PLANE_TOPIC_MAX", self.topic_max.to_string());
        std::env::set_var("NEKO_MESSAGE_PLANE_TOPIC_NAME_MAX_LEN", self.topic_name_max_len.to_string());
        std::env::set_var("NEKO_MESSAGE_PLANE_PAYLOAD_MAX_BYTES", self.payload_max_bytes.to_string());
        std::env::set_var(
            "NEKO_MESSAGE_PLANE_VALIDATE_PAYLOAD_BYTES",
            if self.validate_payload_bytes { "true" } else { "false" },
        );
        std::env::set_var("NEKO_MESSAGE_PLANE_GET_RECENT_MAX_LIMIT", self.get_recent_max_limit.to_string());
    }
}
