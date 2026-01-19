mod config;
mod handlers;
mod query;
mod rpc;
mod types;
mod utils;

use clap::Parser;
use crossbeam::channel;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use std::thread;

use config::Cli;
use handlers::{handle_rpc, handle_rpc_mp};
use types::{MpState, PubMsg};
use utils::{decode_json, decode_msgpack, decode_msgpack_value};

fn main() {
    env_logger::init();

    let mut cli = Cli::parse();
    cli.apply_env_overrides();
    cli.export_to_env();

    let rpc_endpoint = cli.rpc_endpoint.clone();
    let ingest_endpoint = cli.ingest_endpoint.clone();
    let pub_endpoint = cli.pub_endpoint.clone();
    let maxlen = cli.store_maxlen;
    let topic_max = cli.topic_max;
    let topic_name_max_len = cli.topic_name_max_len;
    let payload_max_bytes = cli.payload_max_bytes;
    let validate_payload_bytes = cli.validate_payload_bytes;
    let pub_enabled = cli.pub_enabled;

    let n_workers = std::env::var("NEKO_MESSAGE_PLANE_WORKERS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| num_cpus::get().max(4));
    
    log::info!("[message_plane] starting with {} worker threads", n_workers);

    let ctx = zmq::Context::new();
    let state = Arc::new(MpState::new(maxlen, topic_max));

    let (pub_tx, pub_rx) = std::sync::mpsc::channel::<PubMsg>();
    let (task_tx, task_rx) = channel::unbounded::<(Vec<Vec<u8>>, Vec<u8>)>();
    let (result_tx, result_rx) = channel::unbounded::<(Vec<Vec<u8>>, Vec<u8>)>();

    // Ingest thread
    {
        let ctx = ctx.clone();
        let state = Arc::clone(&state);
        let pub_ep = pub_endpoint.clone();
        thread::spawn(move || {
            let pull = ctx.socket(zmq::PULL).expect("PULL");
            pull.set_linger(0).ok();
            pull.bind(&ingest_endpoint).expect("bind ingest");

            let pub_sock = ctx.socket(zmq::PUB).expect("PUB");
            pub_sock.set_linger(0).ok();
            if pub_enabled {
                pub_sock.bind(&pub_ep).expect("bind pub");
            }

            loop {
                if pub_enabled {
                    for _ in 0..256 {
                        match pub_rx.try_recv() {
                            Ok(pm) => {
                                let _ = pub_sock.send_multipart(&[pm.topic, pm.body], 0);
                            }
                            Err(std::sync::mpsc::TryRecvError::Empty) => break,
                            Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
                        }
                    }
                }

                let raw = match pull.recv_bytes(0) {
                    Ok(b) => b,
                    Err(_) => {
                        std::thread::yield_now();
                        continue;
                    }
                };

                let msg = match decode_msgpack(&raw) {
                    Some(v) => v,
                    None => continue,
                };
                let obj = match msg.as_object() {
                    Some(o) => o,
                    None => continue,
                };

                let kind = obj.get("kind").and_then(|x| x.as_str()).unwrap_or("delta_batch");
                if kind == "snapshot" {
                    handle_snapshot(&state, obj, topic_max, topic_name_max_len, payload_max_bytes, validate_payload_bytes, pub_enabled, &pub_sock);
                    continue;
                }

                handle_delta_batch(&state, obj, topic_max, topic_name_max_len, payload_max_bytes, validate_payload_bytes, pub_enabled, &pub_sock);
            }
        });
    }

    // Worker threads pool
    for worker_id in 0..n_workers {
        let task_rx = task_rx.clone();
        let result_tx = result_tx.clone();
        let state = Arc::clone(&state);
        let pub_tx = pub_tx.clone();

        thread::spawn(move || {
            log::debug!("[worker-{}] started", worker_id);
            loop {
                let (envelope, body) = match task_rx.recv() {
                    Ok(task) => task,
                    Err(_) => break,
                };

                let resp_raw = if let Some(v) = decode_msgpack_value(&body) {
                    handle_rpc_mp(&v, &state, Some(&pub_tx))
                } else {
                    let req = decode_msgpack(&body).or_else(|| decode_json(&body)).unwrap_or(JsonValue::Null);
                    let resp = handle_rpc(&req, &state, Some(&pub_tx));
                    rmp_serde::to_vec_named(&resp).unwrap_or_default()
                };

                if result_tx.send((envelope, resp_raw)).is_err() {
                    break;
                }
            }
            log::debug!("[worker-{}] stopped", worker_id);
        });
    }

    // RPC receiver thread
    {
        let ctx = ctx.clone();
        let rpc_ep = rpc_endpoint.clone();
        thread::spawn(move || {
            let router = ctx.socket(zmq::ROUTER).expect("ROUTER");
            router.set_linger(0).ok();
            router.bind(&rpc_ep).expect("bind rpc");
            log::info!("[message_plane] rpc server bound: {}", rpc_ep);

            loop {
                let parts = match router.recv_multipart(zmq::DONTWAIT) {
                    Ok(p) => p,
                    Err(zmq::Error::EAGAIN) => {
                        std::thread::sleep(std::time::Duration::from_micros(100));
                        continue;
                    }
                    Err(_) => {
                        std::thread::yield_now();
                        continue;
                    }
                };
                if parts.len() < 2 {
                    continue;
                }
                let envelope = parts[..parts.len() - 1].to_vec();
                let body = parts[parts.len() - 1].clone();

                if task_tx.send((envelope, body)).is_err() {
                    log::error!("[message_plane] failed to send task to workers");
                    break;
                }
            }
        });
    }

    // RPC sender thread (main thread)
    let ctx_main = ctx.clone();
    let router = ctx_main.socket(zmq::ROUTER).expect("ROUTER main");
    router.set_linger(0).ok();
    router.connect(&format!("inproc://rpc-results")).ok();

    let sender_ctx = ctx.clone();
    thread::spawn(move || {
        let sender = sender_ctx.socket(zmq::DEALER).expect("DEALER");
        sender.set_linger(0).ok();
        sender.bind("inproc://rpc-results").expect("bind inproc");

        loop {
            let (envelope, resp_raw) = match result_rx.recv() {
                Ok(r) => r,
                Err(_) => break,
            };

            let mut out = Vec::with_capacity(envelope.len() + 1);
            for f in envelope {
                out.push(f);
            }
            out.push(resp_raw);
            
            if sender.send_multipart(out, 0).is_err() {
                log::error!("[message_plane] failed to send response");
            }
        }
    });

    // Main loop: forward responses to clients
    let final_router = ctx.socket(zmq::ROUTER).expect("ROUTER final");
    final_router.set_linger(0).ok();
    final_router.bind(&rpc_endpoint).expect("bind rpc final");

    loop {
        match result_rx.recv() {
            Ok((envelope, resp_raw)) => {
                let mut out = Vec::with_capacity(envelope.len() + 1);
                for f in envelope {
                    out.push(f);
                }
                out.push(resp_raw);
                let _ = final_router.send_multipart(out, 0);
            }
            Err(_) => break,
        }
    }
}

fn handle_snapshot(
    state: &Arc<MpState>,
    obj: &serde_json::Map<String, JsonValue>,
    topic_max: usize,
    topic_name_max_len: usize,
    payload_max_bytes: usize,
    validate_payload_bytes: bool,
    pub_enabled: bool,
    pub_sock: &zmq::Socket,
) {
    let store = obj
        .get("store")
        .or_else(|| obj.get("bus"))
        .and_then(|x| x.as_str())
        .unwrap_or("messages");
    let topic = obj.get("topic").and_then(|x| x.as_str()).unwrap_or("snapshot.all");
    if topic.is_empty() || topic.len() > topic_name_max_len {
        return;
    }
    let items = obj.get("items").and_then(|x| x.as_array()).cloned().unwrap_or_default();
    let mode = obj.get("mode").and_then(|x| x.as_str()).unwrap_or("replace");
    let mut records: Vec<JsonValue> = Vec::with_capacity(items.len());
    for it in items {
        if !it.is_object() {
            continue;
        }
        if validate_payload_bytes {
            if let Ok(b) = rmp_serde::to_vec_named(&it) {
                if b.len() > payload_max_bytes {
                    continue;
                }
            } else {
                continue;
            }
        }
        records.push(it);
    }

    if let Some(store_ref) = state.store(store) {
        let is_new_topic = !store_ref.meta.contains_key(topic);
        if is_new_topic && store_ref.meta.len() >= topic_max {
            return;
        }

        let events = if mode == "append" {
            let mut out = Vec::with_capacity(records.len());
            for rec in records {
                out.push(store_ref.publish(store, topic, rec));
            }
            out
        } else {
            store_ref.replace_topic(store, topic, records)
        };
        
        if pub_enabled {
            for ev in events {
                let topic_bytes = format!("{}.{}", ev.store, ev.topic).as_bytes().to_vec();
                let mut pub_map: Vec<(rmpv::Value, rmpv::Value)> = Vec::with_capacity(6);
                pub_map.push((rmpv::Value::from("seq"), rmpv::Value::from(ev.seq as i64)));
                pub_map.push((rmpv::Value::from("ts"), rmpv::Value::from(ev.ts)));
                pub_map.push((rmpv::Value::from("store"), rmpv::Value::from(ev.store.as_str())));
                pub_map.push((rmpv::Value::from("topic"), rmpv::Value::from(ev.topic.as_str())));
                pub_map.push((rmpv::Value::from("payload"), (*ev.payload_mp).clone()));
                pub_map.push((rmpv::Value::from("index"), (*ev.index_mp).clone()));
                let body = rmp_serde::to_vec_named(&rmpv::Value::Map(pub_map)).unwrap_or_default();
                let _ = pub_sock.send_multipart(&[topic_bytes, body], 0);
            }
        }
    }
}

fn handle_delta_batch(
    state: &Arc<MpState>,
    obj: &serde_json::Map<String, JsonValue>,
    topic_max: usize,
    topic_name_max_len: usize,
    payload_max_bytes: usize,
    validate_payload_bytes: bool,
    pub_enabled: bool,
    pub_sock: &zmq::Socket,
) {
    let items = obj.get("items").and_then(|x| x.as_array()).cloned().unwrap_or_default();
    for it in items {
        let it_obj = match it.as_object() {
            Some(o) => o,
            None => continue,
        };
        let store = it_obj
            .get("store")
            .or_else(|| it_obj.get("bus"))
            .and_then(|x| x.as_str())
            .unwrap_or("messages");
        let topic = it_obj.get("topic").and_then(|x| x.as_str()).unwrap_or("all");
        if topic.is_empty() || topic.len() > topic_name_max_len {
            continue;
        }
        let payload = it_obj.get("payload").cloned().unwrap_or(JsonValue::Null);
        let payload = if payload.is_object() {
            payload
        } else {
            serde_json::json!({"value": payload})
        };

        if validate_payload_bytes {
            if let Ok(b) = rmp_serde::to_vec_named(&payload) {
                if b.len() > payload_max_bytes {
                    continue;
                }
            } else {
                continue;
            }
        }

        let ev = match state.store(store) {
            Some(store_ref) => {
                let is_new_topic = !store_ref.meta.contains_key(topic);
                if is_new_topic && store_ref.meta.len() >= topic_max {
                    continue;
                }
                store_ref.publish(store, topic, payload)
            }
            None => continue,
        };

        if pub_enabled {
            let topic_bytes = format!("{}.{}", ev.store, ev.topic).as_bytes().to_vec();
            let mut pub_map: Vec<(rmpv::Value, rmpv::Value)> = Vec::with_capacity(6);
            pub_map.push((rmpv::Value::from("seq"), rmpv::Value::from(ev.seq as i64)));
            pub_map.push((rmpv::Value::from("ts"), rmpv::Value::from(ev.ts)));
            pub_map.push((rmpv::Value::from("store"), rmpv::Value::from(ev.store.as_str())));
            pub_map.push((rmpv::Value::from("topic"), rmpv::Value::from(ev.topic.as_str())));
            pub_map.push((rmpv::Value::from("payload"), (*ev.payload_mp).clone()));
            pub_map.push((rmpv::Value::from("index"), (*ev.index_mp).clone()));
            let body = rmp_serde::to_vec_named(&rmpv::Value::Map(pub_map)).unwrap_or_default();
            let _ = pub_sock.send_multipart(&[topic_bytes, body], 0);
        }
    }
}
