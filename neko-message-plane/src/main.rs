mod config;
mod handlers;
mod query;
mod rpc;
mod types;
mod utils;

use clap::Parser;
use serde_json::Value as JsonValue;
use std::sync::mpsc;
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

    let ctx = zmq::Context::new();
    let state = Arc::new(MpState::new(maxlen, topic_max));

    let (pub_tx, pub_rx) = mpsc::channel::<PubMsg>();

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
                // Flush any queued pub messages from RPC side.
                if pub_enabled {
                    for _ in 0..256 {
                        match pub_rx.try_recv() {
                            Ok(pm) => {
                                let _ = pub_sock.send_multipart(&[pm.topic, pm.body], 0);
                            }
                            Err(mpsc::TryRecvError::Empty) => break,
                            Err(mpsc::TryRecvError::Disconnected) => break,
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

    // RPC server loop (single thread)
    let router = ctx.socket(zmq::ROUTER).expect("ROUTER");
    router.set_linger(0).ok();
    router.bind(&rpc_endpoint).expect("bind rpc");

    loop {
        let parts = match router.recv_multipart(0) {
            Ok(p) => p,
            Err(_) => {
                std::thread::yield_now();
                continue;
            }
        };
        if parts.len() < 2 {
            continue;
        }
        let envelope = &parts[..parts.len() - 1];
        let body = &parts[parts.len() - 1];

        let resp_raw = if let Some(v) = decode_msgpack_value(body) {
            handle_rpc_mp(&v, &state, Some(&pub_tx))
        } else {
            // Fallback path: accept JSON envelope
            let req = decode_msgpack(body).or_else(|| decode_json(body)).unwrap_or(JsonValue::Null);
            let resp = handle_rpc(&req, &state, Some(&pub_tx));
            rmp_serde::to_vec_named(&resp).unwrap_or_default()
        };

        let mut out = Vec::with_capacity(envelope.len() + 1);
        for f in envelope {
            out.push(f.clone());
        }
        out.push(resp_raw);
        let _ = router.send_multipart(out, 0);
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
