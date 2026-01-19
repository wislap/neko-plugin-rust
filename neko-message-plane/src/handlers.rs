use parking_lot::Mutex;
use rmpv::Value as MpValue;
use serde_json::Value as JsonValue;
use std::sync::mpsc;
use std::sync::Arc;

use crate::query::eval_plan;
use crate::rpc::{
    rpc_err, rpc_ok, RpcGetRecentResult, RpcHealthResult, RpcPublishResult, RpcQueryResult,
    RpcReplayResult,
};
use crate::types::{Event, MpState, PubMsg};
use crate::utils::{json_obj, mp_get, mp_get_str, mp_to_json, now_ts};

/// Handle RPC request in MessagePack format
pub fn handle_rpc_mp(
    req: &MpValue,
    state: &Arc<MpState>,
    pub_tx: Option<&mpsc::Sender<PubMsg>>,
) -> Vec<u8> {
    let req_id = mp_get_str(req, "req_id").unwrap_or("");
    let op = mp_get_str(req, "op").unwrap_or("");
    let args = mp_get(req, "args").cloned().unwrap_or(MpValue::Nil);
    let args_obj = args.as_map().cloned().unwrap_or_default();

    let mode = std::env::var("NEKO_MESSAGE_PLANE_VALIDATE_MODE")
        .unwrap_or_else(|_| "strict".to_string())
        .to_lowercase();
    let strict = mode == "strict";

    let v_raw = mp_get(req, "v");
    let v = match (&*mode, v_raw) {
        ("off", Some(vv)) => vv.as_i64().unwrap_or(1),
        ("off", None) => 1,
        ("warn", Some(vv)) => vv.as_i64().unwrap_or(1),
        ("warn", None) => 1,
        ("strict", Some(vv)) => vv.as_i64().unwrap_or(-1),
        ("strict", None) => {
            return rpc_err(req_id, "BAD_VERSION", "missing protocol version", None)
        }
        (_, Some(vv)) => vv.as_i64().unwrap_or(1),
        (_, None) => 1,
    };
    if v != 1 {
        return rpc_err(
            req_id,
            "BAD_VERSION",
            &format!("unsupported protocol version: {}", v),
            None,
        );
    }

    if op == "ping" || op == "health" {
        return rpc_ok(
            req_id,
            RpcHealthResult {
                ok: true,
                ts: now_ts(),
            },
        );
    }

    if op == "bus.get_recent" {
        return handle_get_recent_mp(req_id, &args_obj, state);
    }

    if op == "bus.replay" {
        return handle_replay_mp(req_id, &args, &mode, state);
    }

    if op == "bus.query" {
        return handle_query_mp(req_id, &args, &mode, state);
    }

    if op == "bus.publish" {
        return handle_publish_mp(req_id, &args, state, pub_tx);
    }

    if strict {
        return rpc_err(req_id, "UNKNOWN_OP", &format!("unknown op: {}", op), None);
    }
    rpc_err(req_id, "UNKNOWN_OP", &format!("unknown op: {}", op), None)
}

fn handle_get_recent_mp(
    req_id: &str,
    args_obj: &[(MpValue, MpValue)],
    state: &Arc<MpState>,
) -> Vec<u8> {
    let store = {
        let mut s = "messages";
        for (k, v) in args_obj.iter() {
            if k.as_str() == Some("store") {
                if let Some(ss) = v.as_str() {
                    s = ss;
                }
            }
        }
        s.to_string()
    };
    let topic = {
        let mut t = "all";
        for (k, v) in args_obj.iter() {
            if k.as_str() == Some("topic") {
                if let Some(ts) = v.as_str() {
                    t = ts;
                }
            }
        }
        t.to_string()
    };
    let mut limit: usize = 200;
    let mut light = false;
    for (k, v) in args_obj.iter() {
        if k.as_str() == Some("limit") {
            if let Some(n) = v.as_u64() {
                limit = n as usize;
            } else if let Some(n) = v.as_i64() {
                if n > 0 {
                    limit = n as usize;
                }
            }
        }
        if k.as_str() == Some("light") {
            if let Some(b) = v.as_bool() {
                light = b;
            }
        }
    }
    let max_limit = std::env::var("NEKO_MESSAGE_PLANE_GET_RECENT_MAX_LIMIT")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000);
    if limit > max_limit {
        limit = max_limit;
    }

    let items = match state.store(&store) {
        Some(s) => s.get_recent("", &topic, limit),
        None => return rpc_err(req_id, "BAD_STORE", "invalid store", None),
    };

    let out_items = events_to_mp_vec(&items, light);
    rpc_ok(
        req_id,
        RpcGetRecentResult {
            store,
            topic,
            items: out_items,
            light,
        },
    )
}

fn handle_replay_mp(
    req_id: &str,
    args: &MpValue,
    mode: &str,
    state: &Arc<MpState>,
) -> Vec<u8> {
    if mode == "strict" {
        let st_raw = mp_get_str(args, "store").or_else(|| mp_get_str(args, "bus"));
        if st_raw.is_none() {
            return rpc_err(req_id, "BAD_ARGS", "invalid args: missing store", None);
        }
        let plan_raw = mp_get(args, "plan").or_else(|| mp_get(args, "trace"));
        if !matches!(plan_raw, Some(v) if v.is_map()) {
            return rpc_err(
                req_id,
                "BAD_ARGS",
                "invalid args: missing/invalid plan",
                None,
            );
        }
    } else if mode == "warn" {
        let plan_raw = mp_get(args, "plan").or_else(|| mp_get(args, "trace"));
        if !matches!(plan_raw, Some(v) if v.is_map()) {
            log::warn!("[message_plane] invalid args for bus.replay: missing/invalid plan");
        }
    }

    let store_name = mp_get_str(args, "store")
        .or_else(|| mp_get_str(args, "bus"))
        .unwrap_or("messages");
    let plan_mp = mp_get(args, "plan").or_else(|| mp_get(args, "trace"));
    let plan_mp = match plan_mp {
        Some(v) if v.is_map() => v,
        _ => return rpc_err(req_id, "BAD_ARGS", "plan is required", None),
    };
    let plan_json = match mp_to_json(plan_mp) {
        Some(j) => j,
        None => return rpc_err(req_id, "BAD_ARGS", "invalid plan", None),
    };
    let light = mp_get(args, "light")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let items = match state.store(store_name) {
        Some(store_ref) => eval_plan(&*store_ref, &plan_json),
        None => return rpc_err(req_id, "BAD_STORE", "invalid store", None),
    };

    let mut items = match items {
        Some(v) => v,
        None => return rpc_err(req_id, "BAD_ARGS", "unsupported plan", None),
    };

    let max_limit = std::env::var("NEKO_MESSAGE_PLANE_GET_RECENT_MAX_LIMIT")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000);
    if items.len() > max_limit {
        items.truncate(max_limit);
    }

    let out_items = events_to_mp_vec(&items, light);
    rpc_ok(
        req_id,
        RpcReplayResult {
            store: store_name.to_string(),
            items: out_items,
            light,
        },
    )
}

fn handle_query_mp(
    req_id: &str,
    args: &MpValue,
    mode: &str,
    state: &Arc<MpState>,
) -> Vec<u8> {
    let store = mp_get_str(args, "store").unwrap_or("messages");
    let mut topic = mp_get_str(args, "topic").unwrap_or("*");
    let light = mp_get(args, "light")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let mut limit = mp_get(args, "limit")
        .and_then(|v| v.as_u64())
        .unwrap_or(200) as i64;
    if limit <= 0 {
        if mode == "strict" {
            return rpc_err(req_id, "BAD_ARGS", "invalid args: limit<=0", None);
        }
        if mode == "warn" {
            log::warn!("[message_plane] invalid args for bus.query: limit<=0");
        }
        limit = 200;
    }
    if limit > 10000 {
        if mode == "warn" {
            log::warn!("[message_plane] bus.query clamp limit {} -> 10000", limit);
        }
        limit = 10000;
    }

    if topic.is_empty() {
        if mode == "strict" {
            return rpc_err(req_id, "BAD_ARGS", "invalid args: empty topic", None);
        }
        if mode == "warn" {
            log::warn!("[message_plane] invalid args for bus.query: empty topic; using '*'");
        }
        topic = "*";
    }

    let plugin_id = mp_get_str(args, "plugin_id").filter(|s| !s.is_empty());
    let source = mp_get_str(args, "source").filter(|s| !s.is_empty());
    let kind = mp_get_str(args, "kind").filter(|s| !s.is_empty());
    let type_ = mp_get_str(args, "type").filter(|s| !s.is_empty());

    let priority_min = mp_get(args, "priority_min")
        .and_then(|v| v.as_i64())
        .or_else(|| {
            mp_get_str(args, "priority_min").and_then(|s| s.parse::<i64>().ok())
        });
    let since_ts = mp_get(args, "since_ts")
        .and_then(|v| v.as_f64())
        .or_else(|| mp_get_str(args, "since_ts").and_then(|s| s.parse::<f64>().ok()));
    let until_ts = mp_get(args, "until_ts")
        .and_then(|v| v.as_f64())
        .or_else(|| mp_get_str(args, "until_ts").and_then(|s| s.parse::<f64>().ok()));

    let mut snapshots: Vec<Event> = Vec::new();
    if let Some(s) = state.store(store) {
        if topic.trim() == "*" {
            for entry in s.topics.iter() {
                let dq = entry.value().read();
                snapshots.extend(dq.iter().cloned());
            }
        } else if let Some(dq_arc) = s.topics.get(topic) {
            let dq = dq_arc.read();
            snapshots.extend(dq.iter().cloned());
        }
    }

    let mut out: Vec<Event> = Vec::new();
    for ev in snapshots {
        let idx = match ev.index_json.as_ref().as_object() {
            Some(o) => o,
            None => continue,
        };

        if let Some(pid) = plugin_id {
            if idx.get("plugin_id").and_then(|v| v.as_str()) != Some(pid) {
                continue;
            }
        }
        if let Some(src) = source {
            if idx.get("source").and_then(|v| v.as_str()) != Some(src) {
                continue;
            }
        }
        if let Some(kd) = kind {
            if idx.get("kind").and_then(|v| v.as_str()) != Some(kd) {
                continue;
            }
        }
        if let Some(tp) = type_ {
            if idx.get("type").and_then(|v| v.as_str()) != Some(tp) {
                continue;
            }
        }
        if let Some(pmin) = priority_min {
            let p = idx.get("priority").and_then(|v| v.as_i64()).unwrap_or(0);
            if p < pmin {
                continue;
            }
        }
        if let Some(s_ts) = since_ts {
            let tsv = idx
                .get("timestamp")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            if tsv < s_ts {
                continue;
            }
        }
        if let Some(u_ts) = until_ts {
            let tsv = idx
                .get("timestamp")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            if tsv > u_ts {
                continue;
            }
        }

        out.push(ev);
    }

    out.sort_by(|a, b| b.seq.cmp(&a.seq));
    let nn = limit as usize;
    if out.len() > nn {
        out.truncate(nn);
    }

    let out_items = events_to_mp_vec(&out, light);
    rpc_ok(
        req_id,
        RpcQueryResult {
            store: store.to_string(),
            topic: topic.to_string(),
            items: out_items,
            light,
        },
    )
}

fn handle_publish_mp(
    req_id: &str,
    args: &MpValue,
    state: &Arc<MpState>,
    pub_tx: Option<&mpsc::Sender<PubMsg>>,
) -> Vec<u8> {
    let topic_name_max_len = std::env::var("NEKO_MESSAGE_PLANE_TOPIC_NAME_MAX_LEN")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(128);
    let payload_max_bytes = std::env::var("NEKO_MESSAGE_PLANE_PAYLOAD_MAX_BYTES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(262144);

    let store = mp_get_str(args, "store").unwrap_or("messages");
    let topic = mp_get_str(args, "topic").unwrap_or("");
    if topic.is_empty() {
        return rpc_err(req_id, "BAD_ARGS", "topic is required", None);
    }
    if topic.len() > topic_name_max_len {
        return rpc_err(req_id, "BAD_ARGS", "topic too long", None);
    }

    let payload = mp_get(args, "payload").cloned().unwrap_or(MpValue::Nil);
    let payload_bytes = rmp_serde::to_vec_named(&payload).unwrap_or_default();
    if payload_bytes.len() > payload_max_bytes {
        return rpc_err(req_id, "BAD_ARGS", "payload too large", None);
    }

    let payload_json = match mp_to_json(&payload) {
        Some(j) => j,
        None => return rpc_err(req_id, "BAD_ARGS", "invalid payload", None),
    };

    let ev = match state.store(store) {
        Some(s) => s.publish(store, topic, payload_json),
        None => return rpc_err(req_id, "BAD_STORE", "invalid store", None),
    };

    if let Some(tx) = pub_tx {
        let _ = tx.send(PubMsg {
            topic: ev.topic.as_bytes().to_vec(),
            body: rmp_serde::to_vec_named(&serde_json::json!({
                "seq": ev.seq,
                "ts": ev.ts,
                "store": ev.store,
                "topic": ev.topic,
                "payload": (*ev.payload_json).clone(),
                "index": (*ev.index_json).clone(),
            }))
            .unwrap_or_default(),
        });
    }

    let mut ev_map: Vec<(MpValue, MpValue)> = Vec::with_capacity(6);
    ev_map.push((MpValue::from("seq"), MpValue::from(ev.seq as i64)));
    ev_map.push((MpValue::from("ts"), MpValue::from(ev.ts)));
    ev_map.push((MpValue::from("store"), MpValue::from(ev.store.as_str())));
    ev_map.push((MpValue::from("topic"), MpValue::from(ev.topic.as_str())));
    ev_map.push((MpValue::from("payload"), (*ev.payload_mp).clone()));
    ev_map.push((MpValue::from("index"), (*ev.index_mp).clone()));

    rpc_ok(
        req_id,
        RpcPublishResult {
            accepted: true,
            event: MpValue::Map(ev_map),
        },
    )
}

/// Convert events to MessagePack value vector
fn events_to_mp_vec(items: &[Event], light: bool) -> Vec<MpValue> {
    let mut out_items: Vec<MpValue> = Vec::with_capacity(items.len());
    for ev in items {
        let mut m: Vec<(MpValue, MpValue)> = Vec::with_capacity(if light { 5 } else { 6 });
        m.push((MpValue::from("seq"), MpValue::from(ev.seq as i64)));
        m.push((MpValue::from("ts"), MpValue::from(ev.ts)));
        m.push((MpValue::from("store"), MpValue::from(ev.store.as_str())));
        m.push((MpValue::from("topic"), MpValue::from(ev.topic.as_str())));
        if !light {
            m.push((MpValue::from("payload"), (*ev.payload_mp).clone()));
        }
        m.push((MpValue::from("index"), (*ev.index_mp).clone()));
        out_items.push(MpValue::Map(m));
    }
    out_items
}

/// Handle RPC request in JSON format
pub fn handle_rpc(
    req: &JsonValue,
    state: &Arc<MpState>,
    pub_tx: Option<&mpsc::Sender<PubMsg>>,
) -> JsonValue {
    let req_obj = match json_obj(req) {
        Some(o) => o,
        None => {
            return serde_json::json!({"v":1,"req_id":"","ok":false,"result":null,"error":{"code":"BAD_REQ","message":"invalid request","details":null}})
        }
    };

    let v_raw = req_obj.get("v");
    let req_id = req_obj
        .get("req_id")
        .and_then(|x| x.as_str())
        .unwrap_or("");
    let op = req_obj.get("op").and_then(|x| x.as_str()).unwrap_or("");
    let args = req_obj
        .get("args")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let args_obj = args.as_object().cloned().unwrap_or_default();

    let mode = std::env::var("NEKO_MESSAGE_PLANE_VALIDATE_MODE")
        .unwrap_or_else(|_| "strict".to_string())
        .to_lowercase();

    let topic_name_max_len = std::env::var("NEKO_MESSAGE_PLANE_TOPIC_NAME_MAX_LEN")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(128);
    let topic_max = std::env::var("NEKO_MESSAGE_PLANE_TOPIC_MAX")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(2000);
    let payload_max_bytes = std::env::var("NEKO_MESSAGE_PLANE_PAYLOAD_MAX_BYTES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(256 * 1024);
    let validate_payload_bytes = std::env::var("NEKO_MESSAGE_PLANE_VALIDATE_PAYLOAD_BYTES")
        .ok()
        .map(|s| matches!(s.to_lowercase().as_str(), "true" | "1" | "yes" | "on"))
        .unwrap_or(true);

    let v = match (&*mode, v_raw) {
        ("off", Some(vv)) => vv.as_i64().unwrap_or(1),
        ("off", None) => 1,
        ("warn", Some(vv)) => vv.as_i64().unwrap_or(1),
        ("warn", None) => {
            log::warn!("[message_plane] rpc envelope missing protocol version (v)");
            1
        }
        ("strict", Some(vv)) => vv.as_i64().unwrap_or(-1),
        ("strict", None) => {
            return serde_json::json!({"v":1,"req_id":req_id,"ok":false,"result":null,"error":{"code":"BAD_VERSION","message":"missing protocol version","details":null}});
        }
        (_, Some(vv)) => vv.as_i64().unwrap_or(1),
        (_, None) => 1,
    };

    if v != 1 {
        if mode == "warn" {
            log::warn!(
                "[message_plane] rpc envelope unsupported protocol version: {}",
                v
            );
        }
        return serde_json::json!({"v":1,"req_id":req_id,"ok":false,"result":null,"error":{"code":"BAD_VERSION","message":format!("unsupported protocol version: {}", v),"details":null}});
    }

    if op == "ping" || op == "health" {
        return serde_json::json!({"v":1,"req_id":req_id,"ok":true,"result":{"ok":true,"ts": now_ts()},"error":null});
    }

    if op == "bus.get_recent" {
        let store = args_obj
            .get("store")
            .and_then(|x| x.as_str())
            .unwrap_or("messages");
        let topic = args_obj
            .get("topic")
            .and_then(|x| x.as_str())
            .unwrap_or("all");
        let mut limit = args_obj
            .get("limit")
            .and_then(|x| x.as_u64())
            .unwrap_or(200) as usize;
        let max_limit = std::env::var("NEKO_MESSAGE_PLANE_GET_RECENT_MAX_LIMIT")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1000);
        if limit > max_limit {
            limit = max_limit;
        }
        let light = args_obj
            .get("light")
            .and_then(|x| x.as_bool())
            .unwrap_or(false);

        let items = match state.store(store) {
            Some(s) => s.get_recent("", topic, limit),
            None => {
                return serde_json::json!({"v":1,"req_id":req_id,"ok":false,"result":null,"error":{"code":"BAD_STORE","message":"invalid store","details":null}});
            }
        };

        let out_items: Vec<JsonValue> = items
            .into_iter()
            .map(|ev| {
                if light {
                    serde_json::json!({
                        "seq": ev.seq,
                        "ts": ev.ts,
                        "store": ev.store,
                        "topic": ev.topic,
                        "index": (*ev.index_json).clone(),
                    })
                } else {
                    serde_json::json!({
                        "seq": ev.seq,
                        "ts": ev.ts,
                        "store": ev.store,
                        "topic": ev.topic,
                        "payload": (*ev.payload_json).clone(),
                        "index": (*ev.index_json).clone(),
                    })
                }
            })
            .collect();

        return serde_json::json!({"v":1,"req_id":req_id,"ok":true,"result":{"store":store,"topic":topic,"items":out_items,"light":light},"error":null});
    }

    if op == "bus.publish" {
        let store = args_obj
            .get("store")
            .and_then(|x| x.as_str())
            .unwrap_or("messages");
        let topic = args_obj
            .get("topic")
            .and_then(|x| x.as_str())
            .unwrap_or("");
        if topic.is_empty() {
            return serde_json::json!({"v":1,"req_id":req_id,"ok":false,"result":null,"error":{"code":"BAD_ARGS","message":"topic is required","details":null}});
        }
        if topic.len() > topic_name_max_len {
            return serde_json::json!({"v":1,"req_id":req_id,"ok":false,"result":null,"error":{"code":"BAD_ARGS","message":"topic too long","details":null}});
        }

        let mut payload = args_obj.get("payload").cloned().unwrap_or(JsonValue::Null);
        if !payload.is_object() {
            payload = serde_json::json!({"value": payload});
        }
        if validate_payload_bytes {
            match rmp_serde::to_vec_named(&payload) {
                Ok(b) => {
                    if b.len() > payload_max_bytes {
                        return serde_json::json!({"v":1,"req_id":req_id,"ok":false,"result":null,"error":{"code":"BAD_ARGS","message":"payload too large","details":null}});
                    }
                }
                Err(_) => {
                    return serde_json::json!({"v":1,"req_id":req_id,"ok":false,"result":null,"error":{"code":"BAD_ARGS","message":"payload not serializable","details":null}});
                }
            }
        }

        let ev = match state.store(store) {
            Some(s) => {
                let is_new_topic = !s.meta.contains_key(topic);
                if is_new_topic && s.meta.len() >= topic_max {
                    return serde_json::json!({"v":1,"req_id":req_id,"ok":false,"result":null,"error":{"code":"BAD_ARGS","message":"too many topics","details":null}});
                }
                s.publish(store, topic, payload)
            }
            None => {
                return serde_json::json!({"v":1,"req_id":req_id,"ok":false,"result":null,"error":{"code":"BAD_STORE","message":"invalid store","details":null}});
            }
        };

        // Publish to pub socket via the pub thread.
        if let Some(tx) = pub_tx {
            if std::env::var("NEKO_MESSAGE_PLANE_PUB_ENABLED")
                .ok()
                .map(|s| matches!(s.to_lowercase().as_str(), "true" | "1" | "yes" | "on"))
                .unwrap_or(true)
            {
                let topic_bytes = format!("{}.{}", ev.store, ev.topic).as_bytes().to_vec();
                let body = serde_json::to_vec(&serde_json::json!({
                    "seq": ev.seq,
                    "ts": ev.ts,
                    "store": ev.store,
                    "topic": ev.topic,
                    "payload": (*ev.payload_json).clone(),
                    "index": (*ev.index_json).clone(),
                }))
                .unwrap_or_default();
                let _ = tx.send(PubMsg { topic: topic_bytes, body });
            }
        }

        return serde_json::json!({"v":1,"req_id":req_id,"ok":true,"result":{"accepted":true,"event":{
            "seq": ev.seq,
            "ts": ev.ts,
            "store": ev.store,
            "topic": ev.topic,
            "payload": (*ev.payload_json).clone(),
            "index": (*ev.index_json).clone()
        }},"error":null});
    }

    serde_json::json!({"v":1,"req_id":req_id,"ok":false,"result":null,"error":{"code":"UNKNOWN_OP","message":format!("unknown op: {}", op),"details":null}})
}
