use rmpv::Value as MpValue;
use serde_json::Value as JsonValue;
use std::io::Cursor;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_ts() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

pub fn json_obj(v: &JsonValue) -> Option<&serde_json::Map<String, JsonValue>> {
    v.as_object()
}

#[allow(dead_code)]
pub fn encode_msgpack(v: &JsonValue) -> Vec<u8> {
    rmp_serde::to_vec_named(v).unwrap_or_else(|_| {
        rmp_serde::to_vec_named(&serde_json::json!({"ok":false,"error":"encode"})).unwrap()
    })
}

pub fn decode_msgpack(bytes: &[u8]) -> Option<JsonValue> {
    rmp_serde::from_slice::<JsonValue>(bytes).ok()
}

pub fn decode_msgpack_value(bytes: &[u8]) -> Option<MpValue> {
    let mut cur = Cursor::new(bytes);
    rmpv::decode::read_value(&mut cur).ok()
}

pub fn decode_json(bytes: &[u8]) -> Option<JsonValue> {
    serde_json::from_slice::<JsonValue>(bytes).ok()
}

pub fn mp_get<'a>(m: &'a MpValue, key: &str) -> Option<&'a MpValue> {
    let mm = m.as_map()?;
    for (k, v) in mm.iter() {
        if let Some(ks) = k.as_str() {
            if ks == key {
                return Some(v);
            }
        }
    }
    None
}

pub fn mp_get_str<'a>(m: &'a MpValue, key: &str) -> Option<&'a str> {
    mp_get(m, key).and_then(|v| v.as_str())
}

#[allow(dead_code)]
pub fn mp_get_i64(m: &MpValue, key: &str) -> Option<i64> {
    mp_get(m, key).and_then(|v| v.as_i64().or_else(|| v.as_u64().map(|x| x as i64)))
}

#[allow(dead_code)]
pub fn mp_get_bool(m: &MpValue, key: &str) -> Option<bool> {
    mp_get(m, key).and_then(|v| v.as_bool())
}

#[allow(dead_code)]
pub fn mp_map_to_json(v: &MpValue) -> Option<JsonValue> {
    rmpv::ext::from_value::<JsonValue>(v.clone()).ok()
}

pub fn mp_to_json(v: &MpValue) -> Option<JsonValue> {
    rmpv::ext::from_value::<JsonValue>(v.clone()).ok()
}

pub fn extract_index(payload: &JsonValue, default_ts: f64) -> JsonValue {
    let obj = match payload.as_object() {
        Some(o) => o,
        None => {
            return serde_json::json!({
                "plugin_id": JsonValue::Null,
                "source": JsonValue::Null,
                "priority": 0,
                "kind": JsonValue::Null,
                "type": JsonValue::Null,
                "timestamp": default_ts,
                "id": JsonValue::Null
            })
        }
    };

    let plugin_id = obj
        .get("plugin_id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    let source = obj
        .get("source")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());

    let priority = match obj.get("priority") {
        Some(v) if v.is_number() => v.as_i64().unwrap_or(0),
        Some(v) if v.is_string() => v
            .as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0),
        _ => 0,
    };

    let kind = obj
        .get("kind")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    let mut type_ = obj
        .get("type")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    if type_.is_none() {
        type_ = obj
            .get("message_type")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());
    }

    let ts_raw = obj.get("timestamp").or_else(|| obj.get("time"));
    let ts = match ts_raw {
        Some(v) if v.is_number() => v.as_f64().unwrap_or(default_ts),
        Some(v) if v.is_string() => v
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(default_ts),
        _ => default_ts,
    };

    let mut record_id: Option<&str> = None;
    for k in [
        "message_id",
        "event_id",
        "lifecycle_id",
        "id",
        "task_id",
        "run_id",
    ] {
        if let Some(v) = obj.get(k).and_then(|v| v.as_str()).filter(|s| !s.is_empty()) {
            record_id = Some(v);
            break;
        }
    }

    serde_json::json!({
        "plugin_id": plugin_id,
        "source": source,
        "priority": priority,
        "kind": kind,
        "type": type_,
        "timestamp": ts,
        "id": record_id,
    })
}
