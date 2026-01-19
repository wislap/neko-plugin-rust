use regex::Regex;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};

use crate::types::{Event, Store};

pub fn dedupe_key(ev: &Event) -> (String, String) {
    if let Some(idv) = ev
        .index_json
        .as_ref()
        .as_object()
        .and_then(|o| o.get("id"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
    {
        return ("id".to_string(), idv.to_string());
    }
    ("seq".to_string(), ev.seq.to_string())
}

pub fn field_value(ev: &Event, field: &str) -> Option<JsonValue> {
    if let Some(idx) = ev.index_json.as_ref().as_object() {
        if let Some(v) = idx.get(field) {
            return Some(v.clone());
        }
    }
    if let Some(p) = ev.payload_json.as_ref().as_object() {
        if let Some(v) = p.get(field) {
            return Some(v.clone());
        }
    }
    match field {
        "seq" => Some(JsonValue::from(ev.seq)),
        "ts" => Some(JsonValue::from(ev.ts)),
        "store" => Some(JsonValue::from(ev.store.clone())),
        "topic" => Some(JsonValue::from(ev.topic.clone())),
        _ => None,
    }
}

fn cmp_sort_value(v: &JsonValue) -> (i32, String) {
    if v.is_null() {
        return (2, "".to_string());
    }
    if let Some(n) = v.as_f64() {
        return (0, n.to_string());
    }
    (1, v.as_str().unwrap_or(&v.to_string()).to_string())
}

fn maybe_match_regex(pattern: &str, value: Option<&JsonValue>, strict: bool) -> Option<bool> {
    if pattern.is_empty() {
        return None;
    }
    if pattern.len() > 128 {
        return if strict { Some(false) } else { None };
    }
    let s = match value {
        Some(v) => {
            if let Some(ss) = v.as_str() {
                ss.to_string()
            } else {
                v.to_string()
            }
        }
        None => return Some(false),
    };
    let text = if s.len() > 1024 { &s[..1024] } else { &s };
    let re = match Regex::new(pattern) {
        Ok(r) => r,
        Err(_) => {
            return if strict { Some(false) } else { None };
        }
    };
    Some(re.is_match(text))
}

pub fn apply_unary_op(
    items: Vec<Event>,
    op: &str,
    params: &serde_json::Map<String, JsonValue>,
) -> Option<Vec<Event>> {
    if op == "limit" {
        let n = params.get("n").and_then(|v| v.as_i64()).unwrap_or(0);
        if n <= 0 {
            return Some(vec![]);
        }
        let mut out = items;
        if out.len() > n as usize {
            out.truncate(n as usize);
        }
        return Some(out);
    }

    if op == "sort" {
        let by = params.get("by");
        let by_fields: Vec<String> = match by {
            None => vec!["timestamp".into(), "created_at".into(), "time".into()],
            Some(v) if v.is_string() => vec![v.as_str().unwrap_or("").to_string()],
            Some(v) if v.is_array() => {
                let arr = v.as_array().cloned().unwrap_or_default();
                arr.iter()
                    .map(|x| x.as_str().unwrap_or(&x.to_string()).to_string())
                    .collect()
            }
            _ => vec!["timestamp".into(), "created_at".into(), "time".into()],
        };
        let reverse = params
            .get("reverse")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let mut out = items;
        out.sort_by(|a, b| {
            let mut ka: Vec<(i32, String)> = Vec::new();
            let mut kb: Vec<(i32, String)> = Vec::new();
            for f in by_fields.iter() {
                ka.push(cmp_sort_value(
                    &field_value(a, f).unwrap_or(JsonValue::Null),
                ));
                kb.push(cmp_sort_value(
                    &field_value(b, f).unwrap_or(JsonValue::Null),
                ));
            }
            if reverse {
                kb.cmp(&ka)
            } else {
                ka.cmp(&kb)
            }
        });
        return Some(out);
    }

    if op == "filter" {
        let mut p: HashMap<String, JsonValue> =
            params.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let strict = p
            .remove("strict")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        if let Some(flt) = p.get("flt").and_then(|v| v.as_object()).cloned() {
            for (k, v) in flt.iter() {
                p.insert(k.clone(), v.clone());
            }
        }

        let mut out: Vec<Event> = Vec::new();
        for ev in items.into_iter() {
            // equality checks
            let mut ok = true;
            for k in ["plugin_id", "source", "kind", "type"] {
                if let Some(v) = p.get(k) {
                    let got = field_value(&ev, k);
                    if got.as_ref() != Some(v) {
                        ok = false;
                        break;
                    }
                }
            }
            if !ok {
                continue;
            }

            if let Some(pmin) = p.get("priority_min") {
                let pmin_i = pmin
                    .as_i64()
                    .or_else(|| pmin.as_str().and_then(|s| s.parse::<i64>().ok()));
                if let Some(pmin_i) = pmin_i {
                    let pri = field_value(&ev, "priority")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    if pri < pmin_i {
                        continue;
                    }
                } else if strict {
                    continue;
                }
            }

            if let Some(since) = p.get("since_ts") {
                let s_ts = since
                    .as_f64()
                    .or_else(|| since.as_str().and_then(|s| s.parse::<f64>().ok()));
                if let Some(s_ts) = s_ts {
                    let ts = field_value(&ev, "timestamp")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                    if ts < s_ts {
                        continue;
                    }
                } else if strict {
                    continue;
                }
            }

            if let Some(until) = p.get("until_ts") {
                let u_ts = until
                    .as_f64()
                    .or_else(|| until.as_str().and_then(|s| s.parse::<f64>().ok()));
                if let Some(u_ts) = u_ts {
                    let ts = field_value(&ev, "timestamp")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                    if ts > u_ts {
                        continue;
                    }
                } else if strict {
                    continue;
                }
            }

            for (prefix, key) in [
                ("plugin_id", "plugin_id"),
                ("source", "source"),
                ("kind", "kind"),
                ("type", "type"),
            ] {
                let pat_key = format!("{}_re", prefix);
                if let Some(pat) = p
                    .get(&pat_key)
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                {
                    let got = field_value(&ev, key);
                    let verdict = maybe_match_regex(pat, got.as_ref(), strict);
                    if let Some(false) = verdict {
                        ok = false;
                        break;
                    }
                }
            }
            if !ok {
                continue;
            }

            if let Some(pat) = p
                .get("content_re")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
            {
                let got = if let Some(obj) = ev.payload_json.as_ref().as_object() {
                    obj.get("content").cloned()
                } else {
                    None
                };
                let verdict = maybe_match_regex(pat, got.as_ref(), strict);
                if let Some(false) = verdict {
                    continue;
                }
            }

            out.push(ev);
        }
        return Some(out);
    }

    // where_* family
    if op == "where_eq" {
        let field = params
            .get("field")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let value = params.get("value");
        if field.is_empty() {
            return Some(items);
        }
        let mut out = Vec::new();
        for ev in items {
            let got = field_value(&ev, &field);
            if got.as_ref() == value {
                out.push(ev);
            }
        }
        return Some(out);
    }

    if op == "where_in" {
        let field = params
            .get("field")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let values = params.get("values").and_then(|v| v.as_array());
        if field.is_empty() || values.is_none() {
            return Some(items);
        }
        let set: HashSet<String> = values
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap_or(&v.to_string()).to_string())
            .collect();
        let mut out = Vec::new();
        for ev in items {
            let got = field_value(&ev, &field).unwrap_or(JsonValue::Null);
            let k = got.as_str().unwrap_or(&got.to_string()).to_string();
            if set.contains(&k) {
                out.push(ev);
            }
        }
        return Some(out);
    }

    if op == "where_contains" {
        let field = params
            .get("field")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let value = params
            .get("value")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if field.is_empty() || value.is_empty() {
            return Some(items);
        }
        let mut out = Vec::new();
        for ev in items {
            let got = field_value(&ev, &field).unwrap_or(JsonValue::Null);
            if got.as_str().unwrap_or(&got.to_string()).contains(value) {
                out.push(ev);
            }
        }
        return Some(out);
    }

    if op == "where_regex" {
        let field = params
            .get("field")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let pattern = params
            .get("pattern")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let strict = params
            .get("strict")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        if field.is_empty() || pattern.is_empty() {
            return Some(items);
        }
        // Validate pattern once
        let ok_pat = maybe_match_regex(pattern, Some(&JsonValue::String("".to_string())), strict);
        if ok_pat == Some(false) {
            return Some(if strict { vec![] } else { items });
        }
        if ok_pat.is_none() {
            return Some(items);
        }
        let mut out = Vec::new();
        for ev in items {
            let got = field_value(&ev, &field);
            let verdict = maybe_match_regex(pattern, got.as_ref(), strict);
            if verdict == Some(true) {
                out.push(ev);
            }
        }
        return Some(out);
    }

    None
}

pub fn apply_binary_op(left: Vec<Event>, right: Vec<Event>, op: &str) -> Option<Vec<Event>> {
    if op != "merge" && op != "intersection" && op != "difference" {
        return None;
    }
    let right_keys: Vec<(String, String)> = right.iter().map(dedupe_key).collect();
    let set_right: HashSet<(String, String)> = right_keys.into_iter().collect();

    if op == "merge" {
        let mut merged: Vec<Event> = Vec::new();
        let mut seen: HashSet<(String, String)> = HashSet::new();
        for ev in left.into_iter().chain(right.into_iter()) {
            let k = dedupe_key(&ev);
            if seen.contains(&k) {
                continue;
            }
            seen.insert(k);
            merged.push(ev);
        }
        merged.sort_by(|a, b| b.seq.cmp(&a.seq));
        return Some(merged);
    }

    if op == "intersection" {
        let mut kept: Vec<Event> = Vec::new();
        let mut seen: HashSet<(String, String)> = HashSet::new();
        for ev in left.into_iter() {
            let k = dedupe_key(&ev);
            if seen.contains(&k) {
                continue;
            }
            if !set_right.contains(&k) {
                continue;
            }
            seen.insert(k);
            kept.push(ev);
        }
        kept.sort_by(|a, b| b.seq.cmp(&a.seq));
        return Some(kept);
    }

    if op == "difference" {
        let mut kept: Vec<Event> = Vec::new();
        let mut seen: HashSet<(String, String)> = HashSet::new();
        for ev in left.into_iter() {
            let k = dedupe_key(&ev);
            if seen.contains(&k) {
                continue;
            }
            if set_right.contains(&k) {
                continue;
            }
            seen.insert(k);
            kept.push(ev);
        }
        kept.sort_by(|a, b| b.seq.cmp(&a.seq));
        return Some(kept);
    }

    None
}

pub fn eval_plan(store: &Store, node: &JsonValue) -> Option<Vec<Event>> {
    let obj = node.as_object()?;
    let kind = obj.get("kind")?.as_str().unwrap_or("");
    let op = obj.get("op").and_then(|v| v.as_str()).unwrap_or("");
    let params = obj
        .get("params")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();

    if kind == "get" {
        let p = params
            .get("params")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();

        let max_count = p
            .get("max_count")
            .or_else(|| p.get("limit"))
            .and_then(|v| v.as_i64())
            .unwrap_or(200);
        let max_limit = std::env::var("NEKO_MESSAGE_PLANE_GET_RECENT_MAX_LIMIT")
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(1000);
        let mut limit_i = max_count;
        if limit_i > max_limit {
            limit_i = max_limit;
        }
        if limit_i <= 0 {
            limit_i = 200;
        }

        let topic = p.get("topic").and_then(|v| v.as_str()).unwrap_or("all");
        let pid = p
            .get("plugin_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());
        let src = p
            .get("source")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());
        let kd = p
            .get("kind")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());
        let tp = p
            .get("type")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());
        let pmin = p.get("priority_min").and_then(|v| v.as_i64());
        let since_ts = p.get("since_ts").and_then(|v| v.as_f64());

        if pid.is_none()
            && src.is_none()
            && kd.is_none()
            && tp.is_none()
            && pmin.is_none()
            && since_ts.is_none()
        {
            return Some(store.get_recent("", topic, limit_i as usize));
        }

        // Use existing query behavior over a single topic
        let mut snapshots: Vec<Event> = Vec::new();
        if let Some(dq_arc) = store.topics.get(topic) {
            let dq = dq_arc.read();
            snapshots.extend(dq.iter().cloned());
        }
        let mut out: Vec<Event> = Vec::new();
        for ev in snapshots {
            let idx = match ev.index_json.as_ref().as_object() {
                Some(o) => o,
                None => continue,
            };
            if let Some(pid) = pid {
                if idx.get("plugin_id").and_then(|v| v.as_str()) != Some(pid) {
                    continue;
                }
            }
            if let Some(src) = src {
                if idx.get("source").and_then(|v| v.as_str()) != Some(src) {
                    continue;
                }
            }
            if let Some(kd) = kd {
                if idx.get("kind").and_then(|v| v.as_str()) != Some(kd) {
                    continue;
                }
            }
            if let Some(tp) = tp {
                if idx.get("type").and_then(|v| v.as_str()) != Some(tp) {
                    continue;
                }
            }
            if let Some(pmin) = pmin {
                let pri = idx.get("priority").and_then(|v| v.as_i64()).unwrap_or(0);
                if pri < pmin {
                    continue;
                }
            }
            if let Some(s_ts) = since_ts {
                let ts = idx
                    .get("timestamp")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                if ts < s_ts {
                    continue;
                }
            }
            out.push(ev);
        }
        out.sort_by(|a, b| b.seq.cmp(&a.seq));
        if out.len() > limit_i as usize {
            out.truncate(limit_i as usize);
        }
        return Some(out);
    }

    if kind == "unary" {
        let child = obj.get("child")?;
        let base = eval_plan(store, child)?;
        let out = apply_unary_op(base, op, &params)?;
        return Some(out);
    }

    if kind == "binary" {
        let left = eval_plan(store, obj.get("left")?)?;
        let right = eval_plan(store, obj.get("right")?)?;
        return apply_binary_op(left, right, op);
    }

    None
}
