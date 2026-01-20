use rmpv::Value as MpValue;
use serde::Serialize;

#[derive(Serialize)]
pub struct RpcError {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<MpValue>,
}

#[derive(Serialize)]
pub struct RpcEnvelope<T: Serialize> {
    pub v: i32,
    pub req_id: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

pub fn rpc_ok<T: Serialize>(req_id: &str, result: T) -> Vec<u8> {
    rmp_serde::to_vec_named(&RpcEnvelope {
        v: 1,
        req_id: req_id.to_string(),
        ok: true,
        result: Some(result),
        error: None,
    })
    .unwrap_or_default()
}

pub fn rpc_err(req_id: &str, code: &str, message: &str, details: Option<MpValue>) -> Vec<u8> {
    rmp_serde::to_vec_named(&RpcEnvelope::<MpValue> {
        v: 1,
        req_id: req_id.to_string(),
        ok: false,
        result: None,
        error: Some(RpcError {
            code: code.to_string(),
            message: message.to_string(),
            details,
        }),
    })
    .unwrap_or_default()
}

#[derive(Serialize)]
pub struct RpcHealthResult {
    pub ok: bool,
    pub ts: f64,
}

/// Lightweight event view for serialization without cloning MpValue
#[derive(Serialize)]
pub struct EventView<'a> {
    pub seq: i64,
    pub ts: f64,
    pub store: &'a str,
    pub topic: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<&'a MpValue>,
    pub index: &'a MpValue,
}

#[derive(Serialize)]
pub struct RpcGetRecentResult<'a> {
    pub store: String,
    pub topic: String,
    pub items: Vec<EventView<'a>>,
    pub light: bool,
}

#[derive(Serialize)]
pub struct RpcReplayResult<'a> {
    pub store: String,
    pub items: Vec<EventView<'a>>,
    pub light: bool,
}

#[derive(Serialize)]
pub struct RpcQueryResult {
    pub store: String,
    pub topic: String,
    pub items: Vec<MpValue>,
    pub light: bool,
}

#[derive(Serialize)]
pub struct RpcPublishResult {
    pub accepted: bool,
    pub event: MpValue,
}

#[derive(Serialize)]
pub struct RpcGetSinceResult {
    pub store: String,
    pub topic: String,
    pub items: Vec<MpValue>,
    pub after_seq: u64,
}
