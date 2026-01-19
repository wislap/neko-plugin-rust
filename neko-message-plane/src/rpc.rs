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

#[derive(Serialize)]
pub struct RpcGetRecentResult {
    pub store: String,
    pub topic: String,
    pub items: Vec<MpValue>,
    pub light: bool,
}

#[derive(Serialize)]
pub struct RpcReplayResult {
    pub store: String,
    pub items: Vec<MpValue>,
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
