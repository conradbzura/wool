//! Caller-side rustification (prototype): a pyo3 tonic gRPC *client* with a
//! tokio<->asyncio bridge (pyo3-async-runtimes). Python builds + pickles the Task
//! and picks the worker; Rust owns the transport, frame serdes, the dispatch-stream
//! FSM, and channel pooling. `await wool_client_rs.dispatch(addr, task_bytes)`
//! returns (kind, payload): kind 0 = result, 1 = exception (both pickled bytes).
//! Coroutine dispatch only (streaming is a follow-up). No chain-manifest context
//! yet (shapebench routines use no contextvars).

use std::collections::HashMap;
use std::sync::OnceLock;

use prost::Message as _;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::Request as TRequest;

pub mod wire {
    tonic::include_proto!("wool.runtime.protobuf.wire");
}
use wire::worker_client::WorkerClient;
use wire::{request, response, Request as WRequest, Task, Void};

static CHANNELS: OnceLock<Mutex<HashMap<String, Channel>>> = OnceLock::new();

fn channels() -> &'static Mutex<HashMap<String, Channel>> {
    CHANNELS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Pooled tonic channel per worker address (connection reuse, like wool's
/// _channel_pool). Channel clones are cheap handles onto the same connection.
async fn get_channel(addr: &str) -> Result<Channel, String> {
    if let Some(ch) = channels().lock().await.get(addr) {
        return Ok(ch.clone());
    }
    let ch = Channel::from_shared(format!("http://{}", addr))
        .map_err(|e| e.to_string())?
        .connect()
        .await
        .map_err(|e| e.to_string())?;
    channels().lock().await.insert(addr.to_string(), ch.clone());
    Ok(ch)
}

async fn do_dispatch(addr: String, task_bytes: Vec<u8>) -> Result<(u8, Vec<u8>), String> {
    let dbg = std::env::var("WOOL_CLIENT_DEBUG").is_ok();
    let ch = get_channel(&addr).await?;
    if dbg { eprintln!("[rs] channel ready"); }
    let mut client = WorkerClient::new(ch);
    let task = Task::decode(&task_bytes[..]).map_err(|e| e.to_string())?;

    let (tx, rx) = mpsc::channel::<WRequest>(4);
    // Pre-load the Task so the outbound stream has the first frame before the
    // server starts reading (grpc.aio may not surface the call until a frame
    // arrives).
    let _ = tx
        .send(WRequest {
            payload: Some(request::Payload::Task(task)),
            context: None,
        })
        .await;
    let outbound = ReceiverStream::new(rx);
    if dbg { eprintln!("[rs] calling dispatch"); }
    let resp = client
        .dispatch(TRequest::new(outbound))
        .await
        .map_err(|e| e.to_string())?;
    if dbg { eprintln!("[rs] dispatch returned; reading ack"); }
    let mut inbound = resp.into_inner();

    // Ack
    let _ack = inbound.message().await.map_err(|e| e.to_string())?;
    if dbg { eprintln!("[rs] got ack; sending next"); }
    // Next (coroutine prime), then close the outbound side (coroutine: no more
    // frames). Dropping tx signals done-writing so the stream can finish cleanly.
    let _ = tx
        .send(WRequest {
            payload: Some(request::Payload::Next(Void {})),
            context: None,
        })
        .await;
    drop(tx);
    // Result / Exception
    let msg = inbound.message().await.map_err(|e| e.to_string())?;
    let out = match msg.and_then(|r| r.payload) {
        Some(response::Payload::Result(m)) => (0u8, m.dump),
        Some(response::Payload::Exception(m)) => (1u8, m.dump),
        Some(response::Payload::Nack(n)) => (1u8, n.exception.map(|e| e.dump).unwrap_or_default()),
        _ => return Err("unexpected/empty response from worker".into()),
    };
    if dbg { eprintln!("[rs] got result; draining"); }
    // Drain to EOF so the HTTP/2 stream closes cleanly (else streams leak and
    // grpc.aio eventually refuses new ones with CANCELLED).
    while inbound.message().await.map_err(|e| e.to_string())?.is_some() {}
    Ok(out)
}

#[pyfunction]
fn dispatch(py: Python<'_>, addr: String, task_bytes: Vec<u8>) -> PyResult<Bound<'_, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let (kind, payload) = do_dispatch(addr, task_bytes)
            .await
            .map_err(PyRuntimeError::new_err)?;
        Python::with_gil(|py| {
            let b = PyBytes::new(py, &payload);
            Ok((kind, b.unbind()).into_pyobject(py)?.unbind())
        })
    })
}

/// Trivial bridge probe: returns 42 after a tokio sleep. Isolates whether the
/// tokio<->asyncio bridge itself works, independent of tonic.
#[pyfunction]
fn ping(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        Ok(42u32)
    })
}

#[pymodule]
fn wool_client_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(dispatch, m)?)?;
    m.add_function(wrap_pyfunction!(ping, m)?)?;
    Ok(())
}
