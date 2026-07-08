#![allow(non_camel_case_types)]
//! Rustified worker plane (slice v2): PUSH streaming. tonic gRPC `Worker` service
//! speaking wool's wire.proto; one START per dispatch, the Python executor
//! self-drives the routine and PUSHES each yield over a unix socket keyed by
//! dispatch id; Rust forwards one push per client Next. The executor produces
//! ahead of the client, overlapping IPC with the gRPC round-trip (recovers the
//! per-yield bridge tax). Bounded channel + socket buffer give backpressure.
//! Pure-anext (Send/Throw TODO). Cancellation via RST_STREAM -> CancelGuard.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures::Stream;
use prost::Message as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tonic::transport::Server;
use tonic::{Status, Streaming};

pub mod wire {
    tonic::include_proto!("wool.runtime.protobuf.wire");
}
use wire::worker_server::{Worker, WorkerServer};
use wire::{request, response, Ack, Message, Request as WRequest, Response as WResponse, StopRequest, Void};

const VERSION: &str = "0.12.0rc0";
const OP_START: u8 = 0;
const OP_CANCEL: u8 = 4;
const ST_VALUE: u8 = 0;
const ST_EXC: u8 = 1;
// ST_STOP = 2 -> any non-VALUE/EXC ends the stream.

struct Executor {
    streams: Arc<Mutex<HashMap<u64, mpsc::Sender<(u8, Vec<u8>)>>>>,
    submit: mpsc::UnboundedSender<Vec<u8>>,
}

impl Executor {
    async fn connect(path: &str) -> std::io::Result<Arc<Self>> {
        let stream = UnixStream::connect(path).await?;
        let (mut rd, mut wr) = stream.into_split();
        let (submit_tx, mut submit_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let streams: Arc<Mutex<HashMap<u64, mpsc::Sender<(u8, Vec<u8>)>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        tokio::spawn(async move {
            while let Some(frame) = submit_rx.recv().await {
                if wr.write_all(&frame).await.is_err() {
                    break;
                }
            }
        });

        let streams_r = streams.clone();
        tokio::spawn(async move {
            let mut lenbuf = [0u8; 4];
            loop {
                if rd.read_exact(&mut lenbuf).await.is_err() {
                    break;
                }
                let total = u32::from_le_bytes(lenbuf) as usize;
                let mut buf = vec![0u8; total];
                if rd.read_exact(&mut buf).await.is_err() {
                    break;
                }
                let disp_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
                let status = buf[8];
                let plen = u32::from_le_bytes(buf[9..13].try_into().unwrap()) as usize;
                let payload = buf[13..13 + plen].to_vec();
                let sender = streams_r.lock().unwrap().get(&disp_id).cloned();
                if let Some(s) = sender {
                    // .await gives end-to-end backpressure: a full channel stalls
                    // the reader -> the socket buffer fills -> the executor blocks.
                    let _ = s.send((status, payload)).await;
                }
            }
        });

        Ok(Arc::new(Self {
            streams,
            submit: submit_tx,
        }))
    }

    fn register(&self, disp_id: u64) -> mpsc::Receiver<(u8, Vec<u8>)> {
        let (tx, rx) = mpsc::channel(16);
        self.streams.lock().unwrap().insert(disp_id, tx);
        rx
    }

    fn unregister(&self, disp_id: u64) {
        self.streams.lock().unwrap().remove(&disp_id);
    }

    fn start(&self, disp_id: u64, task_bytes: &[u8]) {
        let mut body = Vec::with_capacity(13 + task_bytes.len());
        body.extend_from_slice(&disp_id.to_le_bytes());
        body.push(OP_START);
        body.extend_from_slice(&(task_bytes.len() as u32).to_le_bytes());
        body.extend_from_slice(task_bytes);
        let mut frame = Vec::with_capacity(4 + body.len());
        frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
        frame.extend_from_slice(&body);
        let _ = self.submit.send(frame);
    }

    fn cancel(&self, disp_id: u64) {
        let mut body = Vec::with_capacity(9);
        body.extend_from_slice(&disp_id.to_le_bytes());
        body.push(OP_CANCEL);
        let mut frame = Vec::with_capacity(4 + body.len());
        frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
        frame.extend_from_slice(&body);
        let _ = self.submit.send(frame);
    }
}

/// On stream teardown (RST_STREAM / normal end): cancel the routine (no-op if
/// already done) and drop the per-dispatch channel.
struct CancelGuard {
    exec: Arc<Executor>,
    disp_id: u64,
}
impl Drop for CancelGuard {
    fn drop(&mut self) {
        self.exec.cancel(self.disp_id);
        self.exec.unregister(self.disp_id);
    }
}

struct Svc {
    exec: Arc<Executor>,
    next_disp: AtomicU64,
}

#[tonic::async_trait]
impl Worker for Svc {
    type dispatchStream = Pin<Box<dyn Stream<Item = Result<WResponse, Status>> + Send>>;

    async fn dispatch(
        &self,
        req: tonic::Request<Streaming<WRequest>>,
    ) -> Result<tonic::Response<Self::dispatchStream>, Status> {
        let mut inbound = req.into_inner();
        let exec = self.exec.clone();
        let disp_id = self.next_disp.fetch_add(1, Ordering::Relaxed);
        let mut rx = exec.register(disp_id);
        let guard = CancelGuard {
            exec: exec.clone(),
            disp_id,
        };

        let out = async_stream::try_stream! {
            let _guard = guard;
            let first = inbound
                .message()
                .await?
                .ok_or_else(|| Status::invalid_argument("empty request stream"))?;
            let task = match first.payload {
                Some(request::Payload::Task(t)) => t,
                _ => {
                    Err::<(), _>(Status::invalid_argument("expected Task first"))?;
                    unreachable!()
                }
            };
            yield WResponse {
                payload: Some(response::Payload::Ack(Ack { version: VERSION.to_string() })),
                context: None,
            };
            // Fire START with the whole opaque Task; the executor rebuilds it via
            // Task.from_protobuf + routine_scope and self-drives, pushing ahead.
            exec.start(disp_id, &task.encode_to_vec());

            // Forward one pushed item per client Next (pull-paced), until STOP.
            loop {
                let fr = match inbound.message().await? {
                    Some(f) => f,
                    None => break,
                };
                let _ = &fr; // pure-anext: the client frame is a pull signal
                match rx.recv().await {
                    Some((ST_VALUE, pl)) => {
                        yield WResponse {
                            payload: Some(response::Payload::Result(Message { dump: pl })),
                            context: None,
                        };
                    }
                    Some((ST_EXC, pl)) => {
                        yield WResponse {
                            payload: Some(response::Payload::Exception(Message { dump: pl })),
                            context: None,
                        };
                        break;
                    }
                    _ => break, // STOP or channel closed
                }
            }
        };
        Ok(tonic::Response::new(Box::pin(out)))
    }

    async fn stop(
        &self,
        _req: tonic::Request<StopRequest>,
    ) -> Result<tonic::Response<Void>, Status> {
        Ok(tonic::Response::new(Void {}))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sock = std::env::var("WOOL_EXEC_SOCK").unwrap_or_else(|_| "/tmp/wool_executor.sock".into());
    let port: u16 = std::env::var("WOOL_RUST_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50088);

    let mut exec = None;
    for _ in 0..100 {
        match Executor::connect(&sock).await {
            Ok(e) => {
                exec = Some(e);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
        }
    }
    let exec = exec.expect("could not connect to Python executor socket");

    let addr = format!("127.0.0.1:{}", port).parse()?;
    eprintln!("RUST_WORKER_READY 127.0.0.1:{}", port);
    Server::builder()
        .add_service(WorkerServer::new(Svc {
            exec,
            next_disp: AtomicU64::new(1),
        }))
        .serve(addr)
        .await?;
    Ok(())
}
