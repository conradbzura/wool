//! Native Rust gRPC (tonic) bidi echo — the transport ceiling, directly
//! comparable to the grpc-aio probes. Measures per-CALL (fresh stream each
//! iteration) and warm PERSISTENT-stream per-exchange RTT over TCP loopback.

use std::pin::Pin;
use std::time::Instant;

use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

pub mod echo {
    tonic::include_proto!("echo");
}
use echo::echo_client::EchoClient;
use echo::echo_server::{Echo, EchoServer};
use echo::Msg;

#[derive(Default)]
struct EchoSvc;

#[tonic::async_trait]
impl Echo for EchoSvc {
    type ExchangeStream = Pin<Box<dyn Stream<Item = Result<Msg, Status>> + Send>>;

    async fn exchange(
        &self,
        req: Request<Streaming<Msg>>,
    ) -> Result<Response<Self::ExchangeStream>, Status> {
        let mut inbound = req.into_inner();
        let out = async_stream::try_stream! {
            while let Some(msg) = inbound.message().await? {
                yield msg;
            }
        };
        Ok(Response::new(Box::pin(out)))
    }
}

fn summarize(mut times: Vec<f64>) -> (f64, f64) {
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (times[0], times[times.len() / 2])
}

async fn bench_warm(client: &mut EchoClient<tonic::transport::Channel>) {
    let (tx, rx) = mpsc::channel::<Msg>(2);
    let outbound = ReceiverStream::new(rx);
    let resp = client.exchange(Request::new(outbound)).await.unwrap();
    let mut inbound = resp.into_inner();

    let msg = || Msg { data: Vec::new() };
    for _ in 0..500 {
        tx.send(msg()).await.unwrap();
        inbound.next().await.unwrap().unwrap();
    }
    let mut times = Vec::new();
    let n = 2000;
    for _ in 0..7 {
        let t0 = Instant::now();
        for _ in 0..n {
            tx.send(msg()).await.unwrap();
            inbound.next().await.unwrap().unwrap();
        }
        times.push(t0.elapsed().as_nanos() as f64 / n as f64);
    }
    let (mn, md) = summarize(times);
    println!("tonic warm PERSISTENT-stream per-exchange: min={:.0}ns median={:.0}ns", mn, md);
}

async fn bench_percall(client: &mut EchoClient<tonic::transport::Channel>) {
    let one = |client: &mut EchoClient<tonic::transport::Channel>| {
        let mut c = client.clone();
        async move {
            let (tx, rx) = mpsc::channel::<Msg>(2);
            let outbound = ReceiverStream::new(rx);
            let resp = c.exchange(Request::new(outbound)).await.unwrap();
            let mut inbound = resp.into_inner();
            tx.send(Msg { data: Vec::new() }).await.unwrap();
            inbound.next().await.unwrap().unwrap();
            drop(tx);
        }
    };
    for _ in 0..300 {
        one(client).await;
    }
    let mut times = Vec::new();
    let n = 1000;
    for _ in 0..7 {
        let t0 = Instant::now();
        for _ in 0..n {
            one(client).await;
        }
        times.push(t0.elapsed().as_nanos() as f64 / n as f64);
    }
    let (mn, md) = summarize(times);
    println!("tonic per-CALL 1-exchange (fresh stream): min={:.0}ns median={:.0}ns", mn, md);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50077".parse()?;
    tokio::spawn(async move {
        Server::builder()
            .add_service(EchoServer::new(EchoSvc))
            .serve(addr)
            .await
            .unwrap();
    });
    // let the server bind
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    let mut client = EchoClient::connect("http://127.0.0.1:50077").await?;
    bench_warm(&mut client).await;
    bench_percall(&mut client).await;
    Ok(())
}
