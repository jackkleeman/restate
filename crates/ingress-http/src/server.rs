// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

use crate::handler::Handler;
use codederror::CodedError;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto;
use restate_core::{cancellation_watcher, task_center, TaskKind};
use restate_ingress_dispatcher::IngressRequestSender;
use restate_schema_api::component::ComponentMetadataResolver;
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::sync::Semaphore;
use tracing::{info, warn};

pub type StartSignal = oneshot::Receiver<SocketAddr>;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum IngressServerError {
    #[error(
        "failed binding to address '{address}' specified in 'worker.ingress_http.bind_address'"
    )]
    #[code(restate_errors::RT0004)]
    Binding {
        address: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("error while running ingress http server: {0}")]
    #[code(unknown)]
    Running(#[from] hyper::Error),
}

pub struct HyperServerIngress<Schemas> {
    listening_addr: SocketAddr,
    concurrency_limit: usize,

    // Parameters to build the layers
    schemas: Schemas,
    request_tx: IngressRequestSender,

    // Signals
    start_signal_tx: oneshot::Sender<SocketAddr>,
}

impl<Schemas> HyperServerIngress<Schemas>
where
    Schemas: ComponentMetadataResolver + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(
        listening_addr: SocketAddr,
        concurrency_limit: usize,
        schemas: Schemas,
        request_tx: IngressRequestSender,
    ) -> (Self, StartSignal) {
        let (start_signal_tx, start_signal_rx) = oneshot::channel();

        let ingress = Self {
            listening_addr,
            concurrency_limit,
            schemas,
            request_tx,
            start_signal_tx,
        };

        (ingress, start_signal_rx)
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let HyperServerIngress {
            listening_addr,
            concurrency_limit,
            schemas,
            request_tx,
            start_signal_tx,
        } = self;

        // We create a TcpListener and bind it
        let listener =
            TcpListener::bind(listening_addr)
                .await
                .map_err(|err| IngressServerError::Binding {
                    address: listening_addr,
                    source: err,
                })?;
        let local_addr = listener
            .local_addr()
            .map_err(|err| IngressServerError::Binding {
                address: listening_addr,
                source: err,
            })?;

        // Prepare the handler
        let global_concurrency_limit_semaphore = Arc::new(Semaphore::new(concurrency_limit));

        let handler =
            handler::Handler::new(schemas, request_tx, global_concurrency_limit_semaphore);

        info!(
            net.host.addr = %local_addr.ip(),
            net.host.port = %local_addr.port(),
            "Ingress HTTP listening"
        );

        let shutdown = cancellation_watcher();
        tokio::pin!(shutdown);

        // Send start signal
        let _ = start_signal_tx.send(local_addr);

        // We start a loop to continuously accept incoming connections
        loop {
            tokio::select! {
                res = listener.accept() => {
                    let (stream, remote_peer) = res?;
                    Self::handle_connection(stream, remote_peer, handler.clone())?;
                }
                  _ = &mut shutdown => {
                    return Ok(());
                }
            }
        }
    }

    fn handle_connection(
        stream: TcpStream,
        remote_peer: SocketAddr,
        handler: Handler<Schemas>,
    ) -> anyhow::Result<()> {
        let connect_info = ConnectInfo::new(remote_peer);
        let io = TokioIo::new(stream);

        // Spawn a tokio task to serve the connection
        task_center().spawn(TaskKind::Ingress, "ingress", None, async move {
            let svc = service_fn(move |hyper_req| {
                let h = handler.clone();
                async move { Ok::<_, Infallible>(h.handle(connect_info, hyper_req).await) }
            });

            let shutdown = cancellation_watcher();
            let auto_connection = auto::Builder::new(TaskCenterExecutor);
            let serve_connection_fut = auto_connection.serve_connection(io, svc);

            tokio::select! {
                res = serve_connection_fut => {
                    if let Err(err) = res {
                        warn!("Error when serving the connection: {:?}", err);
                    }
                }
                _ = shutdown => {}
            }
            Ok(())
        })?;

        Ok(())
    }
}

#[derive(Default, Debug, Clone, Copy)]
struct TaskCenterExecutor;

impl<Fut> hyper::rt::Executor<Fut> for TaskCenterExecutor
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        let _ = task_center().spawn(TaskKind::Ingress, "ingress", None, async {
            fut.await;
            Ok(())
        });
    }
}

#[cfg(test)]
mod tests {
    use super::mocks::*;
    use super::*;

    use http_body_util::BodyExt;
    use http_body_util::Full;
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;
    use restate_core::{TaskCenter, TaskKind, TestCoreEnv};
    use restate_ingress_dispatcher::IngressRequest;
    use restate_test_util::assert_eq;
    use serde::{Deserialize, Serialize};
    use std::net::SocketAddr;
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;
    use tracing_test::traced_test;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct GreetingRequest {
        pub person: String,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct GreetingResponse {
        pub greeting: String,
    }

    #[tokio::test]
    #[traced_test]
    async fn test_http_post() {
        let (address, input, handle) = bootstrap_test().await;
        let process_fut = tokio::task::spawn(async move {
            // Get the function invocation and assert on it
            let (fid, method_name, argument, _, _, response_tx) =
                input.await.unwrap().unwrap().expect_invocation();
            assert_eq!(fid.service_id.service_name, "greeter.Greeter");
            assert_eq!(method_name, "greet");

            let greeting_req: GreetingRequest = serde_json::from_slice(&argument).unwrap();
            assert_eq!(&greeting_req.person, "Francesco");

            response_tx
                .send(
                    Ok(serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into())
                    .into(),
                )
                .unwrap();
        });

        // Send the request
        let client = Client::builder(TokioExecutor::new())
            .http2_only(true)
            .build_http::<Full<Bytes>>();
        let http_response = client
            .request(
                http::Request::post(format!("http://{address}/greeter.Greeter/greet"))
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(Full::new(
                        serde_json::to_vec(&GreetingRequest {
                            person: "Francesco".to_string(),
                        })
                        .unwrap()
                        .into(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        // check that the input processing has completed
        process_fut.await.unwrap();

        // Read the http_response_future
        assert_eq!(http_response.status(), http::StatusCode::OK);
        let (_, response_body) = http_response.into_parts();
        let response_bytes = response_body.collect().await.unwrap().to_bytes();
        let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
        restate_test_util::assert_eq!(response_value.greeting, "Igal");

        handle.close().await;
    }

    async fn bootstrap_test() -> (SocketAddr, JoinHandle<Option<IngressRequest>>, TestHandle) {
        let node_env = TestCoreEnv::create_with_mock_nodes_config(1, 1).await;
        let (ingress_request_tx, mut ingress_request_rx) = mpsc::unbounded_channel();

        // Create the ingress and start it
        let (ingress, start_signal) = HyperServerIngress::new(
            "0.0.0.0:0".parse().unwrap(),
            Semaphore::MAX_PERMITS,
            mock_component_resolver(),
            ingress_request_tx,
        );
        node_env
            .tc
            .spawn(TaskKind::SystemService, "ingress", None, ingress.run())
            .unwrap();

        // Mock the service invocation receiver
        let input = tokio::spawn(async move { ingress_request_rx.recv().await });

        // Wait server to start
        let address = start_signal.await.unwrap();

        (address, input, TestHandle(node_env.tc))
    }

    struct TestHandle(TaskCenter);

    impl TestHandle {
        async fn close(self) {
            self.0.cancel_tasks(None, None).await;
        }
    }
}
