use crate::exchange::manager as em;
use crate::queue::manager as qm;
use crate::Context;
use http_body_util::Full;
use hyper::{body, Request, Response};
use std::{convert::Infallible, sync::Arc};

pub(crate) async fn route(
    req: Request<body::Incoming>,
    context: Arc<Context>,
) -> Result<Response<Full<body::Bytes>>, Infallible> {
    match req.uri().path() {
        "/exchanges" => {
            let exchanges = em::get_exchanges(&context.exchange_manager).await;
            let _ = serde_json::to_string(&exchanges).unwrap();

            //Ok(Response::new(body))
            Ok(Response::new("".into()))
        }
        "/queues" => {
            let queues = qm::get_queues(&context.queue_manager).await;
            let _ = serde_json::to_string(&queues).unwrap();

            Ok(Response::new("".into()))
        }
        _ => Ok(Response::builder().status(404).body("".into()).unwrap()),
    }
}
