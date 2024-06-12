use crate::exchange::manager as em;
use crate::queue::manager as qm;
use crate::Context;
use bytes::Bytes;
use futures::Future;
use http_body_util::Full;
use hyper::{
    body::{Body, Incoming},
    service::Service,
    Request, Response,
};
use std::convert::Infallible;

pub(crate) async fn route(req: Request<()>, context: Context) -> Result<Response<()>, Infallible> {
    match req.uri().path() {
        "/exchanges" => {
            let exchanges = em::get_exchanges(&context.exchange_manager).await;
            let _ = serde_json::to_string(&exchanges).unwrap();

            //Ok(Response::new(body))
            Ok(Response::new(()))
        }
        "/queues" => {
            let queues = qm::get_queues(&context.queue_manager).await;
            let _ = serde_json::to_string(&queues).unwrap();

            Ok(Response::new(()))
        }
        _ => Ok(Response::builder().status(404).body(Bytes::new()).unwrap()),
    }
}
