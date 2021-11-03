use crate::exchange::manager as em;
use crate::queue::manager as qm;
use crate::Context;
use hyper::{Body, Request, Response};
use std::convert::Infallible;

pub(crate) async fn route(req: Request<Body>, context: Context) -> Result<Response<Body>, Infallible> {
    match req.uri().path() {
        "/exchanges" => {
            let exchanges = em::get_exchanges(&context.exchange_manager).await;
            let body = serde_json::to_string(&exchanges).unwrap();

            Ok(Response::new(body.into()))
        }
        "/queues" => {
            let queues = qm::get_queues(&context.queue_manager).await;
            let body = serde_json::to_string(&queues).unwrap();

            Ok(Response::new(body.into()))
        }
        _ => Ok(Response::builder().status(404).body(Body::empty()).unwrap()),
    }
}
