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
use std::{convert::Infallible, pin::Pin};

//pub(crate) async fn route(req: Request<Incoming>, context: Context) -> Result<Response<>, Infallible> {
//    match req.uri().path() {
//        "/exchanges" => {
//            let exchanges = em::get_exchanges(&context.exchange_manager).await;
//            let body = serde_json::to_string(&exchanges).unwrap();
//
//            Ok(Response::new(body.into()))
//        }
//        "/queues" => {
//            let queues = qm::get_queues(&context.queue_manager).await;
//            let body = serde_json::to_string(&queues).unwrap();
//
//            Ok(Response::new(body.into()))
//        }
//        _ => Ok(Response::builder().status(404).body(Body::empty()).unwrap()),
//    }
//}

//impl Service<Request<Incoming>> for crate::Context {
//    type Response = Response<Full<Bytes>>;
//
//    type Error = hyper::Error;
//
//    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
//
//    fn call(&self, req: Request<Incoming>) -> Self::Future {
//        fn mk_response(s: String) -> Result<Response<Full<Bytes>>, hyper::Error> {
//            Ok(Response::builder().body(Full::new(Bytes::from(s))).unwrap())
//        }
//
//        let res = match req.uri().path() {
//            "/exchanges" => {
//                let exchanges = em::get_exchanges(&self.exchange_manager).await;
//                let body = serde_json::to_string(&exchanges).unwrap();
//
//                Ok(Response::new(body.into()))
//            }
//            _ => Ok(Response::builder().status(404).body("".into()).unwrap()),
//        };
//
//        Box::pin(async { res })
//    }
//}
pub(crate) async fn simple(req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    Ok(Response::new(Full::new(Bytes::from("Hello, World!"))))
}

pub(crate) async fn route(req: Request<Incoming>, context: Context) -> Result<Response<Bytes>, Infallible> {
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
        _ => Ok(Response::builder().status(404).body(Bytes::new()).unwrap()),
    }
}
