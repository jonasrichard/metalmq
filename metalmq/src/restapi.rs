use crate::Context;
use log::info;
use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use hyper::{Body, Request, Response};
use tokio::sync::Mutex;


pub(crate) async fn route(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let ctx = crate::CONTEXT.lock().await;

    let exchanges = ctx.exchanges.exchange_list().await;
    info!("REST {:?}", exchanges);

    Ok(Response::new("[]".into()))
}
