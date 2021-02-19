use crate::Context;
use log::info;
use serde_json;
use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use hyper::{Body, Request, Response};
use tokio::sync::Mutex;


pub(crate) async fn route(req: Request<Body>, ctx: Arc<Mutex<Context>>) -> Result<Response<Body>, Infallible> {
    let ctx = crate::CONTEXT.lock().await;

    let exchanges = ctx.exchanges.exchange_list().await;
    info!("REST {:?}", exchanges);

    let body = serde_json::to_string(&exchanges).unwrap();

    Ok(Response::new(body.into()))
}
