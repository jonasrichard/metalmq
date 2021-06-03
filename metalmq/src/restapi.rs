use crate::{Context, CONTEXT};
use hyper::{Body, Request, Response};
use log::info;
use serde_json;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) async fn route(_req: Request<Body>, _ctx: Arc<Mutex<Context>>) -> Result<Response<Body>, Infallible> {
    let ctx = CONTEXT.lock().await;
    let exchanges = ctx.exchanges.exchange_list().await;
    drop(ctx);

    info!("REST {:?}", exchanges);

    let body = serde_json::to_string(&exchanges).unwrap();

    Ok(Response::new(body.into()))
}
