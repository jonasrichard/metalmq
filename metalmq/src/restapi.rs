use crate::exchange::manager as em;
use crate::Context;
use hyper::{Body, Request, Response};
use log::info;
use serde_json;
use std::convert::Infallible;

pub(crate) async fn route(_req: Request<Body>, context: Context) -> Result<Response<Body>, Infallible> {
    let exchanges = em::get_exchanges(&context.exchange_manager).await;

    info!("REST {:?}", exchanges);

    let body = serde_json::to_string(&exchanges).unwrap();

    Ok(Response::new(body.into()))
}
