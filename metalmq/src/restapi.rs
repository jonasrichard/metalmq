use std::convert::Infallible;
use hyper::{Body, Request, Response};

pub(crate) async fn exchange_list(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    Ok(Response::new("[]".into()))
}
