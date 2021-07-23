use anyhow::Result;
use serde_derive::Deserialize;

#[derive(Deserialize)]
pub(crate) struct Config {
    pub(crate) network: Network,
}

#[derive(Deserialize)]
pub(crate) struct Network {
    pub(crate) amqp_listen: String,
    pub(crate) http_listen: String,
}

pub(crate) fn parse_config() -> Result<Config> {
    let cfg = std::fs::read_to_string("metalmq.toml")?;
    Ok(toml::from_str(&cfg)?)
}
