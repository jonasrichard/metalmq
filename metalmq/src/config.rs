use anyhow::Result;
use clap::Parser;
use serde_derive::Deserialize;

pub const MAX_FRAME_SIZE: usize = 131_072;
pub const MAX_CHANNELS_PER_CONNECTION: u16 = 2047;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub(crate) struct CliConfig {
    #[arg(short, long, default_value_t = String::from("metalmq.toml"))]
    pub(crate) config_file_path: String,
}

#[derive(Deserialize)]
pub(crate) struct Config {
    pub(crate) network: Network,
}

#[derive(Deserialize)]
pub(crate) struct Network {
    pub(crate) amqp_listen: String,
    pub(crate) http_listen: String,
}

pub(crate) fn parse_config(path: &str) -> Result<Config> {
    let cfg = std::fs::read_to_string(path)?;
    Ok(toml::from_str(&cfg)?)
}

pub(crate) fn cli() -> CliConfig {
    CliConfig::parse()
}
