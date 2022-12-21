use anyhow::Result;
use clap::{Arg, Command};
use serde_derive::Deserialize;

pub const MAX_FRAME_SIZE: usize = 131_072;
pub const MAX_CHANNELS_PER_CONNECTION: u16 = 2047;

pub(crate) struct CliConfig {
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
    let matches = Command::new("MetalMQ server")
        .version("0.2.1")
        .author("Richard Jonas <richard.jonas.76@gmail.com>")
        .about("AMQP compatible messaging queue server")
        .arg(
            Arg::new("config")
                .short('c')
                .value_name("FILE")
                .help("Path to the config file")
                .takes_value(true),
        )
        .get_matches();

    CliConfig {
        config_file_path: matches.value_of("config").unwrap_or("metalmq.toml").to_string(),
    }
}
