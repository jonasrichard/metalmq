use anyhow::Result;
use clap::{App, Arg};
use serde_derive::Deserialize;

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
    let matches = App::new("MetalMQ server")
        .version("0.2.1")
        .author("Richard Jonas <richard.jonas.76@gmail.com>")
        .about("AMQP compatible messaging queue server")
        .arg(
            Arg::with_name("config")
                .short("c")
                .value_name("FILE")
                .help("Path to the config file")
                .takes_value(true),
        )
        .get_matches();

    CliConfig {
        config_file_path: matches.value_of("config").unwrap_or("metalmq.toml").to_string(),
    }
}
