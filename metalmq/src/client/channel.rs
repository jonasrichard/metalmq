//! Represents the channel opened in a connection by a client.
//!
//! It maintains the channel state, the consumings the passive consume processes by basic get
//! commands.
//!
//! Each channel is a spawned tokio process, so a longer handling of a message doesn't block the
//! whole connection or another channel.
pub mod basic;
pub mod content;
pub mod exchange;
pub mod open_close;
pub mod queue;
pub mod router;
pub mod types;
