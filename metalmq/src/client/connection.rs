//! Represent the connection when a client connects to the server.
//!
//! It maintains the connection state and also servers as a forwarder of the frames to the open
//! channels. It handles the outgoing messages, too.
pub mod open_close;
pub mod router;
pub mod types;

#[cfg(test)]
mod tests;
