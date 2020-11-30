//! `client_sm` module represents the client state machine which handles incoming
//! commands (from client api side) and incoming AMQP frames from network/server
//! side.
//!
//! So everything which comes from the server or goes to the server is an
//! AMQP frame or `MethodFrame`, content etc. Everything which talks to the client
//! api it is a typed struct.

use crate::Result;
use ironmq_codec::frame;
use ironmq_codec::frame::{MethodFrame};
use log::info;

#[derive(Debug)]
pub(crate) struct ClientState {
    pub(crate) state: Phase
}

#[derive(Debug)]
pub(crate) enum Phase {
    Uninitialized,
    Connected,
    Authenticated,
    Closing
}

#[derive(Debug)]
pub(crate) enum Command {
    ConnectionInit,
    ConnectionStartOk,
    ConnectionTuneOk,
    ConnectionOpen(Box<ConnOpenArgs>),
    ConnectionClose,
    ChannelOpen(Box<ChannelOpenArgs>),
    ExchangeDeclare(Box<ExchangeDeclareArgs>),
    QueueDeclare(Box<QueueDeclareArgs>),
    QueueBind(Box<QueueBindArgs>)
}

/// Handle the connection start frame coming from the server.
///
/// Announces and agrees on capabilities from server and client side.
pub(crate) fn connection_start(cs: &mut ClientState, f: MethodFrame) -> Result<Option<MethodFrame>> {
    // TODO store server properties
    Ok(Some(frame::connection_start_ok(0)))
}

pub(crate) fn connection_tune(cs: &mut ClientState, f: MethodFrame) -> Result<Option<MethodFrame>> {
    cs.state = Phase::Connected;
    // TODO how to handle Error frames?
    Ok(Some(frame::connection_tune_ok(0)))
}

pub(crate) fn connection_open(cs: &mut ClientState, args: ConnOpenArgs) -> Result<MethodFrame> {
    Ok(frame::connection_open(0, args.virtual_host))
}

pub(crate) fn connection_open_ok(cs: &mut ClientState, f: MethodFrame) -> Result<Option<MethodFrame>> {
    Ok(None)
}

pub(crate) fn channel_open_ok(cs: &mut ClientState, f: MethodFrame) -> Result<Option<MethodFrame>> {
    info!("Channel is opened {}", f.channel);
    Ok(None)
}

pub(crate) fn connection_close_ok(cs: &mut ClientState, f: MethodFrame) -> Result<Option<MethodFrame>> {
    Ok(None)
}

pub(crate) fn exchange_declare(cs: &mut ClientState, args: ExchangeDeclareArgs) -> Result<MethodFrame> {
    Ok(frame::exchange_declare(args.channel, args.exchange_name, args.exchange_type))
}

pub(crate) fn exchange_declare_ok(cs: &mut ClientState, f: MethodFrame) -> Result<Option<MethodFrame>> {
    Ok(None)
}

pub(crate) fn queue_declare(cs: &mut ClientState, args: QueueDeclareArgs) -> Result<Option<MethodFrame>> {
    Ok(Some(frame::queue_declare(args.channel, args.queue_name)))
}

pub(crate) fn queue_declare_ok(cs: &mut ClientState, f: MethodFrame) -> Result<Option<MethodFrame>> {
    Ok(None)
}

pub(crate) fn queue_bind(cs: &mut ClientState, args: QueueBindArgs) -> Result<Option<MethodFrame>> {
    Ok(Some(frame::queue_bind(args.channel, args.exchange_name, args.queue_name, args.routing_key)))
}

pub(crate) fn queue_bind_ok(cs: &mut ClientState, f: MethodFrame) -> Result<Option<MethodFrame>> {
    Ok(None)
}





#[derive(Debug)]
pub(crate) struct ConnOpenArgs {
    pub(crate) virtual_host: String,
    pub(crate) insist: bool
}

#[derive(Debug)]
pub(crate) struct ChannelOpenArgs {
    pub(crate) channel: u16
}

#[derive(Debug)]
pub(crate) struct ChannelOpenOkArgs {
    pub(crate) channel: u16
}

#[derive(Debug)]
pub(crate) struct ExchangeDeclareArgs {
    pub(crate) channel: u16,
    pub(crate) exchange_name: String,
    pub(crate) exchange_type: String
}

#[derive(Debug)]
pub(crate) struct ExchangeDeclareOkArgs {
    pub(crate) channel: u16
}

#[derive(Debug)]
pub(crate) struct QueueDeclareArgs {
    pub(crate) channel: u16,
    pub(crate) queue_name: String
}

#[derive(Debug)]
pub(crate) struct QueueDeclareOkArgs {
    pub(crate) channel: u16
}

#[derive(Debug)]
pub(crate) struct QueueBindArgs {
    pub(crate) channel: u16,
    pub(crate) queue_name: String,
    pub(crate) exchange_name: String,
    pub(crate) routing_key: String
}

#[derive(Debug)]
pub(crate) struct QueueBindOkArgs {
    pub(crate) channel: u16
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn connect() {
    }
}
