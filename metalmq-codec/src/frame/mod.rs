mod basic;
mod channel;
mod connection;
mod exchange;
mod queue;

use bitflags::Flags;

pub use self::{
    basic::{
        basic_get_empty, confirm_select, confirm_select_ok, BasicAckArgs, BasicCancelArgs, BasicCancelOkArgs,
        BasicConsumeArgs, BasicConsumeFlags, BasicConsumeOkArgs, BasicDeliverArgs, BasicGetArgs, BasicGetOkArgs,
        BasicNackArgs, BasicNackFlags, BasicPublishArgs, BasicPublishFlags, BasicRejectArgs, BasicReturnArgs,
        ConfirmSelectArgs,
    },
    channel::{channel_close, channel_close_ok, channel_open, channel_open_ok, ChannelCloseArgs},
    connection::{
        connection_close, connection_close_ok, connection_open_ok, connection_tune, connection_tune_ok,
        ConnectionCloseArgs, ConnectionOpenArgs, ConnectionStartArgs, ConnectionStartOkArgs, ConnectionTuneArgs,
        ConnectionTuneOkArgs,
    },
    exchange::{
        exchange_declare_ok, exchange_delete_ok, ExchangeDeclareArgs, ExchangeDeclareFlags, ExchangeDeleteArgs,
        ExchangeDeleteFlags,
    },
    queue::{
        queue_bind_ok, QueueBindArgs, QueueDeclareArgs, QueueDeclareFlags, QueueDeclareOkArgs, QueueDeleteArgs,
        QueueDeleteFlags, QueueDeleteOkArgs, QueuePurgeArgs, QueuePurgeOkArgs, QueueUnbindArgs,
    },
};
use std::collections::HashMap;

pub const CONNECTION_START: u32 = 0x000A000A;
pub const CONNECTION_START_OK: u32 = 0x000A000B;
pub const CONNECTION_SECRET: u32 = 0x000A0014;
pub const CONNECTION_SECRET_OK: u32 = 0x000A0015;
pub const CONNECTION_TUNE: u32 = 0x000A001E;
pub const CONNECTION_TUNE_OK: u32 = 0x000A001F;
pub const CONNECTION_OPEN: u32 = 0x000A0028;
pub const CONNECTION_OPEN_OK: u32 = 0x000A0029;
pub const CONNECTION_CLOSE: u32 = 0x000A0032;
pub const CONNECTION_CLOSE_OK: u32 = 0x000A0033;
pub const CONNECTION_BLOCKED: u32 = 0x000A003C;
pub const CONNECTION_UNBLOCKED: u32 = 0x000A003D;

pub const CHANNEL_OPEN: u32 = 0x0014000A;
pub const CHANNEL_OPEN_OK: u32 = 0x0014000B;
pub const CHANNEL_FLOW: u32 = 0x00140014;
pub const CHANNEL_FLOW_OK: u32 = 0x00140015;
pub const CHANNEL_CLOSE: u32 = 0x00140028;
pub const CHANNEL_CLOSE_OK: u32 = 0x00140029;

pub const EXCHANGE_DECLARE: u32 = 0x0028000A;
pub const EXCHANGE_DECLARE_OK: u32 = 0x0028000B;
pub const EXCHANGE_DELETE: u32 = 0x00280014;
pub const EXCHANGE_DELETE_OK: u32 = 0x00280015;

pub const QUEUE_DECLARE: u32 = 0x0032000A;
pub const QUEUE_DECLARE_OK: u32 = 0x0032000B;
pub const QUEUE_BIND: u32 = 0x00320014;
pub const QUEUE_BIND_OK: u32 = 0x00320015;
pub const QUEUE_PURGE: u32 = 0x0032001E;
pub const QUEUE_PURGE_OK: u32 = 0x0032001F;
pub const QUEUE_DELETE: u32 = 0x00320028;
pub const QUEUE_DELETE_OK: u32 = 0x00320029;
pub const QUEUE_UNBIND: u32 = 0x00320032;
pub const QUEUE_UNBIND_OK: u32 = 0x00320033;

pub const BASIC_QOS: u32 = 0x003C000A;
pub const BASIC_QOS_OK: u32 = 0x003C000B;
pub const BASIC_CONSUME: u32 = 0x003C0014;
pub const BASIC_CONSUME_OK: u32 = 0x003C0015;
pub const BASIC_CANCEL: u32 = 0x003C001E;
pub const BASIC_CANCEL_OK: u32 = 0x003C001F;
pub const BASIC_PUBLISH: u32 = 0x003C0028;
pub const BASIC_RETURN: u32 = 0x003C0032;
pub const BASIC_DELIVER: u32 = 0x003C003C;
pub const BASIC_GET: u32 = 0x003C0046;
pub const BASIC_GET_OK: u32 = 0x003C0047;
pub const BASIC_GET_EMPTY: u32 = 0x003C0048;
pub const BASIC_ACK: u32 = 0x003C0050;
pub const BASIC_REJECT: u32 = 0x003C005A;
pub const BASIC_RECOVER_ASYNC: u32 = 0x003C0064;
pub const BASIC_RECOVER: u32 = 0x003C006E;
pub const BASIC_RECOVER_OK: u32 = 0x003C006F;
pub const BASIC_NACK: u32 = 0x003C0078;

pub const CONFIRM_SELECT: u32 = 0x0055000A;
pub const CONFIRM_SELECT_OK: u32 = 0x0055000B;

pub type Channel = u16;
pub type ClassMethod = u32;
pub type ClassId = u16;
pub type Weight = u16;

// TODO
// We would save a lot of time if we have a trait like
//
// trait IntoFrame {
//     fn frame(channel: u16) -> AMQPFrame
// }
//
// also Frame can implement From<AMQPFrame>, so a lot of times in the server implementation when we
// need to change between Frame and AMQPFrame types at least generating those types would be
// easier.
//
// Also client can write a function, especially the TestClient which would say
//
// fn handle_method_args(channel: u16, args: impl IntoFrame) {
//     send(args.frame(channel));
// }

/// Represents an AMQP frame.
pub enum AMQPFrame {
    /// Header is to be sent to the server at first, announcing the AMQP version we support
    Header,
    /// Represents the AMQP RPC frames. Connection based calls have a channel number 0, otherwise
    /// channel is the current channel on which the frames are sent. The RPC arguments are
    /// represented in `MethodFrameArgs`.
    Method(Channel, ClassMethod, MethodFrameArgs),
    ContentHeader(ContentHeaderFrame),
    ContentBody(ContentBodyFrame),
    Heartbeat(Channel),
}

impl std::fmt::Debug for AMQPFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AMQPFrame::Header => write!(f, "Header"),
            AMQPFrame::Method(ch, cm, args) => write!(f, "Method(channel={}, {:08X}, {:?})", ch, cm, args),
            AMQPFrame::ContentHeader(ch) => write!(f, "ContentHeader({:?})", ch),
            AMQPFrame::ContentBody(cb) => write!(f, "ContentBody({:?})", cb),
            AMQPFrame::Heartbeat(_) => write!(f, "Heartbeat"),
        }
    }
}

/// Represents all types of method frame arguments.
#[derive(Debug)]
pub enum MethodFrameArgs {
    ConnectionStart(ConnectionStartArgs),
    ConnectionStartOk(ConnectionStartOkArgs),
    ConnectionTune(ConnectionTuneArgs),
    ConnectionTuneOk(ConnectionTuneOkArgs),
    ConnectionOpen(ConnectionOpenArgs),
    ConnectionOpenOk,
    ConnectionClose(ConnectionCloseArgs),
    ConnectionCloseOk,
    ChannelOpen,
    ChannelOpenOk,
    ChannelClose(ChannelCloseArgs),
    ChannelCloseOk,
    ExchangeDeclare(ExchangeDeclareArgs),
    ExchangeDeclareOk,
    ExchangeDelete(ExchangeDeleteArgs),
    ExchangeDeleteOk,
    QueueDeclare(QueueDeclareArgs),
    QueueDeclareOk(QueueDeclareOkArgs),
    QueueBind(QueueBindArgs),
    QueueBindOk,
    QueuePurge(QueuePurgeArgs),
    QueuePurgeOk(QueuePurgeOkArgs),
    QueueDelete(QueueDeleteArgs),
    QueueDeleteOk(QueueDeleteOkArgs),
    QueueUnbind(QueueUnbindArgs),
    QueueUnbindOk,
    BasicConsume(BasicConsumeArgs),
    BasicConsumeOk(BasicConsumeOkArgs),
    BasicCancel(BasicCancelArgs),
    BasicCancelOk(BasicCancelOkArgs),
    BasicGet(BasicGetArgs),
    BasicGetOk(BasicGetOkArgs),
    BasicGetEmpty,
    BasicPublish(BasicPublishArgs),
    BasicReturn(BasicReturnArgs),
    BasicDeliver(BasicDeliverArgs),
    BasicAck(BasicAckArgs),
    BasicReject(BasicRejectArgs),
    BasicNack(BasicNackArgs),
    ConfirmSelect(ConfirmSelectArgs),
    ConfirmSelectOk,
}

bitflags! {
    #[derive(Copy, Clone, Debug)]
    pub struct HeaderPropertyFlags: u16 {
        const CLUSTER_ID       = 0b0000_0000_0000_0100;
        const APP_ID           = 0b0000_0000_0000_1000;
        const USER_ID          = 0b0000_0000_0001_0000;
        const MESSAGE_TYPE     = 0b0000_0000_0010_0000;
        const TIMESTAMP        = 0b0000_0000_0100_0000;
        const MESSAGE_ID       = 0b0000_0000_1000_0000;
        const EXPIRATION       = 0b0000_0001_0000_0000;
        const REPLY_TO         = 0b0000_0010_0000_0000;
        const CORRELATION_ID   = 0b0000_0100_0000_0000;
        const PRIORITY         = 0b0000_1000_0000_0000;
        const DELIVERY_MODE    = 0b0001_0000_0000_0000;
        const HEADERS          = 0b0010_0000_0000_0000;
        const CONTENT_ENCODING = 0b0100_0000_0000_0000;
        const CONTENT_TYPE     = 0b1000_0000_0000_0000;
    }
}

impl Default for HeaderPropertyFlags {
    fn default() -> Self {
        HeaderPropertyFlags::empty()
    }
}

// FIXME The property flags is 16 bits but a message can contain more than 16 properties.
// If the last bit of the flags is 0, there are more properties sent. I cannot see how, I need to
// see a good example.
#[derive(Debug, Default)]
pub struct ContentHeaderFrame {
    pub channel: Channel,
    pub class_id: ClassId,
    pub weight: Weight,
    pub body_size: u64,
    pub prop_flags: HeaderPropertyFlags,
    pub cluster_id: Option<String>,
    pub app_id: Option<String>,
    pub user_id: Option<String>,
    pub message_type: Option<String>,
    pub timestamp: Option<u64>,
    pub message_id: Option<String>,
    pub expiration: Option<String>,
    pub reply_to: Option<String>,
    pub correlation_id: Option<String>,
    pub priority: Option<u8>,
    pub delivery_mode: Option<u8>,
    pub headers: Option<FieldTable>,
    pub content_encoding: Option<String>,
    pub content_type: Option<String>,
}

impl ContentHeaderFrame {
    pub fn with_content_type(&mut self, content_type: String) -> &ContentHeaderFrame {
        self.content_type = Some(content_type);
        Flags::set(&mut self.prop_flags, HeaderPropertyFlags::CONTENT_TYPE, true);
        self
    }

    pub fn frame(self) -> AMQPFrame {
        AMQPFrame::ContentHeader(self)
    }
}

pub struct ContentBodyFrame {
    pub channel: Channel,
    // TODO here we need something which can be cloned cheap like Box or Rc. Sometimes we can move
    // out this from the struct and we can build a new struct. But we need to avoid the
    // byte-to-byte copy.
    pub body: Vec<u8>,
}

impl ContentBodyFrame {
    pub fn frame(self) -> AMQPFrame {
        AMQPFrame::ContentBody(self)
    }
}

impl std::fmt::Debug for ContentBodyFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let body = String::from_utf8_lossy(&self.body[..std::cmp::min(64usize, self.body.len())]);

        f.write_fmt(format_args!(
            "ContentBodyFrame {{ channel: {}, body: \"{}\" }}",
            &self.channel, body
        ))
    }
}

/// Type alias for inner type of field value.
pub type FieldTable = HashMap<String, AMQPFieldValue>;

#[derive(Debug)]
pub enum AMQPValue {
    //    Bool(bool),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    SimpleString(String),
    LongString(String),
    EmptyFieldTable,
    FieldTable(Box<FieldTable>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum AMQPFieldValue {
    Bool(bool),
    //    SimpleString(String),
    LongString(String),
    EmptyFieldTable,
    FieldTable(Box<FieldTable>),
}

impl From<ContentHeaderFrame> for AMQPFrame {
    fn from(chf: ContentHeaderFrame) -> AMQPFrame {
        AMQPFrame::ContentHeader(chf)
    }
}

impl From<ContentBodyFrame> for AMQPFrame {
    fn from(cbf: ContentBodyFrame) -> AMQPFrame {
        AMQPFrame::ContentBody(cbf)
    }
}

pub fn fieldtable_to_hashmap(value: FieldTable) -> HashMap<String, String> {
    let mut m = HashMap::new();

    for (k, v) in value {
        match v {
            AMQPFieldValue::Bool(b) => {
                m.insert(k, b.to_string());
            }
            AMQPFieldValue::LongString(s) => {
                m.insert(k, s);
            }
            AMQPFieldValue::EmptyFieldTable => {}
            AMQPFieldValue::FieldTable(_) => panic!("Embedded field table is not supported"),
        }
    }

    m
}

/// Split class id and method id from `u32` combined code.
pub fn split_class_method(cm: u32) -> (u16, u16) {
    let method_id = (cm & 0x0000FFFF) as u16;
    let class_id = (cm >> 16) as u16;

    (class_id, method_id)
}

/// Combine class id and method id to a single `u32` value
pub fn unify_class_method(class: u16, method: u16) -> u32 {
    ((class as u32) << 16) | (method as u32)
}

pub fn heartbeat() -> AMQPFrame {
    AMQPFrame::Heartbeat(0)
}
