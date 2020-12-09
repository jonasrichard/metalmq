use crate::{frame_error, Result};
use std::collections::HashMap;

pub const CONNECTION_START: u32 = 0x000A000A;
pub const CONNECTION_START_OK: u32 = 0x000A000B;
pub const CONNECTION_TUNE: u32 = 0x000A001E;
pub const CONNECTION_TUNE_OK: u32 = 0x000A001F;
pub const CONNECTION_OPEN: u32 = 0x000A0028;
pub const CONNECTION_OPEN_OK: u32 = 0x000A0029;
pub const CONNECTION_CLOSE: u32 = 0x000A0032;
pub const CONNECTION_CLOSE_OK: u32 = 0x000A0033;

pub const CHANNEL_OPEN: u32 = 0x0014000A;
pub const CHANNEL_OPEN_OK: u32 = 0x0014000B;
pub const CHANNEL_CLOSE: u32 = 0x00140028;

pub const EXCHANGE_DECLARE: u32 = 0x0028000A;
pub const EXCHANGE_DECLARE_OK: u32 = 0x0028000B;

pub const QUEUE_DECLARE: u32 = 0x0032000A;
pub const QUEUE_DECLARE_OK: u32 = 0x0032000B;
pub const QUEUE_BIND: u32 = 0x00320014;
pub const QUEUE_BIND_OK: u32 = 0x00320015;

pub const BASIC_CONSUME: u32 = 0x003C0014;
pub const BASIC_CONSUME_OK: u32 = 0x003C0015;
pub const BASIC_PUBLISH: u32 = 0x003C0028;
pub const BASIC_DELIVER: u32 = 0x003C003C;

pub type Channel = u16;
pub type ClassMethod = u32;
pub type ClassId = u16;
pub type Weight = u16;

#[derive(Debug)]
pub enum AMQPFrame {
    Header,
    Method(Channel, ClassMethod, MethodFrameArgs),
    ContentHeader(Box<ContentHeaderFrame>),
    ContentBody(Box<ContentBodyFrame>),
    Heartbeat(Channel),
}

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
    ExchangeDeclare(ExchangeDeclareArgs),
    ExchangeDeclareOk,
    ExchangeBind(ExchangeBindArgs),
    ExchangeBindOk,
    QueueDeclare(QueueDeclareArgs),
    QueueDeclareOk(QueueDeclareOkArgs),
    QueueBind(QueueBindArgs),
    QueueBindOk,
    BasicConsume(BasicConsumeArgs),
    BasicConsumeOk(BasicConsumeOkArgs),
    BasicDeliver(BasicDeliverArgs),
    BasicPublish(BasicPublishArgs),
    Other(Box<Vec<AMQPValue>>),
}

#[derive(Debug)]
pub struct ContentHeaderFrame {
    pub channel: Channel,
    pub class_id: ClassId,
    pub weight: Weight,
    pub body_size: u64,
    pub prop_flags: u16, // we need to elaborate the properties here
    pub args: Vec<AMQPValue>,
}

#[derive(Debug)]
pub struct ContentBodyFrame {
    pub channel: Channel,
    pub body: Vec<u8>,
}

#[derive(Debug)]
pub enum AMQPType {
    U8,
    U16,
    U32,
    U64,
    SimpleString,
    LongString,
    FieldTable,
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

#[derive(Debug)]
pub enum AMQPFieldValue {
    Bool(bool),
    //    SimpleString(String),
    LongString(String),
    EmptyFieldTable,
    FieldTable(Box<FieldTable>),
}

#[derive(Debug, Default)]
pub struct ConnectionStartArgs {
    pub version_major: u8,
    pub version_minor: u8,
    pub capabilities: Option<FieldTable>,
    pub properties: Option<FieldTable>,
    pub mechanisms: String,
    pub locales: String,
}

#[derive(Debug, Default)]
pub struct ConnectionStartOkArgs {
    pub capabilities: Option<FieldTable>,
    pub properties: Option<FieldTable>,
    pub mechanism: String,
    pub response: String,
    pub locale: String,
}

#[derive(Debug, Default)]
pub struct ConnectionTuneArgs {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
}

#[derive(Debug, Default)]
pub struct ConnectionTuneOkArgs {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
}

#[derive(Debug, Default)]
pub struct ConnectionOpenArgs {
    pub virtual_host: String,
    pub insist: bool,
}

#[derive(Debug, Default)]
pub struct ConnectionCloseArgs {
    pub code: u16,
    pub text: String,
    pub class_id: u16,
    pub method_id: u16,
}

#[derive(Debug)]
pub struct ExchangeDeclareArgs {
    pub exchange_name: String,
    pub exchange_type: String,
    pub passive: bool,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub no_wait: bool,
    pub args: Option<FieldTable>,
}

#[derive(Debug)]
pub struct ExchangeBindArgs {
    pub source: String,
    pub destination: String,
    pub routing_key: String,
    pub no_wait: bool,
    pub args: Option<FieldTable>,
}

#[derive(Debug)]
pub struct QueueDeclareArgs {
    pub name: String,
    pub passive: bool,
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub no_wait: bool,
    pub args: Option<FieldTable>,
}

#[derive(Debug)]
pub struct QueueDeclareOkArgs {
    pub name: String,
    pub message_count: u32,
    pub consumer_count: u32,
}

#[derive(Debug)]
pub struct QueueBindArgs {
    pub queue_name: String,
    pub exchange_name: String,
    pub routing_key: String,
    pub no_wait: bool,
    pub args: Option<FieldTable>,
}

#[derive(Debug)]
pub struct BasicConsumeArgs {
    pub queue: String,
    pub consumer_tag: String,
    pub no_local: bool,
    pub no_ack: bool,
    pub exclusive: bool,
    pub no_wait: bool,
    pub args: Option<FieldTable>,
}

#[derive(Debug)]
pub struct BasicConsumeOkArgs {
    pub consumer_tag: String,
}

#[derive(Debug)]
pub struct BasicDeliverArgs {
    pub consumer_tag: String,
    pub delivery_tag: u64,
    pub redelivered: bool,
    pub exchange_name: String,
    pub routing_key: String,
}

#[derive(Debug)]
pub struct BasicPublishArgs {
    pub exchange_name: String,
    pub routing_key: String,
    pub mandatory: bool,
    pub immediate: bool,
}

impl From<ContentHeaderFrame> for AMQPFrame {
    fn from(chf: ContentHeaderFrame) -> AMQPFrame {
        AMQPFrame::ContentHeader(Box::new(chf))
    }
}

impl From<ContentBodyFrame> for AMQPFrame {
    fn from(cbf: ContentBodyFrame) -> AMQPFrame {
        AMQPFrame::ContentBody(Box::new(cbf))
    }
}

// Convenience function for getting string value from an `AMQPValue`.
pub fn value_as_string(val: AMQPValue) -> Result<String> {
    match val {
        AMQPValue::SimpleString(s) => Ok(s),
        AMQPValue::LongString(s) => Ok(s),
        _ => frame_error!(3, "Cannot convert to string"),
    }
}

pub fn arg_as_u8(args: Vec<AMQPValue>, index: usize) -> Result<u8> {
    if let Some(arg) = args.get(index) {
        if let AMQPValue::U8(v) = arg {
            return Ok(*v);
        }

        return frame_error!(3, "Cannot convert arg to u8");
    }

    frame_error!(4, "Arg index out of bound")
}

pub fn arg_as_u16(args: Vec<AMQPValue>, index: usize) -> Result<u16> {
    if let Some(arg) = args.get(index) {
        if let AMQPValue::U16(v) = arg {
            return Ok(*v);
        }

        return frame_error!(3, "Cannot convert arg to u16");
    }

    frame_error!(4, "Arg index out of bound")
}

pub fn arg_as_u32(args: Vec<AMQPValue>, index: usize) -> Result<u32> {
    if let Some(arg) = args.get(index) {
        if let AMQPValue::U32(v) = arg {
            return Ok(*v);
        }

        return frame_error!(3, "Cannot convert arg to u32");
    }

    frame_error!(4, "Arg index out of bound")
}

pub fn arg_as_u64(args: Vec<AMQPValue>, index: usize) -> Result<u64> {
    if let Some(arg) = args.get(index) {
        if let AMQPValue::U64(v) = arg {
            return Ok(*v);
        }

        return frame_error!(3, "Cannot convert arg to u64");
    }

    frame_error!(4, "Arg index out of bound")
}

pub fn get_bool(ft: FieldTable, name: String) -> Result<bool> {
    if let Some(value) = ft.get(&name) {
        if let AMQPFieldValue::Bool(v) = value {
            return Ok(*v);
        }
    }

    frame_error!(6, "Cannot convert field value to bool")
}

pub fn connection_start(channel: u16) -> AMQPFrame {
    let mut capabilities = FieldTable::new();

    capabilities.insert("publisher_confirms".into(), AMQPFieldValue::Bool(true));
    capabilities.insert(
        "exchange_exchange_bindings".into(),
        AMQPFieldValue::Bool(true),
    );
    capabilities.insert("basic.nack".into(), AMQPFieldValue::Bool(true));
    capabilities.insert("consumer_cancel_notify".into(), AMQPFieldValue::Bool(true));
    capabilities.insert("connection.blocked".into(), AMQPFieldValue::Bool(true));
    capabilities.insert("consumer_priorities".into(), AMQPFieldValue::Bool(true));
    capabilities.insert(
        "authentication_failure_close".into(),
        AMQPFieldValue::Bool(true),
    );
    capabilities.insert("per_consumer_qos".into(), AMQPFieldValue::Bool(true));
    capabilities.insert("direct_reply_to".into(), AMQPFieldValue::Bool(true));

    let mut server_properties = FieldTable::new();

    server_properties.insert(
        "capabilities".into(),
        AMQPFieldValue::FieldTable(Box::new(capabilities)),
    );
    server_properties.insert(
        "product".into(),
        AMQPFieldValue::LongString("IronMQ server".into()),
    );
    server_properties.insert("version".into(), AMQPFieldValue::LongString("0.1.0".into()));

    AMQPFrame::Method(
        channel,
        CONNECTION_START,
        MethodFrameArgs::ConnectionStart(ConnectionStartArgs {
            version_major: 0,
            version_minor: 9,
            capabilities: None,
            properties: Some(server_properties),
            mechanisms: "PLAIN".into(),
            locales: "en_US".into()
        }))
}

// TODO here will be an Authentication enum with the different possibilities
pub fn connection_start_ok(username: &str, password: &str, capabilities: FieldTable) -> AMQPFrame {
    let mut client_properties = FieldTable::new();

    client_properties.insert(
        "product".into(),
        AMQPFieldValue::LongString("ironmq-client".into()),
    );
    client_properties.insert("platform".into(), AMQPFieldValue::LongString("Rust".into()));
    client_properties.insert(
        "capabilities".into(),
        AMQPFieldValue::FieldTable(Box::new(capabilities)),
    );
    // TODO get the version from the build vars or an external file
    client_properties.insert("version".into(), AMQPFieldValue::LongString("0.1.0".into()));

    let mut auth = Vec::<u8>::new();
    auth.push(0x00);
    auth.extend_from_slice(username.as_bytes());
    auth.push(0x00);
    auth.extend_from_slice(password.as_bytes());

    let auth_string = String::from_utf8(auth).unwrap();

    AMQPFrame::Method(
        0,
        CONNECTION_START_OK,
        MethodFrameArgs::ConnectionStartOk(ConnectionStartOkArgs {
            capabilities: None,
            properties: Some(client_properties),
            mechanism: "PLAIN".into(),
            response: auth_string,
            locale: "en_US".into()
        }))
}

pub fn connection_tune(channel: u16) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        CONNECTION_TUNE,
        MethodFrameArgs::ConnectionTune(ConnectionTuneArgs {
            channel_max: 2047,
            frame_max: 131_072,
            heartbeat: 60
        }))
}

pub fn connection_tune_ok(channel: u16) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        CONNECTION_TUNE_OK,
        MethodFrameArgs::ConnectionTuneOk(ConnectionTuneOkArgs {
            channel_max: 2047,
            frame_max: 131_072,
            heartbeat: 60
        }))
}

pub fn connection_open(channel: u16, virtual_host: String) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        CONNECTION_OPEN,
        MethodFrameArgs::ConnectionOpen(ConnectionOpenArgs {
            virtual_host: virtual_host,
            insist: true
        }))
}

pub fn connection_open_ok(channel: u16) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        CONNECTION_OPEN_OK,
        MethodFrameArgs::ConnectionOpenOk
    )
}

pub fn connection_close(channel: u16) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        CONNECTION_CLOSE,
        MethodFrameArgs::ConnectionClose(ConnectionCloseArgs {
            code: 200,
            text: "Normal shutdown".into(),
            class_id: 0,
            method_id: 0
        }))
}

pub fn connection_close_ok(channel: u16) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        CONNECTION_CLOSE_OK,
        MethodFrameArgs::ConnectionCloseOk
        )
}

pub fn channel_open(channel: u16) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        CHANNEL_OPEN,
        MethodFrameArgs::ChannelOpen
    )
}

pub fn channel_open_ok(channel: u16) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        CHANNEL_OPEN_OK,
        MethodFrameArgs::ChannelOpenOk
    )
}

pub fn exchange_declare(channel: u16, exchange_name: String, exchange_type: String) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        EXCHANGE_DECLARE,
        MethodFrameArgs::ExchangeDeclare(ExchangeDeclareArgs {
            exchange_name: exchange_name,
            exchange_type: exchange_type,
            passive: false,
            durable: false,
            auto_delete: false,
            internal: false,
            no_wait: false,
            args: None
        }))
}

pub fn exchange_declare_ok(channel: u16) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        EXCHANGE_DECLARE_OK,
        MethodFrameArgs::ExchangeDeclareOk
    )
}

pub fn queue_bind(
    channel: u16,
    queue_name: String,
    exchange_name: String,
    routing_key: String,
) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        QUEUE_BIND,
        MethodFrameArgs::QueueBind(QueueBindArgs {
            queue_name: queue_name,
            exchange_name: exchange_name,
            routing_key: routing_key,
            no_wait: false,
            args: None
        }))
}

pub fn queue_bind_ok(channel: u16) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        QUEUE_BIND_OK,
        MethodFrameArgs::QueueBindOk
    )
}

pub fn queue_declare(channel: u16, queue_name: String) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        QUEUE_DECLARE,
        MethodFrameArgs::QueueDeclare(QueueDeclareArgs {
            name: queue_name,
            passive: false,
            durable: false,
            exclusive: false,
            auto_delete: false,
            no_wait: false,
            args: None
        }))
}

pub fn queue_declare_ok(
    channel: u16,
    queue_name: String,
    message_count: u32,
    consumer_count: u32,
) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        QUEUE_DECLARE_OK,
        MethodFrameArgs::QueueDeclareOk(QueueDeclareOkArgs {
            name: queue_name,
            message_count: message_count,
            consumer_count: consumer_count
        }))
}

pub fn basic_consume(channel: u16, queue_name: String, consumer_tag: String) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        BASIC_CONSUME,
        MethodFrameArgs::BasicConsume(BasicConsumeArgs {
            queue: queue_name,
            consumer_tag: consumer_tag,
            no_local: false,
            no_ack: false,
            exclusive: false,
            no_wait: false,
            args: None
        }))
}

pub fn basic_consume_ok(channel: u16, consumer_tag: String) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        BASIC_CONSUME_OK,
        MethodFrameArgs::BasicConsumeOk(BasicConsumeOkArgs {
            consumer_tag: consumer_tag
        }))
}

pub fn basic_deliver(
    channel: u16,
    consumer_tag: String,
    delivery_tag: u64,
    redelivered: bool,
    exchange_name: String,
    routing_key: String,
) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        BASIC_DELIVER,
        MethodFrameArgs::BasicDeliver(BasicDeliverArgs {
            consumer_tag: consumer_tag,
            delivery_tag: delivery_tag,
            redelivered: redelivered,
            exchange_name: exchange_name,
            routing_key: routing_key
        })
    )
}

pub fn basic_publish(channel: u16, exchange_name: String, routing_key: String) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        BASIC_PUBLISH,
        MethodFrameArgs::BasicPublish(BasicPublishArgs {
            exchange_name: exchange_name,
            routing_key: routing_key,
            mandatory: true,
            immediate: true
        })
    )
}

pub fn content_header(channel: u16, size: u64) -> ContentHeaderFrame {
    ContentHeaderFrame {
        channel: channel,
        class_id: 0x003C,
        weight: 0,
        body_size: size,
        prop_flags: 0x0000,
        args: vec![],
    }
}

pub fn content_body(channel: u16, payload: &[u8]) -> ContentBodyFrame {
    ContentBodyFrame {
        channel: channel,
        body: payload.to_vec(),
    }
}
