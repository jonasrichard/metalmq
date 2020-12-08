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
    ConnectionOpen(ConnectionOpenArgs),
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
type FieldTable = HashMap<String, AMQPFieldValue>;

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

#[derive(Debug)]
pub struct ConnectionStartArgs {
    version_major: u8,
    version_minor: u8,
    capabilities: FieldTable,
    properties: FieldTable,
    mechanisms: String,
    locales: String,
}

#[derive(Debug)]
pub struct ConnectionStartOkArgs {
    capabilities: FieldTable,
    properties: FieldTable,
    mechanism: String,
    response: String,
    locale: String,
}

#[derive(Debug)]
pub struct ConnectionTuneArgs {
    channel_max: u16,
    frame_max: u16,
    heartbeat: u16,
}

#[derive(Debug)]
pub struct ConnectionTuneOkArgs {
    channel_max: u16,
    frame_max: u16,
    heartbeat: u16,
}

#[derive(Debug)]
pub struct ConnectionOpenArgs {
    pub virtual_host: String,
    pub insist: bool,
}

#[derive(Debug)]
pub struct ConnectionOpenOkArgs {}

#[derive(Debug)]
pub struct ChannelOpenArgs {}

#[derive(Debug)]
pub struct ChannelOpenOkArgs {}

#[derive(Debug)]
pub struct ExchangeDeclareArgs {
    exchange_name: String,
    exchange_type: String,
    passive: bool,
    durable: bool,
    auto_delete: bool,
    internal: bool,
    no_wait: bool,
    args: FieldTable,
}

#[derive(Debug)]
pub struct ExchangeDeclareOkArgs {}

#[derive(Debug)]
pub struct ExchangeBindArgs {
    source: String,
    destination: String,
    routing_key: String,
    no_wait: bool,
    args: FieldTable,
}

#[derive(Debug)]
pub struct ExchangeBindOkArgs {}

#[derive(Debug)]
pub struct QueueDeclareArgs {
    name: String,
    passive: bool,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
    no_wait: bool,
    args: FieldTable,
}

#[derive(Debug)]
pub struct QueueDeclareOkArgs {
    name: String,
    message_count: u32,
    consumer_count: u32,
}

#[derive(Debug)]
pub struct QueueBindArgs {
    queue_name: String,
    exchange_name: String,
    routing_key: String,
    no_wait: bool,
    args: FieldTable,
}

#[derive(Debug)]
pub struct QueueBindOkArgs {}

#[derive(Debug)]
pub struct BasicConsumeArgs {
    queue: String,
    consumer_tag: String,
    no_local: bool,
    no_ack: bool,
    exclusive: bool,
    no_wait: bool,
    args: FieldTable,
}

#[derive(Debug)]
pub struct BasicConsumeOkArgs {
    consumer_tag: String,
}

#[derive(Debug)]
pub struct BasicDeliverArgs {
    consumer_tag: String,
    delivery_tag: String,
    redelivered: bool,
    exchange_name: String,
    routing_key: String,
}

#[derive(Debug)]
pub struct BasicPublishArgs {
    exchange_name: String,
    routing_key: String,
    mandatory: bool,
    immediate: bool,
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

    let args = vec![
        AMQPValue::U8(0),
        AMQPValue::U8(9),
        AMQPValue::FieldTable(Box::new(server_properties)),
        AMQPValue::LongString("PLAIN".into()),
        AMQPValue::LongString("en_US".into()),
    ];

    AMQPFrame::Method(
        channel,
        CONNECTION_START,
        MethodFrameArgs::Other(Box::new(args)),
    )
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

    let args = vec![
        AMQPValue::FieldTable(Box::new(client_properties)),
        AMQPValue::SimpleString("PLAIN".into()),
        AMQPValue::LongString(auth_string),
        AMQPValue::SimpleString("en_US".into()),
    ];

    AMQPFrame::Method(
        0,
        CONNECTION_START_OK,
        MethodFrameArgs::Other(Box::new(args)),
    )
}

pub fn connection_tune(channel: u16) -> AMQPFrame {
    let args = vec![
        AMQPValue::U16(2047),
        AMQPValue::U32(131_072),
        AMQPValue::U16(60),
    ];

    AMQPFrame::Method(
        channel,
        CONNECTION_TUNE,
        MethodFrameArgs::Other(Box::new(args)),
    )
}

pub fn connection_tune_ok(channel: u16) -> AMQPFrame {
    let args = vec![
        AMQPValue::U16(2047),
        AMQPValue::U32(131_072),
        AMQPValue::U16(60),
    ];

    AMQPFrame::Method(
        channel,
        CONNECTION_TUNE_OK,
        MethodFrameArgs::Other(Box::new(args)),
    )
}

impl Default for ConnectionOpenArgs {
    fn default() -> Self {
        ConnectionOpenArgs {
            virtual_host: "/".into(),
            insist: true,
        }
    }
}

pub fn connection_open(channel: u16, virtual_host: String) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        CONNECTION_OPEN,
        MethodFrameArgs::ConnectionOpen(ConnectionOpenArgs {
            virtual_host: virtual_host,
            ..Default::default()
        }),
    )
}

pub fn connection_open_ok(channel: u16) -> AMQPFrame {
    let args = vec![AMQPValue::SimpleString("".into())];

    AMQPFrame::Method(
        channel,
        CONNECTION_OPEN_OK,
        MethodFrameArgs::Other(Box::new(args)),
    )
}

pub fn connection_close(channel: u16) -> AMQPFrame {
    let args = vec![
        AMQPValue::U16(200),
        AMQPValue::SimpleString("Normal shutdown".into()),
        AMQPValue::U16(0),
        AMQPValue::U16(0),
    ];

    AMQPFrame::Method(
        channel,
        CONNECTION_CLOSE,
        MethodFrameArgs::Other(Box::new(args)),
    )
}

pub fn connection_close_ok(channel: u16) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        CONNECTION_CLOSE_OK,
        MethodFrameArgs::Other(Box::new(vec![])),
    )
}

pub fn channel_open(channel: u16) -> AMQPFrame {
    let args = vec![AMQPValue::SimpleString("".into())];

    AMQPFrame::Method(
        channel,
        CHANNEL_OPEN,
        MethodFrameArgs::Other(Box::new(args)),
    )
}

pub fn channel_open_ok(channel: u16) -> AMQPFrame {
    let args = vec![AMQPValue::LongString("".into())];

    AMQPFrame::Method(
        channel,
        CHANNEL_OPEN_OK,
        MethodFrameArgs::Other(Box::new(args)),
    )
}

pub fn exchange_declare(channel: u16, exchange_name: String, exchange_type: String) -> AMQPFrame {
    let args = vec![
        AMQPValue::U16(0),
        AMQPValue::SimpleString(exchange_name),
        AMQPValue::SimpleString(exchange_type),
        AMQPValue::U8(0), // bits are: x, x, x, nowait, internal, autodelete, durable, passive
        AMQPValue::EmptyFieldTable,
    ];

    AMQPFrame::Method(
        channel,
        EXCHANGE_DECLARE,
        MethodFrameArgs::Other(Box::new(args)),
    )
}

pub fn exchange_declare_ok(channel: u16) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        EXCHANGE_DECLARE_OK,
        MethodFrameArgs::Other(Box::new(vec![])),
    )
}

pub fn queue_bind(
    channel: u16,
    queue_name: String,
    exchange_name: String,
    routing_key: String,
) -> AMQPFrame {
    let args = vec![
        AMQPValue::U16(0),
        AMQPValue::SimpleString(queue_name),
        AMQPValue::SimpleString(exchange_name),
        AMQPValue::SimpleString(routing_key),
        AMQPValue::U8(0), // xxxxxxx, nowait
        AMQPValue::EmptyFieldTable,
    ];

    AMQPFrame::Method(channel, QUEUE_BIND, MethodFrameArgs::Other(Box::new(args)))
}

pub fn queue_bind_ok(channel: u16) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        QUEUE_BIND_OK,
        MethodFrameArgs::Other(Box::new(vec![])),
    )
}

pub fn queue_declare(channel: u16, queue_name: String) -> AMQPFrame {
    let args = vec![
        AMQPValue::U16(0),
        AMQPValue::SimpleString(queue_name),
        AMQPValue::U8(0), // xxx nowait, autodelete, exclusive, durable, passive
        AMQPValue::EmptyFieldTable,
    ];

    AMQPFrame::Method(
        channel,
        QUEUE_DECLARE,
        MethodFrameArgs::Other(Box::new(args)),
    )
}

pub fn queue_declare_ok(
    channel: u16,
    queue_name: String,
    message_count: u32,
    consumer_count: u32,
) -> AMQPFrame {
    let args = vec![
        AMQPValue::SimpleString(queue_name),
        AMQPValue::U32(message_count),
        AMQPValue::U32(consumer_count),
    ];

    AMQPFrame::Method(
        channel,
        QUEUE_DECLARE_OK,
        MethodFrameArgs::Other(Box::new(args)),
    )
}

pub fn basic_consume(channel: u16, queue_name: String, consumer_tag: String) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        BASIC_CONSUME,
        MethodFrameArgs::Other(Box::new(vec![
            AMQPValue::U16(0),
            AMQPValue::SimpleString(queue_name),
            AMQPValue::SimpleString(consumer_tag),
            AMQPValue::U8(0x02), // no ack = true
            AMQPValue::EmptyFieldTable,
        ])),
    )
}

pub fn basic_consume_ok(channel: u16, consumer_tag: String) -> AMQPFrame {
    let args = Box::new(vec![AMQPValue::SimpleString(consumer_tag)]);

    AMQPFrame::Method(channel, BASIC_CONSUME_OK, MethodFrameArgs::Other(args))
}

pub fn basic_deliver(
    channel: u16,
    consumer_tag: String,
    delivery_tag: u64,
    flags: u8,
    exchange_name: String,
    queue_name: String,
) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        BASIC_DELIVER,
        MethodFrameArgs::Other(Box::new(vec![
            AMQPValue::SimpleString(consumer_tag),
            AMQPValue::U64(delivery_tag),
            AMQPValue::U8(flags),
            AMQPValue::SimpleString(exchange_name),
            AMQPValue::SimpleString(queue_name),
        ])),
    )
}

pub fn basic_publish(channel: u16, exchange_name: String, routing_key: String) -> AMQPFrame {
    let args = vec![
        AMQPValue::U16(0),
        AMQPValue::SimpleString(exchange_name),
        AMQPValue::SimpleString(routing_key),
        AMQPValue::U8(0), // bits xxxxxx immediate, mandatory
    ];

    AMQPFrame::Method(
        channel,
        BASIC_PUBLISH,
        MethodFrameArgs::Other(Box::new(args)),
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
