use crate::{frame_error, Result};
use std::collections::HashMap;
use std::fmt;

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

lazy_static! {
    static ref FROM_SERVER: Vec<u32> = vec![
        CONNECTION_START,
        CONNECTION_TUNE,
        CONNECTION_OPEN_OK,
        CONNECTION_CLOSE_OK,
        CHANNEL_OPEN_OK,
        CHANNEL_CLOSE,
        EXCHANGE_DECLARE_OK,
        QUEUE_DECLARE_OK,
        QUEUE_BIND_OK,
        BASIC_CONSUME_OK
    ];
}

pub type Channel = u16;
pub type ClassMethod = u32;
pub type ClassId = u16;
pub type Weight = u16;

// #[derive(Clone)]
#[derive(Debug)]
pub enum AMQPFrame {
    AMQPHeader,
    Method(Box<MethodFrame>),
    ContentHeader(Box<ContentHeaderFrame>),
    ContentBody(Box<ContentBodyFrame>),
    Heartbeat(Channel)
}

//#[derive(Clone)]
pub struct MethodFrame {
    pub channel: Channel,
    pub class_method: ClassMethod,
    pub args: Vec<AMQPValue>
}

impl fmt::Debug for MethodFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MethodFrame {{ channel={}, class_method={:08X}, args={:?} }}", &self.channel,
            &self.class_method, &self.args)
    }
}

//#[derive(Clone)]
#[derive(Debug)]
pub struct ContentHeaderFrame {
    pub channel: Channel,
    pub class_id: ClassId,
    pub weight: Weight,
    pub body_size: u64,
    pub prop_flags: u16,  // we need to elaborate the properties here
    pub args: Vec<AMQPValue>
}

//#[derive(Clone)]
#[derive(Debug)]
pub struct ContentBodyFrame {
    pub channel: Channel,
    pub body: Vec<u8>
}

#[derive(Debug)]
pub enum AMQPType {
    U8,
    U16,
    U32,
    U64,
    SimpleString,
    LongString,
    FieldTable
}

/// Type alias for inner type of field value.
type FieldTable = HashMap<String, AMQPFieldValue>;

//#[derive(Clone)]
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
    FieldTable(Box<FieldTable>)
}

//#[derive(Clone)]
#[derive(Debug)]
pub enum AMQPFieldValue {
    Bool(bool),
//    SimpleString(String),
    LongString(String),
    EmptyFieldTable,
    FieldTable(Box<FieldTable>)
}

macro_rules! t_u8 { () => { AMQPType::U8 } }
macro_rules! t_u16 { () => { AMQPType::U16 } }
macro_rules! t_u32 { () => { AMQPType::U32 } }
macro_rules! t_u64 { () => { AMQPType::U64 } }
macro_rules! t_ls{ () => { AMQPType::LongString } }
macro_rules! t_ss { () => { AMQPType::SimpleString } }
macro_rules! t_ft { () => { AMQPType::FieldTable } }

// Implement enum or lookup table to avoid vec! allocations
pub fn get_method_frame_args_list(class_method: u32) -> Vec<AMQPType> {
    match class_method {
        CONNECTION_START =>
            vec![t_u8!(), t_u8!(), t_ft!(), t_ls!(), t_ls!()],
        CONNECTION_START_OK =>
            vec![t_ft!(), t_ss!(), t_ls!(), t_ss!()],
        CONNECTION_TUNE =>
            vec![t_u16!(), t_u32!(), t_u16!()],
        CONNECTION_TUNE_OK =>
            vec![t_u16!(), t_u32!(), t_u16!()],
        CONNECTION_OPEN =>
            vec![t_ss!(), t_ss!(), t_u8!()],
        CONNECTION_OPEN_OK =>
            vec![t_ss!()],
        CONNECTION_CLOSE =>
            vec![t_u16!(), t_ss!(), t_u16!(), t_u16!()],
        CONNECTION_CLOSE_OK =>
            vec![],
        CHANNEL_OPEN =>
            vec![t_ss!()],
        CHANNEL_OPEN_OK =>
            vec![t_ls!()],
        CHANNEL_CLOSE =>
            vec![t_u16!(), t_ss!(), t_u16!(), t_u16!()],
        EXCHANGE_DECLARE =>
            vec![t_u16!(), t_ss!(), t_ss!(), t_u8!(), t_ft!()],
        EXCHANGE_DECLARE_OK =>
            vec![],
        QUEUE_BIND =>
            vec![t_u16!(), t_ss!(), t_ss!(), t_ss!(),
                 t_u8!(), t_ft!()],
        QUEUE_BIND_OK =>
            vec![],
        QUEUE_DECLARE =>
            vec![t_u16!(), t_ss!(), t_u8!(), t_ft!()],
        QUEUE_DECLARE_OK =>
            vec![t_ss!(), t_u32!(), t_u32!()],
        BASIC_CONSUME =>
            vec![t_u16!(), t_ss!(), t_ss!(), t_u8!(), t_ft!()],
        BASIC_CONSUME_OK =>
            vec![t_ss!()],
        BASIC_PUBLISH =>
            vec![t_u16!(), t_ss!(), t_ss!(), t_u8!()],
        BASIC_DELIVER =>
            vec![t_ss!(), t_u64!(), t_u8!(), t_ss!(), t_ss!()],
        mc =>
            panic!("Unsupported class+method {:08X}", mc)
    }
}

impl From<MethodFrame> for AMQPFrame {
    fn from(mf: MethodFrame) -> AMQPFrame {
        AMQPFrame::Method(Box::new(mf))
    }
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
        AMQPValue::SimpleString(s) =>
            Ok(s),
        AMQPValue::LongString(s) =>
            Ok(s),
        _ =>
            frame_error!(3, "Cannot convert to string")
    }
}

pub fn arg_as_u8(args: Vec<AMQPValue>, index: usize) -> Result<u8> {
    if let Some(arg) = args.get(index) {
        if let AMQPValue::U8(v) = arg {
            return Ok(*v)
        }

        return frame_error!(3, "Cannot convert arg to u8")
    }

    frame_error!(4, "Arg index out of bound")
}

pub fn arg_as_u16(args: Vec<AMQPValue>, index: usize) -> Result<u16> {
    if let Some(arg) = args.get(index) {
        if let AMQPValue::U16(v) = arg {
            return Ok(*v)
        }

        return frame_error!(3, "Cannot convert arg to u16")
    }

    frame_error!(4, "Arg index out of bound")
}

pub fn arg_as_u32(args: Vec<AMQPValue>, index: usize) -> Result<u32> {
    if let Some(arg) = args.get(index) {
        if let AMQPValue::U32(v) = arg {
            return Ok(*v)
        }

        return frame_error!(3, "Cannot convert arg to u32")
    }

    frame_error!(4, "Arg index out of bound")
}

pub fn arg_as_u64(args: Vec<AMQPValue>, index: usize) -> Result<u64> {
    if let Some(arg) = args.get(index) {
        if let AMQPValue::U64(v) = arg {
            return Ok(*v)
        }

        return frame_error!(3, "Cannot convert arg to u64")
    }

    frame_error!(4, "Arg index out of bound")
}

//pub fn arg_as_field_table(args: Vec<AMQPValue>, index: usize) -> Result<FieldTable> {
//    if let Some(arg) = args.get(index) {
//        if let AMQPValue::FieldTable(v) = arg {
//            return Ok(**v)
//        }
//
//        return frame_error!(3, "Cannot convert arg to field table")
//    }
//
//    frame_error!(4, "Arg index out of bound")
//}

//pub fn get_string(ft: FieldTable, name: String) -> Result<String> {
//    if let Some(value) = ft.get(&name) {
//        if let AMQPFieldValue::LongString(s) = value {
//            return Ok(s.clone())
//        }
//    }
//
//    frame_error!(6, "Cannot convert field value to string")
//}

pub fn get_bool(ft: FieldTable, name: String) -> Result<bool> {
    if let Some(value) = ft.get(&name) {
        if let AMQPFieldValue::Bool(v) = value {
            return Ok(*v)
        }
    }

    frame_error!(6, "Cannot convert field value to bool")
}

pub fn connection_start(channel: u16) -> MethodFrame {
    let mut capabilities = FieldTable::new();

    capabilities.insert("publisher_confirms".into(), AMQPFieldValue::Bool(true));
    capabilities.insert("exchange_exchange_bindings".into(), AMQPFieldValue::Bool(true));
    capabilities.insert("basic.nack".into(), AMQPFieldValue::Bool(true));
    capabilities.insert("consumer_cancel_notify".into(), AMQPFieldValue::Bool(true));
    capabilities.insert("connection.blocked".into(), AMQPFieldValue::Bool(true));
    capabilities.insert("consumer_priorities".into(), AMQPFieldValue::Bool(true));
    capabilities.insert("authentication_failure_close".into(), AMQPFieldValue::Bool(true));
    capabilities.insert("per_consumer_qos".into(), AMQPFieldValue::Bool(true));
    capabilities.insert("direct_reply_to".into(), AMQPFieldValue::Bool(true));

    let mut server_properties = FieldTable::new();

    server_properties.insert("capabilities".into(), AMQPFieldValue::FieldTable(Box::new(capabilities)));
    server_properties.insert("product".into(), AMQPFieldValue::LongString("IronMQ server".into()));
    server_properties.insert("version".into(), AMQPFieldValue::LongString("0.1.0".into()));

    let args = vec![
        AMQPValue::U8(0),
        AMQPValue::U8(9),
        AMQPValue::FieldTable(Box::new(server_properties)),
        AMQPValue::LongString("PLAIN".into()),
        AMQPValue::LongString("en_US".into())
    ];

    MethodFrame {
        channel: channel,
        class_method: CONNECTION_START,
        args: args
    }
}

// TODO here will be an Authentication enum with the different possibilities
pub fn connection_start_ok(username: &str, password: &str, capabilities: FieldTable) -> MethodFrame {
    let mut client_properties = FieldTable::new();

    client_properties.insert("product".into(), AMQPFieldValue::LongString("ironmq-client".into()));
    client_properties.insert("platform".into(), AMQPFieldValue::LongString("Rust".into()));
    client_properties.insert("capabilities".into(), AMQPFieldValue::FieldTable(Box::new(capabilities)));
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

    MethodFrame {
        channel: 0,
        class_method: CONNECTION_START_OK,
        args: args
    }
}

pub fn connection_tune(channel: u16) -> MethodFrame {
    let args = vec![
        AMQPValue::U16(2047),
        AMQPValue::U32(131_072),
        AMQPValue::U16(60),
    ];

    MethodFrame {
        channel: channel,
        class_method: CONNECTION_TUNE,
        args: args
    }
}

pub fn connection_tune_ok(channel: u16) -> MethodFrame {
    let args = vec![
        AMQPValue::U16(2047),
        AMQPValue::U32(131_072),
        AMQPValue::U16(60),
    ];

    MethodFrame {
        channel: channel,
        class_method: CONNECTION_TUNE_OK,
        args: args
    }
}

pub fn connection_open(channel: u16, virtual_host: String) -> MethodFrame {
    let args = vec![
        AMQPValue::SimpleString(virtual_host),
        AMQPValue::SimpleString("".into()),
        AMQPValue::U8(1u8)
    ];

    MethodFrame {
        channel: channel,
        class_method: CONNECTION_OPEN,
        args: args
    }
}

pub fn connection_open_ok(channel: u16) -> MethodFrame {
    let args = vec![
        AMQPValue::SimpleString("".into())
    ];

    MethodFrame {
        channel: channel,
        class_method: CONNECTION_OPEN_OK,
        args: args
    }
}

pub fn connection_close(channel: u16) -> MethodFrame {
    let args = vec![
        AMQPValue::U16(200),
        AMQPValue::SimpleString("Normal shutdown".into()),
        AMQPValue::U16(0),
        AMQPValue::U16(0)
    ];

    MethodFrame {
        channel: channel,
        class_method: CONNECTION_CLOSE,
        args: args
    }
}

pub fn connection_close_ok(channel: u16) -> MethodFrame {
    MethodFrame {
        channel: channel,
        class_method: CONNECTION_CLOSE_OK,
        args: vec![]
    }
}

pub fn channel_open(channel: u16) -> MethodFrame {
    MethodFrame {
        channel: channel,
        class_method: CHANNEL_OPEN,
        args: vec![AMQPValue::SimpleString("".into())]
    }
}

pub fn channel_open_ok(channel: u16) -> MethodFrame {
    MethodFrame {
        channel: channel,
        class_method: CHANNEL_OPEN_OK,
        args: vec![AMQPValue::LongString("".into())]
    }
}

pub fn exchange_declare(channel: u16, exchange_name: String, exchange_type: String) -> MethodFrame {
    let args = vec![
        AMQPValue::U16(0),
        AMQPValue::SimpleString(exchange_name),
        AMQPValue::SimpleString(exchange_type),
        AMQPValue::U8(0), // bits are: x, x, x, nowait, internal, autodelete, durable, passive
        AMQPValue::EmptyFieldTable
    ];

    MethodFrame {
        channel: channel,
        class_method: EXCHANGE_DECLARE,
        args: args
    }
}

pub fn exchange_declare_ok(channel: u16) -> MethodFrame {
    MethodFrame {
        channel: channel,
        class_method: EXCHANGE_DECLARE_OK,
        args: vec![]
    }
}

pub fn queue_bind(channel: u16, queue_name: String, exchange_name: String, routing_key: String) -> MethodFrame {
    let args = vec![
        AMQPValue::U16(0),
        AMQPValue::SimpleString(queue_name),
        AMQPValue::SimpleString(exchange_name),
        AMQPValue::SimpleString(routing_key),
        AMQPValue::U8(0), // xxxxxxx, nowait
        AMQPValue::EmptyFieldTable
    ];

    MethodFrame {
        channel: channel,
        class_method: QUEUE_BIND,
        args: args
    }
}

pub fn queue_bind_ok(channel: u16) -> MethodFrame {
    MethodFrame {
        channel: channel,
        class_method: QUEUE_BIND_OK,
        args: vec![]
    }
}

pub fn queue_declare(channel: u16, queue_name: String) -> MethodFrame {
    let args = vec![
        AMQPValue::U16(0),
        AMQPValue::SimpleString(queue_name),
        AMQPValue::U8(0), // xxx nowait, autodelete, exclusive, durable, passive
        AMQPValue::EmptyFieldTable
    ];

    MethodFrame {
        channel: channel,
        class_method: QUEUE_DECLARE,
        args: args
    }
}

pub fn queue_declare_ok(channel: u16, queue_name: String, message_count: u32, consumer_count: u32) -> MethodFrame {
    let args = vec![
        AMQPValue::SimpleString(queue_name),
        AMQPValue::U32(message_count),
        AMQPValue::U32(consumer_count)
    ];

    MethodFrame {
        channel: channel,
        class_method: QUEUE_DECLARE_OK,
        args: args
    }
}

pub fn basic_consume(channel: u16, queue_name: String, consumer_tag: String) -> MethodFrame {
    MethodFrame {
        channel: channel,
        class_method: BASIC_CONSUME,
        args: vec![
            AMQPValue::U16(0),
            AMQPValue::SimpleString(queue_name),
            AMQPValue::SimpleString(consumer_tag),
            AMQPValue::U8(0x02),  // no ack = true
            AMQPValue::EmptyFieldTable
        ]
    }
}

pub fn basic_consume_ok(channel: u16, consumer_tag: String) -> MethodFrame {
    MethodFrame {
        channel: channel,
        class_method: BASIC_CONSUME_OK,
        args: vec![
            AMQPValue::SimpleString(consumer_tag)
        ]
    }
}

pub fn basic_deliver(channel: u16, consumer_tag: String, delivery_tag: u64, flags: u8,
                     exchange_name: String, queue_name: String) -> MethodFrame {
    MethodFrame {
        channel: channel,
        class_method: BASIC_DELIVER,
        args: vec![
            AMQPValue::SimpleString(consumer_tag),
            AMQPValue::U64(delivery_tag),
            AMQPValue::U8(flags),
            AMQPValue::SimpleString(exchange_name),
            AMQPValue::SimpleString(queue_name)
        ]
    }
}

pub fn basic_publish(channel: u16, exchange_name: String, routing_key: String) -> MethodFrame {
    let args = vec![
        AMQPValue::U16(0),
        AMQPValue::SimpleString(exchange_name),
        AMQPValue::SimpleString(routing_key),
        AMQPValue::U8(0)  // bits xxxxxx immediate, mandatory
    ];

    MethodFrame {
        channel: channel,
        class_method: BASIC_PUBLISH,
        args: args
    }
}

pub fn content_header(channel: u16, size: u64) -> ContentHeaderFrame {
    ContentHeaderFrame {
        channel: channel,
        class_id: 0x003C,
        weight: 0,
        body_size: size,
        prop_flags: 0x0000,
        args: vec![]
    }
}

pub fn content_body(channel: u16, payload: &[u8]) -> ContentBodyFrame {
    ContentBodyFrame {
        channel: channel,
        body: payload.to_vec()
    }
}
