use crate::{frame_error, Result};

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

pub const BASIC_PUBLISH: u32 = 0x003C0028;

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
        QUEUE_BIND_OK
    ];
}

pub type Channel = u16;
pub type ClassMethod = u32;
pub type ClassId = u16;
pub type Weight = u16;

#[derive(Clone, Debug)]
pub enum AMQPFrame {
    AMQPHeader,
    Method(Box<MethodFrame>),
    ContentHeader(Box<ContentHeaderFrame>),
    ContentBody(Box<ContentBodyFrame>)
}

#[derive(Clone, Debug)]
pub struct MethodFrame {
    pub channel: Channel,
    pub class_method: ClassMethod,
    pub args: Vec<AMQPValue>
}

#[derive(Clone, Debug)]
pub struct ContentHeaderFrame {
    pub channel: Channel,
    pub class_id: ClassId,
    pub weight: Weight,
    pub body_size: u64,
    pub prop_flags: u16,  // we need to elaborate the properties here
    pub args: Vec<AMQPValue>
}

#[derive(Clone, Debug)]
pub struct ContentBodyFrame {
    pub channel: Channel,
    pub body: Vec<u8>
}

#[derive(Debug)]
pub enum AMQPType {
    U8,
    U16,
    U32,
    SimpleString,
    LongString,
    FieldTable
}

#[derive(Clone, Debug)]
pub enum AMQPValue {
//    Bool(bool),
    U8(u8),
    U16(u16),
    U32(u32),
    SimpleString(String),
    LongString(String),
    FieldTable(Box<Vec<(String, AMQPFieldValue)>>)
}

#[derive(Clone, Debug)]
pub enum AMQPFieldValue {
    Bool(bool),
//    SimpleString(String),
    LongString(String),
    FieldTable(Box<Vec<(String, AMQPFieldValue)>>)
}

// Implement enum or lookup table to avoid vec! allocations
pub fn get_method_frame_args_list(class_method: u32) -> Vec<AMQPType> {
    match class_method {
        CONNECTION_START =>
            vec![AMQPType::U8, AMQPType::U8, AMQPType::FieldTable, AMQPType::LongString, AMQPType::LongString],
        CONNECTION_START_OK =>
            vec![AMQPType::FieldTable, AMQPType::SimpleString, AMQPType::LongString, AMQPType::SimpleString],
        CONNECTION_TUNE =>
            vec![AMQPType::U16, AMQPType::U32, AMQPType::U16],
        CONNECTION_TUNE_OK =>
            vec![AMQPType::U16, AMQPType::U32, AMQPType::U16],
        CONNECTION_OPEN =>
            vec![AMQPType::SimpleString, AMQPType::SimpleString, AMQPType::U8],
        CONNECTION_OPEN_OK =>
            vec![AMQPType::SimpleString],
        CONNECTION_CLOSE =>
            vec![AMQPType::U16, AMQPType::SimpleString, AMQPType::U16, AMQPType::U16],
        CONNECTION_CLOSE_OK =>
            vec![],
        CHANNEL_OPEN =>
            vec![AMQPType::SimpleString],
        CHANNEL_OPEN_OK =>
            vec![AMQPType::LongString],
        CHANNEL_CLOSE =>
            vec![AMQPType::U16, AMQPType::SimpleString, AMQPType::U16, AMQPType::U16],
        EXCHANGE_DECLARE =>
            vec![AMQPType::U16, AMQPType::SimpleString, AMQPType::SimpleString, AMQPType::U8, AMQPType::FieldTable],
        EXCHANGE_DECLARE_OK =>
            vec![],
        QUEUE_BIND =>
            vec![AMQPType::U16, AMQPType::SimpleString, AMQPType::SimpleString, AMQPType::SimpleString,
                 AMQPType::U8, AMQPType::FieldTable],
        QUEUE_BIND_OK =>
            vec![],
        QUEUE_DECLARE =>
            vec![AMQPType::U16, AMQPType::SimpleString, AMQPType::U8, AMQPType::FieldTable],
        QUEUE_DECLARE_OK =>
            vec![AMQPType::SimpleString, AMQPType::U32, AMQPType::U32],
        BASIC_PUBLISH =>
            vec![AMQPType::U16, AMQPType::SimpleString, AMQPType::SimpleString, AMQPType::U8],
        mc =>
            panic!("Unsupported class+method {:08X}", mc)
    }
}

// Check if frame comes from the server, so the client needs to send feedback
//pub fn _from_server(frame: &AMQPFrame) -> bool {
//    match frame {
//        AMQPFrame::Method(method_frame) =>
//            FROM_SERVER.contains(&method_frame.class_method),
//        _ =>
//            false
//    }
//}

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

/// Convenience function for getting string value from argument list.
pub fn arg_as_string(args: Vec<AMQPValue>, index: usize) -> Result<String> {
    if let Some(arg) = args.get(index) {
        if let AMQPValue::SimpleString(s) = arg {
            return Ok(s.clone())
        }
        if let AMQPValue::LongString(s) = arg {
            return Ok(s.clone())
        }

        return frame_error!(3, "Cannot convert arg to string")
    }

    frame_error!(4, "Arg index out of bound")
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

pub fn arg_as_field_table(args: Vec<AMQPValue>, index: usize) -> Result<Vec<(String, AMQPFieldValue)>> {
    if let Some(arg) = args.get(index) {
        if let AMQPValue::FieldTable(v) = arg {
            return Ok(v.to_vec())
        }

        return frame_error!(3, "Cannot convert arg to field table")
    }

    frame_error!(4, "Arg index out of bound")
}

//pub fn get_string(ft: Vec<(String, AMQPFieldValue)>, name: String) -> Result<String> {
//    let field_value = field_table_lookup(&ft, name)?;
//
//    if let AMQPFieldValue::LongString(s) = field_value {
//        return Ok(s)
//    }
//
//    frame_error!(6, "Cannot convert field value to string")
//}

pub fn connection_start(channel: u16) -> MethodFrame {
    let mut capabilities = Vec::<(String, AMQPFieldValue)>::new();
    capabilities.push(("publisher_confirms".into(), AMQPFieldValue::Bool(true)));
    capabilities.push(("exchange_exchange_bindings".into(), AMQPFieldValue::Bool(true)));
    capabilities.push(("basic.nack".into(), AMQPFieldValue::Bool(true)));
    capabilities.push(("consumer_cancel_notify".into(), AMQPFieldValue::Bool(true)));
    capabilities.push(("connection.blocked".into(), AMQPFieldValue::Bool(true)));
    capabilities.push(("consumer_priorities".into(), AMQPFieldValue::Bool(true)));
    capabilities.push(("authentication_failure_close".into(), AMQPFieldValue::Bool(true)));
    capabilities.push(("per_consumer_qos".into(), AMQPFieldValue::Bool(true)));
    capabilities.push(("direct_reply_to".into(), AMQPFieldValue::Bool(true)));

    let mut server_properties = Vec::<(String, AMQPFieldValue)>::new();
    server_properties.push(("capabilities".into(), AMQPFieldValue::FieldTable(Box::new(capabilities))));
    server_properties.push(("product".into(), AMQPFieldValue::LongString("IronMQ server".into())));
    server_properties.push(("version".into(), AMQPFieldValue::LongString("0.1.0".into())));

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

pub fn connection_start_ok(channel: u16) -> MethodFrame {
    let mut capabilities = Vec::<(String, AMQPFieldValue)>::new();

    capabilities.push(("authentication_failure_on_close".into(), AMQPFieldValue::Bool(true)));
    capabilities.push(("basic.nack".into(), AMQPFieldValue::Bool(true)));
    capabilities.push(("connection.blocked".into(), AMQPFieldValue::Bool(true)));
    capabilities.push(("consumer_cancel_notify".into(), AMQPFieldValue::Bool(true)));
    capabilities.push(("publisher_confirms".into(), AMQPFieldValue::Bool(true)));

    let mut client_properties = Vec::<(String, AMQPFieldValue)>::new();

    client_properties.push(("product".into(), AMQPFieldValue::LongString("ironmq-client".into())));
    client_properties.push(("platform".into(), AMQPFieldValue::LongString("Rust".into())));
    client_properties.push(("capabilities".into(), AMQPFieldValue::FieldTable(Box::new(capabilities))));
    client_properties.push(("version".into(), AMQPFieldValue::LongString("0.1.0".into())));

    let mut auth = Vec::new();
    auth.extend_from_slice(b"\x00guest\x00guest");

    let auth_string = String::from_utf8(auth).unwrap();

    let args = vec![
        AMQPValue::FieldTable(Box::new(client_properties)),
        AMQPValue::SimpleString("PLAIN".into()),
        AMQPValue::LongString(auth_string),
        AMQPValue::SimpleString("en_US".into()),
    ];

    MethodFrame {
        channel: channel,
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
        AMQPValue::FieldTable(Box::new(vec![]))
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
        AMQPValue::FieldTable(Box::new(vec![]))
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
        AMQPValue::FieldTable(Box::new(vec![]))
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
