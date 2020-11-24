use crate::codec::{AMQPFieldValue, AMQPFrame, AMQPType, AMQPValue};

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
        EXCHANGE_DECLARE_OK,
        QUEUE_DECLARE_OK,
        QUEUE_BIND_OK
    ];
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

// TODO it should be crate pub!
/// Check if frame comes from the server, so the client needs to send feedback
pub fn from_server(frame: &AMQPFrame) -> bool {
    match frame {
        AMQPFrame::Method(_, class_method, _) =>
            FROM_SERVER.contains(&class_method),
        _ =>
            false
    }
}

pub fn connection_start(channel: u16) -> AMQPFrame {
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

    AMQPFrame::Method(channel, CONNECTION_START, Box::new(args))
}

pub fn connection_start_ok(channel: u16) -> AMQPFrame {
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

    AMQPFrame::Method(channel, CONNECTION_START_OK, Box::new(args))
}

pub fn connection_tune(channel: u16) -> AMQPFrame {
    let args = vec![
        AMQPValue::U16(2047),
        AMQPValue::U32(131_072),
        AMQPValue::U16(60),
    ];

    AMQPFrame::Method(channel, CONNECTION_TUNE, Box::new(args))
}

pub fn connection_tune_ok(channel: u16) -> AMQPFrame {
    let args = vec![
        AMQPValue::U16(2047),
        AMQPValue::U32(131_072),
        AMQPValue::U16(60),
    ];

    AMQPFrame::Method(channel, CONNECTION_TUNE_OK, Box::new(args))
}

pub fn connection_open(channel: u16, virtual_host: String) -> AMQPFrame {
    let args = vec![
        AMQPValue::SimpleString(virtual_host),
        AMQPValue::SimpleString("".into()),
        AMQPValue::U8(1u8)
    ];

    AMQPFrame::Method(channel, CONNECTION_OPEN, Box::new(args))
}

pub fn connection_open_ok(channel: u16) -> AMQPFrame {
    let args = vec![
        AMQPValue::SimpleString("".into())
    ];

    AMQPFrame::Method(channel, CONNECTION_OPEN_OK, Box::new(args))
}

pub fn connection_close(channel: u16) -> AMQPFrame {
    let args = vec![
        AMQPValue::U16(200),
        AMQPValue::SimpleString("Normal shutdown".into()),
        AMQPValue::U16(0),
        AMQPValue::U16(0)
    ];

    AMQPFrame::Method(channel, CONNECTION_CLOSE, Box::new(args))
}

pub fn connection_close_ok(channel: u16) -> AMQPFrame {
    AMQPFrame::Method(channel, CONNECTION_CLOSE_OK, Box::new(vec![]))
}

pub fn channel_open_ok(channel: u16) -> AMQPFrame {
    AMQPFrame::Method(channel, CHANNEL_OPEN_OK, Box::new(vec![AMQPValue::LongString("".into())]))
}

pub fn exchange_declare(channel: u16, exchange_name: String, exchange_type: String) -> AMQPFrame {
    let args = vec![
        AMQPValue::U16(0),
        AMQPValue::SimpleString(exchange_name),
        AMQPValue::SimpleString(exchange_type),
        AMQPValue::U8(0), // bits are: x, x, x, nowait, internal, autodelete, durable, passive
        AMQPValue::FieldTable(Box::new(vec![]))
    ];

    AMQPFrame::Method(channel, EXCHANGE_DECLARE, Box::new(args))
}

pub fn exchange_declare_ok(channel: u16) -> AMQPFrame {
    AMQPFrame::Method(channel, EXCHANGE_DECLARE_OK, Box::new(vec![]))
}

pub fn queue_bind(channel: u16, queue_name: String, exchange_name: String, routing_key: String) -> AMQPFrame {
    let args = vec![
        AMQPValue::U16(0),
        AMQPValue::SimpleString(queue_name),
        AMQPValue::SimpleString(exchange_name),
        AMQPValue::SimpleString(routing_key),
        AMQPValue::U8(0), // xxxxxxx, nowait
        AMQPValue::FieldTable(Box::new(vec![]))
    ];

    AMQPFrame::Method(channel, QUEUE_BIND, Box::new(args))
}

pub fn queue_bind_ok(channel: u16) -> AMQPFrame {
    AMQPFrame::Method(channel, QUEUE_BIND_OK, Box::new(vec![]))
}

pub fn queue_declare(channel: u16, queue_name: String) -> AMQPFrame {
    let args = vec![
        AMQPValue::U16(0),
        AMQPValue::SimpleString(queue_name),
        AMQPValue::U8(0), // xxx nowait, autodelete, exclusive, durable, passive
        AMQPValue::FieldTable(Box::new(vec![]))
    ];

    AMQPFrame::Method(channel, QUEUE_DECLARE, Box::new(args))
}

pub fn queue_declare_ok(channel: u16, queue_name: String, message_count: u32, consumer_count: u32) -> AMQPFrame {
    let args = vec![
        AMQPValue::SimpleString(queue_name),
        AMQPValue::U32(message_count),
        AMQPValue::U32(consumer_count)
    ];

    AMQPFrame::Method(channel, QUEUE_DECLARE_OK, Box::new(args))
}

pub fn basic_publish(channel: u16, exchange_name: String, routing_key: String) -> AMQPFrame {
    let args = vec![
        AMQPValue::U16(0),
        AMQPValue::SimpleString(exchange_name),
        AMQPValue::SimpleString(routing_key),
        AMQPValue::U8(0)  // bits xxxxxx immediate, mandatory
    ];

    AMQPFrame::Method(channel, BASIC_PUBLISH, Box::new(args))
}

pub fn content_header(channel: u16, size: u64) -> AMQPFrame {
    AMQPFrame::ContentHeader(channel, 0x003C, 0, size, 0x0000, Box::new(vec![]))
}

pub fn content_body(channel: u16, payload: &[u8]) -> AMQPFrame {
    AMQPFrame::ContentBody(channel, Box::new(payload.into()))
}
