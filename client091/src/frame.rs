use crate::codec::{AMQPFrame, AMQPType, AMQPValue};

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

pub const BASIC_PUBLISH: u32 = 0x003C0028;

lazy_static! {
    static ref FROM_SERVER: Vec<u32> = vec![
        CONNECTION_START,
        CONNECTION_TUNE,
        CONNECTION_OPEN_OK,
        CONNECTION_CLOSE_OK,
        CHANNEL_OPEN_OK,
        EXCHANGE_DECLARE_OK
    ];
}

// Implement enum or lookup table to avoid vec! allocations
pub fn get_method_frame_args_list(class_method: u32) -> Vec<AMQPType> {
    match class_method {
        CONNECTION_START =>
            vec![AMQPType::U8, AMQPType::U8, AMQPType::FieldTable, AMQPType::LongString, AMQPType::LongString],
        CONNECTION_TUNE =>
            vec![AMQPType::U16, AMQPType::U32, AMQPType::U16],
        CONNECTION_OPEN =>
            vec![AMQPType::SimpleString, AMQPType::SimpleString, AMQPType::U8],
        CONNECTION_OPEN_OK =>
            vec![AMQPType::SimpleString],
        CONNECTION_CLOSE_OK =>
            vec![],
        CHANNEL_OPEN_OK =>
            vec![AMQPType::LongString],
        EXCHANGE_DECLARE_OK =>
            vec![],
        mc =>
            panic!("Unsupported class+method {:08X}", mc)
    }
}
/// Check if frame comes from the server, so the client needs to send feedback
pub(crate) fn from_server(frame: &AMQPFrame) -> bool {
    match frame {
        AMQPFrame::Method(_, class_method, _) =>
            FROM_SERVER.contains(&class_method),
        _ =>
            false
    }
}

pub(crate) fn exchange_declare(channel: u16, exchange_name: String, exchange_type: String) -> AMQPFrame {
    let args = vec![
        AMQPValue::U16(0),
        AMQPValue::SimpleString(exchange_name),
        AMQPValue::SimpleString(exchange_type),
        AMQPValue::U8(0), // bits are: x, x, x, nowait, internal, autodelete, durable, passive
        AMQPValue::FieldTable(Box::new(vec![]))
    ];

    AMQPFrame::Method(channel, EXCHANGE_DECLARE, Box::new(args))
}

pub(crate) fn basic_publish(channel: u16, exchange_name: String, routing_key: String) -> AMQPFrame {
    let args = vec![
        AMQPValue::U16(0),
        AMQPValue::SimpleString(exchange_name),
        AMQPValue::SimpleString(routing_key),
        AMQPValue::U8(0)  // bits xxxxxx immediate, mandatory
    ];

    AMQPFrame::Method(channel, BASIC_PUBLISH, Box::new(args))
}

pub(crate) fn content_header(channel: u16, size: u64) -> AMQPFrame {
    AMQPFrame::ContentHeader(channel, 0x003C, 0, size, 0x0000, Box::new(vec![]))
}

pub(crate) fn content_body(channel: u16, payload: &[u8]) -> AMQPFrame {
    AMQPFrame::ContentBody(channel, Box::new(payload.into()))
}
