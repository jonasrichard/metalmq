use crate::codec::{AMQPFrame, AMQPType};

pub const CONNECTION_START: u32 = 0x000A000A;
pub const CONNECTION_START_OK: u32 = 0x000A000B;
pub const CONNECTION_TUNE: u32 = 0x000A001E;
pub const CONNECTION_TUNE_OK: u32 = 0x000A001F;
pub const CONNECTION_OPEN: u32 = 0x000A0028;
pub const CONNECTION_OPEN_OK: u32 = 0x000A0029;
pub const CONNECTION_CLOSE: u32 = 0x000A0032;
pub const CONNECTION_CLOSE_OK: u32 = 0x000A0033;

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
        mc =>
            panic!("Unsupported class+method {:08X}", mc)
    }
}

pub fn from_server(frame: &AMQPFrame) -> bool {
    match frame {
        AMQPFrame::Method(_, class_method, _) =>
            if *class_method == CONNECTION_START ||
                    *class_method == CONNECTION_TUNE ||
                    *class_method == CONNECTION_OPEN_OK ||
                    *class_method == CONNECTION_CLOSE_OK {
                true
            } else {
                false
            },
        _ =>
            false
    }
}
