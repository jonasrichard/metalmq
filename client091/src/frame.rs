use crate::codec::{AMQPType};

pub const ConnectionStart: u32 = 0x000A000A;
pub const ConnectionStartOk: u32 = 0x000A000B;
pub const ConnectionTune: u32 = 0x000A001E;

// Implement enum or lookup table to avoid vec! allocations
pub fn get_method_frame_args_list(class: u16, method: u16) -> Vec<AMQPType> {
    match ((class as u32) << 16) | method as u32 {
        ConnectionStart =>
            vec![AMQPType::U8, AMQPType::U8, AMQPType::FieldTable, AMQPType::LongString, AMQPType::LongString],
        ConnectionTune =>
            vec![AMQPType::U16, AMQPType::U32, AMQPType::U16],
        mc =>
            panic!("Unsupported class+method {:08X}", mc)
    }
}
