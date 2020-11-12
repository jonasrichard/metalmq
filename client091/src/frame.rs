use crate::codec::{AMQPType};

pub const ConnectionStart: u32 = 0x000A000A;
pub const ConnectionStartOk: u32 = 0x000A000B;

// Implement enum or lookup table to avoid vec! allocations
pub fn get_method_frame_args_list(class: u16, method: u16) -> Vec<AMQPType> {
    match ((class as u32) << 16) | method as u32 {
        ConnectionStart =>
            vec![AMQPType::U8, AMQPType::U8, AMQPType::FieldTable, AMQPType::LongString, AMQPType::LongString],
        ConnectionStartOk =>
            vec![],
        mc =>
            panic!("Unsupported class+method {:08X}", mc)
    }
}
