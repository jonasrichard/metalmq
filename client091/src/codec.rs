use crate::frame::*;
use bytes::{Buf, BufMut, BytesMut};
use log::{info};
use tokio_util::codec::{Decoder, Encoder};

pub struct AMQPCodec {
}

type Channel = u16;
type ClassMethod = u32;
type ClassId = u16;
type Weight = u16;
type BodySize = u64;
type PropFlags = u16;

#[derive(Debug)]
pub enum AMQPFrame {
    AMQPHeader,
    Method(Channel, ClassMethod, Box<Vec<AMQPValue>>),
    ContentHeader(Channel, ClassId, Weight, BodySize, PropFlags, Box<Vec<AMQPValue>>),
    ContentBody(Channel, Box<Vec<u8>>)
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

impl Encoder<AMQPFrame> for AMQPCodec {
    type Error = std::io::Error;

    fn encode(&mut self, event: AMQPFrame, mut buf: &mut BytesMut) -> Result<(), Self::Error> {
        info!("[encode] {:?}", event);

        match event {
            AMQPFrame::AMQPHeader =>
                buf.put(&b"AMQP\x00\x00\x09\x01"[..]),

            AMQPFrame::Method(channel, class_method, args) => {
                encode_method_frame(&mut buf, channel, class_method, args.to_vec())
            },

            AMQPFrame::ContentHeader(channel, class_id, weight, body_size, property_flags, properties) =>
                encode_content_header_frame(&mut buf, channel, class_id, weight, body_size,
                                            property_flags, properties.to_vec()),

            AMQPFrame::ContentBody(channel, payload) =>
                encode_content_body_frame(&mut buf, channel, payload.to_vec())
        }
        Ok(())
    }
}

impl Decoder for AMQPCodec {
    type Item = AMQPFrame;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 8 {
            Ok(None)
        } else {
            match src.get_u8() {
                1 => {
                    let channel = src.get_u16();
                    // TODO amqp frame can be u32 but Buf handles only usize buffes
                    let frame_len = src.get_u32() as usize;

                    if src.len() < frame_len {
                        return Ok(None)
                    }

                    let mut frame_buf = src.split_to(frame_len);
                    let frame = decode_method_frame(&mut frame_buf, channel);

                    let _frame_separator = src.get_u8();

                    Ok(Some(frame))
                },
                2 => {
                    let channel = src.get_u16();
                    let frame_len = src.get_u32() as usize;

                    // We don't have the full frame in the buffer
                    if src.len() < frame_len {
                        return Ok(None)
                    }

                    let mut frame_buf = src.split_to(frame_len);
                    let frame = decode_content_header_frame(&mut frame_buf, channel);

                    let _frame_separator = src.get_u8();

                    Ok(Some(frame))
                },
                f =>
                    panic!("Unknown frame {:02X}", f)
            }
        }
    }
}

// TODO have an Error type here, and it should be result<>
fn decode_method_frame(mut src: &mut BytesMut, channel: u16) -> AMQPFrame {
    let class_method = src.get_u32();

    let args_type_list = get_method_frame_args_list(class_method);

    let mut args = Vec::<AMQPValue>::new();

    for arg_type in args_type_list {
        match arg_type {
            AMQPType::U8 =>
                args.push(AMQPValue::U8(src.get_u8())),
            AMQPType::U16 =>
                args.push(AMQPValue::U16(src.get_u16())),
            AMQPType::U32 =>
                args.push(AMQPValue::U32(src.get_u32())),
            AMQPType::SimpleString =>
                args.push(AMQPValue::SimpleString(decode_short_string(&mut src))),
            AMQPType::LongString =>
                args.push(AMQPValue::LongString(decode_long_string(&mut src))),
            AMQPType::FieldTable =>
                args.push(AMQPValue::FieldTable(Box::new(decode_field_table(&mut src)))),
        }
    }

    AMQPFrame::Method(channel, class_method, Box::new(args))
}

fn decode_content_header_frame(mut src: &mut BytesMut, channel: u16) -> AMQPFrame {
    dump(&src);

    let class_id = src.get_u16();
    let weight = src.get_u16();
    let body_size = src.get_u64();
    let property_flags = src.get_u16();
    // TODO property list, it seems that we need to know from the class_id what is the type list

    AMQPFrame::ContentHeader(channel, class_id, weight, body_size, property_flags, Box::new(vec![]))
}

fn decode_value(mut buf: &mut BytesMut) -> AMQPFieldValue {
    match buf.get_u8() {
        b't' => {
            let bool_value = buf.get_u8() != 0;

            AMQPFieldValue::Bool(bool_value)
        },
        b'S' => {
            let string_value = decode_long_string(&mut buf);

            AMQPFieldValue::LongString(string_value)
        },
        b'F' => {
            let table = decode_field_table(&mut buf);

            AMQPFieldValue::FieldTable(Box::new(table))
        },
        t =>
            panic!("Unknown type {}", t)
    }
}

fn decode_short_string(buf: &mut BytesMut) -> String {
    let len = buf.get_u8() as usize;
    let sb = buf.split_to(len);

    String::from_utf8(sb.to_vec()).unwrap()
}

fn decode_long_string(buf: &mut BytesMut) -> String {
    let len = buf.get_u32() as usize;
    let sb = buf.split_to(len);

    String::from_utf8(sb.to_vec()).unwrap()
}

/// Decode a field table
///
/// The buffer points to the beginning of the field table which is a `u32` length
/// information.
fn decode_field_table(buf: &mut BytesMut) -> Vec<(String, AMQPFieldValue)> {
    let ft_len = buf.get_u32() as usize;
    let mut ft_buf = buf.split_to(ft_len);
    let mut table = Vec::new();

    while ft_buf.has_remaining() {
        let field_name = decode_short_string(&mut ft_buf);
        let field_value = decode_value(&mut ft_buf);

        //info!("Field name -> value {} -> {:?}", field_name, field_value);

        table.push((field_name, field_value));
    }

    table
}

fn encode_method_frame(buf: &mut BytesMut, channel: u16, class_method: u32, args: Vec<AMQPValue>) {
    buf.put_u8(1u8);
    buf.put_u16(channel);

    let mut fr_buf = BytesMut::with_capacity(4096);
    fr_buf.put_u32(class_method);

    for arg in args {
        encode_value(&mut fr_buf, arg);
    }

    buf.put_u32(fr_buf.len() as u32);
    buf.put(fr_buf);
    buf.put_u8(0xCE);
}

fn encode_content_header_frame(buf: &mut BytesMut, channel: Channel, class_id: ClassId, weight: Weight,
                               body_size: BodySize, property_flags: PropFlags, properties: Vec<AMQPValue>) {
    buf.put_u8(2u8);
    buf.put_u16(channel);

    let mut fr_buf = BytesMut::with_capacity(4096);
    fr_buf.put_u16(class_id);
    fr_buf.put_u16(weight);
    fr_buf.put_u64(body_size);
    fr_buf.put_u16(property_flags);

    // TODO encode property list here

    buf.put_u32(fr_buf.len() as u32);
    buf.put(fr_buf);
    buf.put_u8(0xCE);
}

fn encode_content_body_frame(buf: &mut BytesMut, channel: Channel, payload: Vec<u8>) {
    buf.put_u8(3u8);
    buf.put_u16(channel);

    let mut fr_buf = BytesMut::with_capacity(payload.len());
    fr_buf.put(payload.as_slice());

    buf.put_u32(fr_buf.len() as u32);
    buf.put(fr_buf);
    buf.put_u8(0xCE);
}

fn encode_value(mut buf: &mut BytesMut, value: AMQPValue) {
    match value {
        AMQPValue::U8(v) =>
            buf.put_u8(v),
        AMQPValue::U16(v) =>
            buf.put_u16(v),
        AMQPValue::U32(v) =>
            buf.put_u32(v),
        AMQPValue::SimpleString(v) =>
            encode_short_string(&mut buf, v),
        AMQPValue::LongString(v) =>
            encode_long_string(&mut buf, v),
        AMQPValue::FieldTable(v) =>
            encode_field_table(&mut buf, v.to_vec()),
    }
}

fn encode_short_string(buf: &mut BytesMut, s: String) {
    // TODO assert! that size is below 256
    buf.put_u8(s.len() as u8);
    buf.put(s.as_bytes());
}

fn encode_long_string(buf: &mut BytesMut, s: String) {
    buf.put_u32(s.len() as u32);
    buf.put(s.as_bytes());
}

fn encode_field_table(buf: &mut BytesMut, ft: Vec<(String, AMQPFieldValue)>) {
    let mut ft_buf = BytesMut::with_capacity(4096);

    for (name, value) in ft {
        encode_short_string(&mut ft_buf, name);

        match value {
            AMQPFieldValue::Bool(v) => {
                ft_buf.put_u8(b't');

                if v {
                    ft_buf.put_u8(1u8);
                } else {
                    ft_buf.put_u8(0u8);
                }
            },
            AMQPFieldValue::LongString(v) => {
                ft_buf.put_u8(b'S');
                ft_buf.put_u32(v.len() as u32);
                ft_buf.put(v.as_bytes());
            },
            AMQPFieldValue::FieldTable(v) => {
                ft_buf.put_u8(b'F');

                encode_field_table(&mut ft_buf, v.to_vec());
            },
        }
    }

    buf.put_u32(ft_buf.len() as u32);
    buf.put(ft_buf);
}

#[allow(dead_code)]
fn dump(buf: &BytesMut) {
    let mut cloned = buf.clone();
    let mut i: usize = 0;
    let mut text: Vec<u8> = Vec::new();

    println!("---");

    while cloned.has_remaining() {
        let b = cloned.get_u8();

        print!("{:02X} ", b);

        if (b as char).is_alphanumeric() {
            text.push(b);
        } else {
            text.push(b'.');
        }

        i += 1;

        if i % 16 == 0 {
            println!("{}", std::str::from_utf8(&text).unwrap_or_default());
            text.clear();
        }
    }

    println!("---");
}
