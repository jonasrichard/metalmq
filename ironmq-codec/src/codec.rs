use crate::frame::*;
use bytes::{Buf, BufMut, BytesMut};
use std::collections::HashMap;
use tokio_util::codec::{Decoder, Encoder};

const FRAME_METHOD_FRAME: u8 = 0x01;
const FRAME_CONTENT_HEADER: u8 = 0x02;
const FRAME_CONTENT_BODY: u8 = 0x03;
const FRAME_AMQP_VERSION: u8 = 0x41;

pub struct AMQPCodec {
}

impl Encoder<AMQPFrame> for AMQPCodec {
    type Error = std::io::Error;

    fn encode(&mut self, event: AMQPFrame, mut buf: &mut BytesMut) -> Result<(), Self::Error> {
        match event {
            AMQPFrame::AMQPHeader =>
                buf.put(&b"AMQP\x00\x00\x09\x01"[..]),

            AMQPFrame::Method(method_frame) => {
                encode_method_frame(&mut buf, *method_frame)
            },

            AMQPFrame::ContentHeader(header_frame) =>
                encode_content_header_frame(&mut buf, *header_frame),

            AMQPFrame::ContentBody(body_frame) =>
                encode_content_body_frame(&mut buf, body_frame)
        }

        Ok(())
    }
}

impl Decoder for AMQPCodec {
    type Item = AMQPFrame;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        //println!("Decode remaining {}", src.remaining());
        //dump(&src);

        if src.len() < 8 {
            Ok(None)
        } else {
            match src.get_u8() {
                FRAME_METHOD_FRAME => {
                    let channel = src.get_u16();
                    // TODO amqp frame can be u32 but Buf handles only usize buffes
                    let frame_len = src.get_u32() as usize;

                    let mut frame_buf = src.split_to(frame_len);
                    let frame = decode_method_frame(&mut frame_buf, channel);

                    let _frame_separator = src.get_u8();

                    Ok(Some(frame))
                },
                FRAME_CONTENT_HEADER => {
                    let channel = src.get_u16();
                    let frame_len = src.get_u32() as usize;

                    let mut frame_buf = src.split_to(frame_len);
                    let frame = decode_content_header_frame(&mut frame_buf, channel);

                    let _frame_separator = src.get_u8();

                    Ok(Some(frame))
                },
                FRAME_CONTENT_BODY => {
                    let channel = src.get_u16();
                    let body_len = src.get_u32();
                    let bytes = src.split_to(body_len as usize);

                    let _frame_separator = src.get_u8();

                    // TODO more effective copy
                    let frame = AMQPFrame::ContentBody(Box::new(ContentBodyFrame {
                        channel: channel,
                        body: bytes.to_vec()
                    }));

                    Ok(Some(frame))
                },
                FRAME_AMQP_VERSION => {
                    let mut head = [0u8; 7];
                    src.copy_to_slice(&mut head);

                    // TODO check if version is 0091

                    Ok(Some(AMQPFrame::AMQPHeader))
                },
                f => {
                    Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Unknown frame {}", f)))
                }
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
                match decode_field_table(&mut src) {
                    None =>
                        args.push(AMQPValue::EmptyFieldTable),
                    Some(table) =>
                        args.push(AMQPValue::FieldTable(Box::new(table)))
                }
        }
    }

    AMQPFrame::Method(Box::new(MethodFrame {
        channel: channel,
        class_method: class_method,
        args: args
    }))
}

fn decode_content_header_frame(src: &mut BytesMut, channel: u16) -> AMQPFrame {
    dump(&src);

    let class_id = src.get_u16();
    let weight = src.get_u16();
    let body_size = src.get_u64();
    let property_flags = src.get_u16();
    // TODO property list, it seems that we need to know from the class_id what is the type list

    AMQPFrame::ContentHeader(Box::new(ContentHeaderFrame {
        channel: channel,
        class_id: class_id,
        weight: weight,
        body_size: body_size,
        prop_flags: property_flags,
        args: vec![]
    }))
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
            match decode_field_table(&mut buf) {
                None =>
                    AMQPFieldValue::EmptyFieldTable,
                Some(table) =>
                    AMQPFieldValue::FieldTable(Box::new(table))
            }
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
fn decode_field_table(buf: &mut BytesMut) -> Option<HashMap<String, AMQPFieldValue>> {
    let ft_len = buf.get_u32() as usize;

    if ft_len == 0 {
        return None
    }

    let mut ft_buf = buf.split_to(ft_len);
    let mut table = HashMap::new();

    while ft_buf.has_remaining() {
        let field_name = decode_short_string(&mut ft_buf);
        let field_value = decode_value(&mut ft_buf);

        table.insert(field_name, field_value);
    }

    Some(table)
}

fn encode_method_frame(buf: &mut BytesMut, mf: MethodFrame) {
    buf.put_u8(1u8);
    buf.put_u16(mf.channel);

    let mut fr_buf = BytesMut::with_capacity(4096);
    fr_buf.put_u32(mf.class_method);

    for arg in mf.args {
        encode_value(&mut fr_buf, arg);
    }

    buf.put_u32(fr_buf.len() as u32);
    buf.put(fr_buf);
    buf.put_u8(0xCE);
}

fn encode_content_header_frame(buf: &mut BytesMut, hf: ContentHeaderFrame) {
    buf.put_u8(2u8);
    buf.put_u16(hf.channel);

    let mut fr_buf = BytesMut::with_capacity(4096);
    fr_buf.put_u16(hf.class_id);
    fr_buf.put_u16(hf.weight);
    fr_buf.put_u64(hf.body_size);
    fr_buf.put_u16(hf.prop_flags);

    // TODO encode property list here

    buf.put_u32(fr_buf.len() as u32);
    buf.put(fr_buf);
    buf.put_u8(0xCE);
}

fn encode_content_body_frame(buf: &mut BytesMut, bf: Box<ContentBodyFrame>) {
    buf.put_u8(3u8);
    buf.put_u16(bf.channel);

    let mut fr_buf = BytesMut::with_capacity(bf.body.len());
    fr_buf.put(bf.body.as_slice());

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
        AMQPValue::EmptyFieldTable =>
            encode_empty_field_table(&mut buf),
        AMQPValue::FieldTable(v) =>
            encode_field_table(&mut buf, *v),
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

fn encode_empty_field_table(buf: &mut BytesMut) {
    buf.put_u32(0);
}

fn encode_field_table(buf: &mut BytesMut, ft: HashMap<String, AMQPFieldValue>) {
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
            AMQPFieldValue::EmptyFieldTable =>
                encode_empty_field_table(&mut ft_buf),
            AMQPFieldValue::FieldTable(v) => {
                ft_buf.put_u8(b'F');

                // TODO we are copying here
                encode_field_table(&mut ft_buf, *v);
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
