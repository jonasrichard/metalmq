use bytes::{Buf, BufMut, BytesMut};
use log::{info};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug)]
pub enum AMQPFrame {
    AMQPHeader,
    Method(Box<MethodFrame>),
}

pub struct AMQPCodec {
}

#[derive(Clone, Debug)]
pub enum AMQPValue {
    Bool(bool),
    SimpleString(String),
    LongString(String),
    FieldTable(Vec<(String, AMQPValue)>)
}

#[derive(Debug)]
pub struct MethodFrame {
    channel: u16,
    class: u16,
    method: u16,
    version_major: u8,
    version_minor: u8,
    server_properties: Vec<(String, AMQPValue)>,
    mechanisms: String,
    locales: String
}

impl Encoder<AMQPFrame> for AMQPCodec {
    type Error = std::io::Error;

    fn encode(&mut self, event: AMQPFrame, buf: &mut BytesMut) -> Result<(), Self::Error> {
        match event {
            AMQPFrame::AMQPHeader =>
                buf.put(&b"AMQP\x00\x00\x09\x01"[..]),

            AMQPFrame::Method(method_frame) => {
            }
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

                    Ok(Some(frame))
                },
                f =>
                    panic!("Unknown frame {}", f)
            }
        }
    }
}

// TODO have an Error type here, and it should be result<>
fn decode_method_frame(mut src: &mut BytesMut, channel: u16) -> AMQPFrame {
    let class = src.get_u16();
    let method = src.get_u16();
    let version_major = src.get_u8();
    let version_minor = src.get_u8();

    let server_properties = decode_field_table(&mut src);
    let mechanism = decode_long_string(&mut src);
    let locales = decode_long_string(&mut src);

    AMQPFrame::Method(Box::new(MethodFrame {
        channel: channel,
        method: method,
        class: class,
        version_major: version_major,
        version_minor: version_minor,
        server_properties: server_properties,
        mechanisms: mechanism,
        locales: locales
    }))
}

fn decode_value_list(src: &mut BytesMut) -> Vec<AMQPValue> {
    let mut values = Vec::new();

    while src.has_remaining() {
        let value = decode_value(src);
        info!("Decoded value {:?}", value);
        values.push(value);
    }

    values
}

fn decode_value(mut buf: &mut BytesMut) -> AMQPValue {
    match buf.get_u8() {
        b't' => {
            let bool_value = buf.get_u8() != 0;

            AMQPValue::Bool(bool_value)
        },
        b'S' => {
            let string_value = decode_long_string(&mut buf);

            AMQPValue::LongString(string_value)
        },
        b'F' => {
            let table = decode_field_table(&mut buf);

            AMQPValue::FieldTable(table)
        },
        t =>
            panic!("Unknown type {}", t)
    }
}

fn decode_short_string(buf: &mut BytesMut) -> String {
    let len = buf.get_u8() as usize;
    let sb = buf.split_to(len);

    dump(&sb);

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
fn decode_field_table(buf: &mut BytesMut) -> Vec<(String, AMQPValue)> {
    let ft_len = buf.get_u32() as usize;
    let mut ft_buf = buf.split_to(ft_len);
    let mut table = Vec::new();

    dump(&ft_buf);

    while ft_buf.has_remaining() {
        let field_name = decode_short_string(&mut ft_buf);
        let field_value = decode_value(&mut ft_buf);

        info!("Field name -> value {} -> {:?}", field_name, field_value);

        table.push((field_name, field_value));
    }

    table
}

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
