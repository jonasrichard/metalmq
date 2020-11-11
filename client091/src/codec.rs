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
fn decode_method_frame(src: &mut BytesMut, channel: u16) -> AMQPFrame {
    let class = src.get_u16();
    let method = src.get_u16();
    let version_major = src.get_u8();
    let version_minor = src.get_u8();

    let arg_len = src.get_u32() as usize;
    let mut arg_buf = src.split_to(arg_len);

    let args = decode_value_list(&mut arg_buf);

    // TODO find out something more elegant

    let server_properties = match args.get(0) {
        Some(AMQPValue::FieldTable(hm)) =>
            Some(hm),
        _ =>
            None
    };

    let mechanism = match args.get(1) {
        Some(AMQPValue::SimpleString(v)) =>
            Some(v),
        _ =>
            None
    };

    let locales = match args.get(2) {
        Some(AMQPValue::SimpleString(v)) =>
            Some(v),
        _ =>
            None
    };

    AMQPFrame::Method(Box::new(MethodFrame {
        channel: channel,
        method: method,
        class: class,
        version_major: version_major,
        version_minor: version_minor,
        server_properties: server_properties.unwrap().to_vec(),
        mechanisms: mechanism.unwrap().to_owned(),
        locales: locales.unwrap().to_owned()
    }))
}

fn decode_value_list(src: &mut BytesMut) -> Vec<AMQPValue> {
    let mut values = Vec::new();

    while src.has_remaining() {
        let value = decode_value(src);
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

    String::from_utf8(sb.to_vec()).unwrap()
}

fn decode_long_string(buf: &mut BytesMut) -> String {
    let len = buf.get_u32() as usize;
    let sb = buf.split_to(len);

    String::from_utf8(sb.to_vec()).unwrap()
}

fn decode_field_table(buf: &mut BytesMut) -> Vec<(String, AMQPValue)> {
    let len = buf.get_u32() as usize;
    let mut sub_buf = buf.split_to(len);
    let mut table = Vec::new();

    while sub_buf.has_remaining() {
        let field_name = decode_short_string(&mut sub_buf);
        info!("Field name {}", field_name);

        let field_value = decode_value(&mut sub_buf);

        table.push((field_name, field_value));
    }

    table
}
