use crate::frame::*;
use bytes::{Buf, BufMut, BytesMut};
use std::collections::HashMap;
use tokio_util::codec::{Decoder, Encoder};

const FRAME_METHOD_FRAME: u8 = 0x01;
const FRAME_CONTENT_HEADER: u8 = 0x02;
const FRAME_CONTENT_BODY: u8 = 0x03;
const FRAME_HEARTBEAT: u8 = 0x08;
const FRAME_AMQP_VERSION: u8 = 0x41;

/// Placeholder for AMQP encoder and decoder functions.
pub struct AMQPCodec {}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum Frame {
    Frame(AMQPFrame),
    Frames(Vec<AMQPFrame>),
}

impl Encoder<Frame> for AMQPCodec {
    type Error = std::io::Error;

    fn encode(&mut self, event: Frame, buf: &mut BytesMut) -> Result<(), Self::Error> {
        match event {
            Frame::Frame(frame) => encode_amqp_frame(buf, frame),
            Frame::Frames(frames) => {
                for frame in frames {
                    encode_amqp_frame(buf, frame);
                }
            }
        }

        Ok(())
    }
}

impl Decoder for AMQPCodec {
    type Item = Frame;
    type Error = std::io::Error;

    // TODO here we can decode more frames until the buffer contains data
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 7 || !is_full_frame(src) {
            return Ok(None);
        }

        match src.get_u8() {
            FRAME_METHOD_FRAME => {
                let channel = src.get_u16();
                // TODO amqp frame can be u32 but Buf handles only usize buffes
                let frame_len = src.get_u32() as usize;

                // TODO here there is a panic is the frame is not long enough!
                // FIXME rewrite it to assert instead
                if src.len() < frame_len + 1 {
                    unreachable!();
                }
                let mut frame_buf = src.split_to(frame_len);
                let frame = decode_method_frame(&mut frame_buf, channel);

                let _frame_separator = src.get_u8();

                Ok(Some(Frame::Frame(frame)))
            }
            FRAME_CONTENT_HEADER => {
                let channel = src.get_u16();
                let frame_len = src.get_u32() as usize;

                let mut frame_buf = src.split_to(frame_len);
                let frame = decode_content_header_frame(&mut frame_buf, channel);

                let _frame_separator = src.get_u8();

                Ok(Some(Frame::Frame(frame)))
            }
            FRAME_CONTENT_BODY => {
                let channel = src.get_u16();
                let body_len = src.get_u32();
                let bytes = src.split_to(body_len as usize);

                let _frame_separator = src.get_u8();

                // TODO more effective copy
                let frame = AMQPFrame::ContentBody(ContentBodyFrame {
                    channel,
                    body: bytes.to_vec(),
                });

                Ok(Some(Frame::Frame(frame)))
            }
            FRAME_HEARTBEAT => {
                let channel = src.get_u16();
                let len = src.get_u32();
                let _ = src.split_to(len as usize);

                let _frame_separator = src.get_u8();

                Ok(Some(Frame::Frame(AMQPFrame::Heartbeat(channel))))
            }
            FRAME_AMQP_VERSION => {
                let mut head = [0u8; 7];
                src.copy_to_slice(&mut head);

                // TODO check if version is 0091

                Ok(Some(Frame::Frame(AMQPFrame::Header)))
            }
            f => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Unknown frame {}", f),
            )),
        }
    }
}

impl From<AMQPFrame> for Frame {
    fn from(value: AMQPFrame) -> Self {
        Frame::Frame(value)
    }
}

/// Check if the buffer contains the full frame. We can do that easily since
/// most of the time the frame contains the length information.
fn is_full_frame(src: &BytesMut) -> bool {
    match src[0] {
        FRAME_AMQP_VERSION => src.len() >= 8,
        _ => {
            let mut bs = [0u8; 4];
            bs.copy_from_slice(&src[3..7]);

            let len = u32::from_be_bytes(bs) as usize;

            src.len() >= len + 8
        }
    }
}

// TODO have an Error type here, and it should be result<>
fn decode_method_frame(src: &mut BytesMut, channel: u16) -> AMQPFrame {
    let class_method = src.get_u32();

    let method_frame_args = match class_method {
        CONNECTION_START => decode_connection_start(src),
        CONNECTION_START_OK => decode_connection_start_ok(src),
        CONNECTION_TUNE => decode_connection_tune(src),
        CONNECTION_TUNE_OK => decode_connection_tune_ok(src),
        CONNECTION_OPEN => decode_connection_open(src),
        CONNECTION_OPEN_OK => decode_connection_open_ok(src),
        CONNECTION_CLOSE => decode_connection_close(src),
        CONNECTION_CLOSE_OK => MethodFrameArgs::ConnectionCloseOk,
        CHANNEL_OPEN => decode_channel_open(src),
        CHANNEL_OPEN_OK => decode_channel_open_ok(src),
        CHANNEL_CLOSE => decode_channel_close(src),
        CHANNEL_CLOSE_OK => MethodFrameArgs::ChannelCloseOk,
        EXCHANGE_DECLARE => decode_exchange_declare(src),
        EXCHANGE_DECLARE_OK => MethodFrameArgs::ExchangeDeclareOk,
        EXCHANGE_DELETE => decode_exchange_delete(src),
        EXCHANGE_DELETE_OK => MethodFrameArgs::ExchangeDeleteOk,
        QUEUE_DECLARE => decode_queue_declare(src),
        QUEUE_DECLARE_OK => decode_queue_declare_ok(src),
        QUEUE_BIND => decode_queue_bind(src),
        QUEUE_BIND_OK => MethodFrameArgs::QueueBindOk,
        QUEUE_PURGE => decode_queue_purge(src),
        QUEUE_PURGE_OK => decode_queue_purge_ok(src),
        QUEUE_DELETE => decode_queue_delete(src),
        QUEUE_DELETE_OK => decode_queue_delete_ok(src),
        QUEUE_UNBIND => decode_queue_unbind(src),
        QUEUE_UNBIND_OK => MethodFrameArgs::QueueUnbindOk,
        BASIC_CONSUME => decode_basic_consume(src),
        BASIC_CONSUME_OK => decode_basic_consume_ok(src),
        BASIC_CANCEL => decode_basic_cancel(src),
        BASIC_CANCEL_OK => decode_basic_cancel_ok(src),
        BASIC_GET => decode_basic_get(src),
        BASIC_GET_OK => decode_basic_get_ok(src),
        BASIC_GET_EMPTY => decode_basic_get_empty(src),
        BASIC_PUBLISH => decode_basic_publish(src),
        BASIC_RETURN => decode_basic_return(src),
        BASIC_DELIVER => decode_basic_deliver(src),
        BASIC_ACK => decode_basic_ack(src),
        BASIC_REJECT => decode_basic_reject(src),
        BASIC_NACK => decode_basic_nack(src),
        CONFIRM_SELECT => decode_confirm_select(src),
        CONFIRM_SELECT_OK => MethodFrameArgs::ConfirmSelectOk,
        _ => unimplemented!("{:08X}", class_method),
    };

    AMQPFrame::Method(channel, class_method, method_frame_args)
}

fn decode_connection_start(src: &mut BytesMut) -> MethodFrameArgs {
    let args = ConnectionStartArgs {
        version_major: src.get_u8(),
        version_minor: src.get_u8(),
        properties: decode_field_table(src),
        mechanisms: decode_long_string(src),
        locales: decode_long_string(src),
        ..Default::default()
    };

    //if let Some(ref table) = args.properties {
    //    if let Some(AMQPFieldValue::FieldTable(cap)) = table.get("capabilities".into()) {
    //        args.capabilities = Some(**cap.clone());
    //    }
    //}

    MethodFrameArgs::ConnectionStart(args)
}

fn decode_connection_start_ok(src: &mut BytesMut) -> MethodFrameArgs {
    let args = ConnectionStartOkArgs {
        properties: decode_field_table(src),
        mechanism: decode_short_string(src),
        response: decode_long_string(src),
        locale: decode_short_string(src),
        ..Default::default()
    };

    // TODO init capabilities!

    MethodFrameArgs::ConnectionStartOk(args)
}

fn decode_connection_tune(src: &mut BytesMut) -> MethodFrameArgs {
    let args = ConnectionTuneArgs {
        channel_max: src.get_u16(),
        frame_max: src.get_u32(),
        heartbeat: src.get_u16(),
    };

    MethodFrameArgs::ConnectionTune(args)
}

fn decode_connection_tune_ok(src: &mut BytesMut) -> MethodFrameArgs {
    let args = ConnectionTuneOkArgs {
        channel_max: src.get_u16(),
        frame_max: src.get_u32(),
        heartbeat: src.get_u16(),
    };

    MethodFrameArgs::ConnectionTuneOk(args)
}

fn decode_connection_open(src: &mut BytesMut) -> MethodFrameArgs {
    let virtual_host = decode_short_string(src);
    let _reserved = decode_short_string(src);
    let flags = src.get_u8();

    MethodFrameArgs::ConnectionOpen(ConnectionOpenArgs {
        virtual_host,
        insist: flags & 0x01 != 0,
    })
}

fn decode_connection_open_ok(src: &mut BytesMut) -> MethodFrameArgs {
    let _ = decode_short_string(src);

    MethodFrameArgs::ConnectionOpenOk
}

fn decode_connection_close(src: &mut BytesMut) -> MethodFrameArgs {
    let args = ConnectionCloseArgs {
        code: src.get_u16(),
        text: decode_short_string(src),
        class_id: src.get_u16(),
        method_id: src.get_u16(),
    };

    MethodFrameArgs::ConnectionClose(args)
}

fn decode_channel_open(src: &mut BytesMut) -> MethodFrameArgs {
    let _ = decode_short_string(src);

    MethodFrameArgs::ChannelOpen
}

fn decode_channel_open_ok(src: &mut BytesMut) -> MethodFrameArgs {
    let _ = decode_long_string(src);

    MethodFrameArgs::ChannelOpenOk
}

fn decode_channel_close(src: &mut BytesMut) -> MethodFrameArgs {
    let args = ChannelCloseArgs {
        code: src.get_u16(),
        text: decode_short_string(src),
        class_id: src.get_u16(),
        method_id: src.get_u16(),
    };

    MethodFrameArgs::ChannelClose(args)
}

fn decode_exchange_declare(src: &mut BytesMut) -> MethodFrameArgs {
    let mut args = ExchangeDeclareArgs::default();
    let _ = src.get_u16();
    args.exchange_name = decode_short_string(src);
    args.exchange_type = decode_short_string(src);
    args.flags = ExchangeDeclareFlags::from_bits(src.get_u8()).unwrap_or_default();
    args.args = decode_field_table(src);

    MethodFrameArgs::ExchangeDeclare(args)
}

fn decode_exchange_delete(src: &mut BytesMut) -> MethodFrameArgs {
    let mut args = ExchangeDeleteArgs::default();
    let _ = src.get_u16();
    args.exchange_name = decode_short_string(src);
    args.flags = ExchangeDeleteFlags::from_bits(src.get_u8()).unwrap_or_default();

    MethodFrameArgs::ExchangeDelete(args)
}

fn decode_queue_declare(src: &mut BytesMut) -> MethodFrameArgs {
    let mut args = QueueDeclareArgs::default();
    let _ = src.get_u16();
    args.name = decode_short_string(src);
    args.flags = QueueDeclareFlags::from_bits(src.get_u8()).unwrap_or_default();
    args.args = decode_field_table(src);

    MethodFrameArgs::QueueDeclare(args)
}

fn decode_queue_declare_ok(src: &mut BytesMut) -> MethodFrameArgs {
    let args = QueueDeclareOkArgs {
        name: decode_short_string(src),
        message_count: src.get_u32(),
        consumer_count: src.get_u32(),
    };

    MethodFrameArgs::QueueDeclareOk(args)
}

fn decode_queue_bind(src: &mut BytesMut) -> MethodFrameArgs {
    let mut args = QueueBindArgs::default();
    let _ = src.get_u16();
    args.queue_name = decode_short_string(src);
    args.exchange_name = decode_short_string(src);
    args.routing_key = decode_short_string(src);

    args.no_wait = src.get_u8() != 0;
    args.args = decode_field_table(src);

    MethodFrameArgs::QueueBind(args)
}

fn decode_queue_purge(src: &mut BytesMut) -> MethodFrameArgs {
    let mut args = QueuePurgeArgs::default();
    let _ = src.get_u16();
    args.queue_name = decode_short_string(src);
    args.no_wait = src.get_u8() != 0;

    MethodFrameArgs::QueuePurge(args)
}

fn decode_queue_purge_ok(src: &mut BytesMut) -> MethodFrameArgs {
    let args = QueuePurgeOkArgs {
        message_count: src.get_u32(),
    };

    MethodFrameArgs::QueuePurgeOk(args)
}

fn decode_queue_delete(src: &mut BytesMut) -> MethodFrameArgs {
    let mut args = QueueDeleteArgs::default();
    let _ = src.get_u16();
    args.queue_name = decode_short_string(src);
    args.flags = QueueDeleteFlags::from_bits(src.get_u8()).unwrap_or_default();

    MethodFrameArgs::QueueDelete(args)
}

fn decode_queue_delete_ok(src: &mut BytesMut) -> MethodFrameArgs {
    let args = QueueDeleteOkArgs {
        message_count: src.get_u32(),
    };

    MethodFrameArgs::QueueDeleteOk(args)
}

fn decode_queue_unbind(src: &mut BytesMut) -> MethodFrameArgs {
    let mut args = QueueUnbindArgs::default();
    let _ = src.get_u16();
    args.queue_name = decode_short_string(src);
    args.exchange_name = decode_short_string(src);
    args.routing_key = decode_short_string(src);
    args.args = decode_field_table(src);

    MethodFrameArgs::QueueUnbind(args)
}

fn decode_basic_consume(src: &mut BytesMut) -> MethodFrameArgs {
    let mut args = BasicConsumeArgs::default();
    let _ = src.get_u16();
    args.queue = decode_short_string(src);
    args.consumer_tag = decode_short_string(src);
    args.flags = BasicConsumeFlags::from_bits(src.get_u8()).unwrap_or_default();
    args.args = decode_field_table(src);

    MethodFrameArgs::BasicConsume(args)
}

fn decode_basic_consume_ok(src: &mut BytesMut) -> MethodFrameArgs {
    let args = BasicConsumeOkArgs {
        consumer_tag: decode_short_string(src),
    };

    MethodFrameArgs::BasicConsumeOk(args)
}

fn decode_basic_cancel(src: &mut BytesMut) -> MethodFrameArgs {
    let args = BasicCancelArgs {
        consumer_tag: decode_short_string(src),
        no_wait: src.get_u8() != 0,
    };

    MethodFrameArgs::BasicCancel(args)
}

fn decode_basic_cancel_ok(src: &mut BytesMut) -> MethodFrameArgs {
    let args = BasicCancelOkArgs {
        consumer_tag: decode_short_string(src),
    };

    MethodFrameArgs::BasicCancelOk(args)
}

fn decode_basic_get(src: &mut BytesMut) -> MethodFrameArgs {
    let mut args = BasicGetArgs::default();
    let _ = src.get_u16();

    args.queue = decode_short_string(src);
    args.no_ack = src.get_u8() != 0;

    MethodFrameArgs::BasicGet(args)
}

fn decode_basic_get_ok(src: &mut BytesMut) -> MethodFrameArgs {
    let args = BasicGetOkArgs {
        delivery_tag: src.get_u64(),
        redelivered: src.get_u8() != 0,
        exchange_name: decode_short_string(src),
        routing_key: decode_short_string(src),
        message_count: src.get_u32(),
    };

    MethodFrameArgs::BasicGetOk(args)
}

fn decode_basic_get_empty(src: &mut BytesMut) -> MethodFrameArgs {
    let _ = src.get_u8();

    MethodFrameArgs::BasicGetEmpty
}

fn decode_basic_publish(src: &mut BytesMut) -> MethodFrameArgs {
    let mut args = BasicPublishArgs::default();
    let _ = src.get_u16();
    args.exchange_name = decode_short_string(src);
    args.routing_key = decode_short_string(src);
    args.flags = BasicPublishFlags::from_bits(src.get_u8()).unwrap_or_default();

    MethodFrameArgs::BasicPublish(args)
}

fn decode_basic_return(src: &mut BytesMut) -> MethodFrameArgs {
    let args = BasicReturnArgs {
        reply_code: src.get_u16(),
        reply_text: decode_short_string(src),
        exchange_name: decode_short_string(src),
        routing_key: decode_short_string(src),
    };

    MethodFrameArgs::BasicReturn(args)
}

fn decode_basic_deliver(src: &mut BytesMut) -> MethodFrameArgs {
    let args = BasicDeliverArgs {
        consumer_tag: decode_short_string(src),
        delivery_tag: src.get_u64(),
        redelivered: src.get_u8() != 0,
        exchange_name: decode_short_string(src),
        routing_key: decode_short_string(src),
    };

    MethodFrameArgs::BasicDeliver(args)
}

fn decode_basic_ack(src: &mut BytesMut) -> MethodFrameArgs {
    let args = BasicAckArgs {
        delivery_tag: src.get_u64(),
        multiple: src.get_u8() != 0,
    };

    MethodFrameArgs::BasicAck(args)
}

fn decode_basic_reject(src: &mut BytesMut) -> MethodFrameArgs {
    let args = BasicRejectArgs {
        delivery_tag: src.get_u64(),
        requeue: src.get_u8() != 0,
    };

    MethodFrameArgs::BasicReject(args)
}

fn decode_basic_nack(src: &mut BytesMut) -> MethodFrameArgs {
    let args = BasicNackArgs {
        delivery_tag: src.get_u64(),
        flags: BasicNackFlags::from_bits(src.get_u8()).unwrap_or_default(),
    };

    MethodFrameArgs::BasicNack(args)
}

fn decode_confirm_select(src: &mut BytesMut) -> MethodFrameArgs {
    let args = ConfirmSelectArgs {
        no_wait: src.get_u8() != 0,
    };

    MethodFrameArgs::ConfirmSelect(args)
}

fn decode_content_header_frame(src: &mut BytesMut, channel: u16) -> AMQPFrame {
    let class_id = src.get_u16();
    let weight = src.get_u16();
    let body_size = src.get_u64();
    let property_flags = HeaderPropertyFlags::from_bits(src.get_u16()).unwrap_or_default();

    let content_type = decode_short_string_flag(src, property_flags, HeaderPropertyFlags::CONTENT_TYPE);
    let content_encoding = decode_short_string_flag(src, property_flags, HeaderPropertyFlags::CONTENT_ENCODING);
    let headers = decode_field_table_flag(src, property_flags, HeaderPropertyFlags::HEADERS);
    let delivery_mode = if property_flags.contains(HeaderPropertyFlags::DELIVERY_MODE) {
        Some(src.get_u8())
    } else {
        None
    };
    let priority = if property_flags.contains(HeaderPropertyFlags::PRIORITY) {
        Some(src.get_u8())
    } else {
        None
    };
    let correlation_id = decode_short_string_flag(src, property_flags, HeaderPropertyFlags::CORRELATION_ID);
    let reply_to = decode_short_string_flag(src, property_flags, HeaderPropertyFlags::REPLY_TO);
    let expiration = decode_short_string_flag(src, property_flags, HeaderPropertyFlags::EXPIRATION);
    let message_id = decode_short_string_flag(src, property_flags, HeaderPropertyFlags::MESSAGE_ID);
    let timestamp = if property_flags.contains(HeaderPropertyFlags::TIMESTAMP) {
        Some(src.get_u64())
    } else {
        None
    };
    let message_type = decode_short_string_flag(src, property_flags, HeaderPropertyFlags::MESSAGE_TYPE);
    let user_id = decode_short_string_flag(src, property_flags, HeaderPropertyFlags::USER_ID);
    let app_id = decode_short_string_flag(src, property_flags, HeaderPropertyFlags::APP_ID);
    let cluster_id = decode_short_string_flag(src, property_flags, HeaderPropertyFlags::CLUSTER_ID);

    AMQPFrame::ContentHeader(ContentHeaderFrame {
        channel,
        class_id,
        weight,
        body_size,
        prop_flags: property_flags,
        cluster_id,
        app_id,
        user_id,
        message_type,
        timestamp,
        message_id,
        expiration,
        reply_to,
        correlation_id,
        priority,
        delivery_mode,
        headers,
        content_encoding,
        content_type,
    })
}

fn decode_short_string_flag(
    src: &mut BytesMut,
    flags: HeaderPropertyFlags,
    flag: HeaderPropertyFlags,
) -> Option<String> {
    if flags.contains(flag) {
        Some(decode_short_string(src))
    } else {
        None
    }
}

fn decode_field_table_flag(
    src: &mut BytesMut,
    flags: HeaderPropertyFlags,
    flag: HeaderPropertyFlags,
) -> Option<FieldTable> {
    if flags.contains(flag) {
        decode_field_table(src)
    } else {
        None
    }
}

fn decode_value(buf: &mut BytesMut) -> AMQPFieldValue {
    match buf.get_u8() {
        b't' => {
            let bool_value = buf.get_u8() != 0;

            AMQPFieldValue::Bool(bool_value)
        }
        b'S' => {
            let string_value = decode_long_string(buf);

            AMQPFieldValue::LongString(string_value)
        }
        b'F' => match decode_field_table(buf) {
            None => AMQPFieldValue::EmptyFieldTable,
            Some(table) => AMQPFieldValue::FieldTable(Box::new(table)),
        },
        t => panic!("Unknown type {}", t),
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
        return None;
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

fn encode_amqp_frame(buf: &mut BytesMut, frame: AMQPFrame) {
    match frame {
        AMQPFrame::Header => buf.put(&b"AMQP\x00\x00\x09\x01"[..]),

        AMQPFrame::Method(ch, cm, args) => encode_method_frame(buf, ch, cm, &args),

        AMQPFrame::ContentHeader(header_frame) => encode_content_header_frame(buf, &header_frame),

        AMQPFrame::ContentBody(body_frame) => encode_content_body_frame(buf, &body_frame),

        AMQPFrame::Heartbeat(channel) => encode_heartbeat_frame(buf, channel),
    }
}

fn encode_method_frame(buf: &mut BytesMut, channel: Channel, cm: ClassMethod, args: &MethodFrameArgs) {
    buf.put_u8(1u8);
    buf.put_u16(channel);

    let mut fr = BytesMut::with_capacity(4096);
    fr.put_u32(cm);

    match args {
        MethodFrameArgs::ConnectionStart(args) => encode_connection_start(&mut fr, args),
        MethodFrameArgs::ConnectionStartOk(args) => encode_connection_start_ok(&mut fr, args),
        MethodFrameArgs::ConnectionTune(args) => encode_connection_tune(&mut fr, args),
        MethodFrameArgs::ConnectionTuneOk(args) => encode_connection_tune_ok(&mut fr, args),
        MethodFrameArgs::ConnectionOpen(args) => encode_connection_open(&mut fr, args),
        MethodFrameArgs::ConnectionOpenOk => encode_connection_open_ok(&mut fr),
        MethodFrameArgs::ConnectionClose(args) => encode_connection_close(&mut fr, args),
        MethodFrameArgs::ConnectionCloseOk => (),
        MethodFrameArgs::ChannelOpen => encode_channel_open(&mut fr),
        MethodFrameArgs::ChannelOpenOk => encode_channel_open_ok(&mut fr),
        MethodFrameArgs::ChannelClose(args) => encode_channel_close(&mut fr, args),
        MethodFrameArgs::ChannelCloseOk => (),
        MethodFrameArgs::ExchangeDeclare(args) => encode_exchange_declare(&mut fr, args),
        MethodFrameArgs::ExchangeDeclareOk => (),
        MethodFrameArgs::ExchangeDelete(args) => encode_exchange_delete(&mut fr, args),
        MethodFrameArgs::ExchangeDeleteOk => (),
        MethodFrameArgs::QueueDeclare(args) => encode_queue_declare(&mut fr, args),
        MethodFrameArgs::QueueDeclareOk(args) => encode_queue_declare_ok(&mut fr, args),
        MethodFrameArgs::QueueBind(args) => encode_queue_bind(&mut fr, args),
        MethodFrameArgs::QueueBindOk => (),
        MethodFrameArgs::QueuePurge(args) => encode_queue_purge(&mut fr, args),
        MethodFrameArgs::QueuePurgeOk(args) => encode_queue_purge_ok(&mut fr, args),
        MethodFrameArgs::QueueDelete(args) => encode_queue_delete(&mut fr, args),
        MethodFrameArgs::QueueDeleteOk(args) => encode_queue_delete_ok(&mut fr, args),
        MethodFrameArgs::QueueUnbind(args) => encode_queue_unbind(&mut fr, args),
        MethodFrameArgs::QueueUnbindOk => (),
        MethodFrameArgs::BasicConsume(args) => encode_basic_consume(&mut fr, args),
        MethodFrameArgs::BasicConsumeOk(args) => encode_basic_consume_ok(&mut fr, args),
        MethodFrameArgs::BasicCancel(args) => encode_basic_cancel(&mut fr, args),
        MethodFrameArgs::BasicCancelOk(args) => encode_basic_cancel_ok(&mut fr, args),
        MethodFrameArgs::BasicGet(args) => encode_basic_get(&mut fr, args),
        MethodFrameArgs::BasicGetOk(args) => encode_basic_get_ok(&mut fr, args),
        MethodFrameArgs::BasicGetEmpty => encode_basic_get_empty(&mut fr),
        MethodFrameArgs::BasicPublish(args) => encode_basic_publish(&mut fr, args),
        MethodFrameArgs::BasicReturn(args) => encode_basic_return(&mut fr, args),
        MethodFrameArgs::BasicDeliver(args) => encode_basic_deliver(&mut fr, args),
        MethodFrameArgs::BasicAck(args) => encode_basic_ack(&mut fr, args),
        MethodFrameArgs::BasicReject(args) => encode_basic_reject(&mut fr, args),
        MethodFrameArgs::BasicNack(args) => encode_basic_nack(&mut fr, args),
        MethodFrameArgs::ConfirmSelect(args) => encode_confirm_select(&mut fr, args),
        MethodFrameArgs::ConfirmSelectOk => (),
    }

    buf.put_u32(fr.len() as u32);
    buf.put(fr);
    buf.put_u8(0xCE);
}

fn encode_connection_start(buf: &mut BytesMut, args: &ConnectionStartArgs) {
    buf.put_u8(args.version_major);
    buf.put_u8(args.version_minor);
    encode_field_table(buf, args.properties.as_ref());
    encode_long_string(buf, &args.mechanisms);
    encode_long_string(buf, &args.locales);
}

fn encode_connection_start_ok(buf: &mut BytesMut, args: &ConnectionStartOkArgs) {
    encode_field_table(buf, args.properties.as_ref());
    encode_short_string(buf, &args.mechanism);
    encode_long_string(buf, &args.response);
    encode_short_string(buf, &args.locale);
}

fn encode_connection_tune(buf: &mut BytesMut, args: &ConnectionTuneArgs) {
    buf.put_u16(args.channel_max);
    buf.put_u32(args.frame_max);
    buf.put_u16(args.heartbeat);
}

fn encode_connection_tune_ok(buf: &mut BytesMut, args: &ConnectionTuneOkArgs) {
    buf.put_u16(args.channel_max);
    buf.put_u32(args.frame_max);
    buf.put_u16(args.heartbeat);
}

fn encode_connection_open(buf: &mut BytesMut, args: &ConnectionOpenArgs) {
    encode_short_string(buf, &args.virtual_host);
    encode_short_string(buf, "");
    let mut flags = 0x00;

    if args.insist {
        flags |= 0x01;
    }

    buf.put_u8(flags);
}

fn encode_connection_open_ok(buf: &mut BytesMut) {
    // encode empty short string
    buf.put_u8(0);
}

fn encode_connection_close(buf: &mut BytesMut, args: &ConnectionCloseArgs) {
    buf.put_u16(args.code);
    encode_short_string(buf, &args.text);
    buf.put_u16(args.class_id);
    buf.put_u16(args.method_id);
}

fn encode_channel_open(buf: &mut BytesMut) {
    // encode empty short string
    buf.put_u8(0);
}

fn encode_channel_open_ok(buf: &mut BytesMut) {
    // encode empty long string
    buf.put_u32(0);
}

fn encode_channel_close(buf: &mut BytesMut, args: &ChannelCloseArgs) {
    buf.put_u16(args.code);
    encode_short_string(buf, &args.text);
    buf.put_u16(args.class_id);
    buf.put_u16(args.method_id);
}

fn encode_exchange_declare(buf: &mut BytesMut, args: &ExchangeDeclareArgs) {
    buf.put_u16(0);
    encode_short_string(buf, &args.exchange_name);
    encode_short_string(buf, &args.exchange_type);
    buf.put_u8(args.flags.bits());
    encode_field_table(buf, args.args.as_ref());
}

fn encode_exchange_delete(buf: &mut BytesMut, args: &ExchangeDeleteArgs) {
    buf.put_u16(0);
    encode_short_string(buf, &args.exchange_name);
    buf.put_u8(args.flags.bits());
}

fn encode_queue_declare(buf: &mut BytesMut, args: &QueueDeclareArgs) {
    buf.put_u16(0);
    encode_short_string(buf, &args.name);
    buf.put_u8(args.flags.bits());
    encode_field_table(buf, args.args.as_ref());
}

fn encode_queue_declare_ok(buf: &mut BytesMut, args: &QueueDeclareOkArgs) {
    encode_short_string(buf, &args.name);
    buf.put_u32(args.message_count);
    buf.put_u32(args.consumer_count);
}

fn encode_queue_bind(buf: &mut BytesMut, args: &QueueBindArgs) {
    buf.put_u16(0);
    encode_short_string(buf, &args.queue_name);
    encode_short_string(buf, &args.exchange_name);
    encode_short_string(buf, &args.routing_key);
    buf.put_u8(if args.no_wait { 1 } else { 0 });
    encode_field_table(buf, args.args.as_ref());
}

fn encode_queue_purge(buf: &mut BytesMut, args: &QueuePurgeArgs) {
    buf.put_u16(0);
    encode_short_string(buf, &args.queue_name);
    buf.put_u8(if args.no_wait { 1 } else { 0 });
}

fn encode_queue_purge_ok(buf: &mut BytesMut, args: &QueuePurgeOkArgs) {
    buf.put_u32(args.message_count);
}

fn encode_queue_delete(buf: &mut BytesMut, args: &QueueDeleteArgs) {
    buf.put_u16(0);
    encode_short_string(buf, &args.queue_name);
    buf.put_u8(args.flags.bits());
}

fn encode_queue_delete_ok(buf: &mut BytesMut, args: &QueueDeleteOkArgs) {
    buf.put_u32(args.message_count);
}

fn encode_queue_unbind(buf: &mut BytesMut, args: &QueueUnbindArgs) {
    buf.put_u16(0);
    encode_short_string(buf, &args.queue_name);
    encode_short_string(buf, &args.exchange_name);
    encode_short_string(buf, &args.routing_key);
    encode_field_table(buf, args.args.as_ref());
}

fn encode_basic_consume(buf: &mut BytesMut, args: &BasicConsumeArgs) {
    buf.put_u16(0);
    encode_short_string(buf, &args.queue);
    encode_short_string(buf, &args.consumer_tag);
    buf.put_u8(args.flags.bits());
    encode_field_table(buf, args.args.as_ref());
}

fn encode_basic_consume_ok(buf: &mut BytesMut, args: &BasicConsumeOkArgs) {
    encode_short_string(buf, &args.consumer_tag);
}

fn encode_basic_cancel(buf: &mut BytesMut, args: &BasicCancelArgs) {
    encode_short_string(buf, &args.consumer_tag);
    buf.put_u8(if args.no_wait { 1 } else { 0 });
}

fn encode_basic_cancel_ok(buf: &mut BytesMut, args: &BasicCancelOkArgs) {
    encode_short_string(buf, &args.consumer_tag);
}

fn encode_basic_get(buf: &mut BytesMut, args: &BasicGetArgs) {
    buf.put_u16(0);
    encode_short_string(buf, &args.queue);
    buf.put_u8(if args.no_ack { 1 } else { 0 });
}

fn encode_basic_get_ok(buf: &mut BytesMut, args: &BasicGetOkArgs) {
    buf.put_u64(args.delivery_tag);
    buf.put_u8(if args.redelivered { 1 } else { 0 });
    encode_short_string(buf, &args.exchange_name);
    encode_short_string(buf, &args.routing_key);
    buf.put_u32(args.message_count);
}

fn encode_basic_get_empty(buf: &mut BytesMut) {
    buf.put_u8(0);
}

fn encode_basic_publish(buf: &mut BytesMut, args: &BasicPublishArgs) {
    buf.put_u16(0);
    encode_short_string(buf, &args.exchange_name);
    encode_short_string(buf, &args.routing_key);
    buf.put_u8(args.flags.bits());
}

fn encode_basic_return(buf: &mut BytesMut, args: &BasicReturnArgs) {
    buf.put_u16(args.reply_code);
    encode_short_string(buf, &args.reply_text);
    encode_short_string(buf, &args.exchange_name);
    encode_short_string(buf, &args.routing_key);
}

fn encode_basic_deliver(buf: &mut BytesMut, args: &BasicDeliverArgs) {
    encode_short_string(buf, &args.consumer_tag);
    buf.put_u64(args.delivery_tag);
    buf.put_u8(if args.redelivered { 1 } else { 0 });
    encode_short_string(buf, &args.exchange_name);
    encode_short_string(buf, &args.routing_key);
}

fn encode_basic_ack(buf: &mut BytesMut, args: &BasicAckArgs) {
    buf.put_u64(args.delivery_tag);
    buf.put_u8(if args.multiple { 1 } else { 0 });
}

fn encode_basic_reject(buf: &mut BytesMut, args: &BasicRejectArgs) {
    buf.put_u64(args.delivery_tag);
    buf.put_u8(if args.requeue { 1 } else { 0 });
}

fn encode_basic_nack(buf: &mut BytesMut, args: &BasicNackArgs) {
    buf.put_u64(args.delivery_tag);
    buf.put_u8(args.flags.bits());
}

fn encode_confirm_select(buf: &mut BytesMut, args: &ConfirmSelectArgs) {
    buf.put_u8(if args.no_wait { 1 } else { 0 });
}

fn encode_content_header_frame(buf: &mut BytesMut, hf: &ContentHeaderFrame) {
    buf.put_u8(2u8);
    buf.put_u16(hf.channel);

    let mut fr_buf = BytesMut::with_capacity(4096);
    fr_buf.put_u16(hf.class_id);
    fr_buf.put_u16(hf.weight);
    fr_buf.put_u64(hf.body_size);
    fr_buf.put_u16(hf.prop_flags.bits());

    if let Some(s) = hf.content_type.as_ref() {
        encode_short_string(&mut fr_buf, s);
    }
    if let Some(s) = hf.content_encoding.as_ref() {
        encode_short_string(&mut fr_buf, s);
    }
    if hf.prop_flags.contains(HeaderPropertyFlags::HEADERS) {
        encode_field_table(&mut fr_buf, hf.headers.as_ref());
    }
    if let Some(v) = hf.delivery_mode {
        fr_buf.put_u8(v);
    }
    if let Some(v) = hf.priority {
        fr_buf.put_u8(v);
    }
    if let Some(s) = hf.correlation_id.as_ref() {
        encode_short_string(&mut fr_buf, s);
    }
    if let Some(s) = hf.reply_to.as_ref() {
        encode_short_string(&mut fr_buf, s);
    }
    if let Some(s) = hf.expiration.as_ref() {
        encode_short_string(&mut fr_buf, s);
    }
    if let Some(s) = hf.message_id.as_ref() {
        encode_short_string(&mut fr_buf, s);
    }
    if let Some(v) = hf.timestamp {
        fr_buf.put_u64(v);
    }
    if let Some(s) = hf.message_type.as_ref() {
        encode_short_string(&mut fr_buf, s);
    }
    if let Some(s) = hf.user_id.as_ref() {
        encode_short_string(&mut fr_buf, s);
    }
    if let Some(s) = hf.app_id.as_ref() {
        encode_short_string(&mut fr_buf, s);
    }
    if let Some(s) = hf.cluster_id.as_ref() {
        encode_short_string(&mut fr_buf, s);
    }

    buf.put_u32(fr_buf.len() as u32);
    buf.put(fr_buf);
    buf.put_u8(0xCE);
}

fn encode_content_body_frame(buf: &mut BytesMut, bf: &ContentBodyFrame) {
    // TODO buf.reserve()
    buf.put_u8(3u8);
    buf.put_u16(bf.channel);

    let mut fr_buf = BytesMut::with_capacity(bf.body.len());
    fr_buf.put(bf.body.as_slice());

    buf.put_u32(fr_buf.len() as u32);
    buf.put(fr_buf);
    buf.put_u8(0xCE);
}

fn encode_heartbeat_frame(buf: &mut BytesMut, channel: Channel) {
    buf.put_u16(channel);
    buf.put_u32(0);
    buf.put_u8(0xCE);
}

fn encode_short_string(buf: &mut BytesMut, s: &str) {
    // TODO assert! that size is below 256
    buf.put_u8(s.len() as u8);
    buf.put(s.as_bytes());
}

fn encode_long_string(buf: &mut BytesMut, s: &str) {
    buf.put_u32(s.len() as u32);
    buf.put(s.as_bytes());
}

fn encode_empty_field_table(buf: &mut BytesMut) {
    buf.put_u32(0);
}

fn encode_field_table(buf: &mut BytesMut, ft: Option<&HashMap<String, AMQPFieldValue>>) {
    match ft {
        None => buf.put_u32(0),
        Some(t) => encode_field_table2(buf, t),
    }
}

fn encode_field_table2(buf: &mut BytesMut, ft: &HashMap<String, AMQPFieldValue>) {
    let mut ft_buf = BytesMut::with_capacity(4096);

    for (name, value) in ft {
        encode_short_string(&mut ft_buf, name);

        match value {
            AMQPFieldValue::Bool(v) => {
                ft_buf.put_u8(b't');
                ft_buf.put_u8(if *v { 1 } else { 0 });
            }
            AMQPFieldValue::LongString(v) => {
                ft_buf.put_u8(b'S');
                ft_buf.put_u32(v.len() as u32);
                ft_buf.put(v.as_bytes());
            }
            AMQPFieldValue::EmptyFieldTable => encode_empty_field_table(&mut ft_buf),
            AMQPFieldValue::FieldTable(v) => {
                ft_buf.put_u8(b'F');

                // TODO we are copying here
                encode_field_table2(&mut ft_buf, v);
            }
        }
    }

    buf.put_u32(ft_buf.len() as u32);
    buf.put(ft_buf);
}
