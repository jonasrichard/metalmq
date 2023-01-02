use std::collections::HashMap;

pub const CONNECTION_START: u32 = 0x000A000A;
pub const CONNECTION_START_OK: u32 = 0x000A000B;
pub const CONNECTION_SECRET: u32 = 0x000A0014;
pub const CONNECTION_SECRET_OK: u32 = 0x000A0015;
pub const CONNECTION_TUNE: u32 = 0x000A001E;
pub const CONNECTION_TUNE_OK: u32 = 0x000A001F;
pub const CONNECTION_OPEN: u32 = 0x000A0028;
pub const CONNECTION_OPEN_OK: u32 = 0x000A0029;
pub const CONNECTION_CLOSE: u32 = 0x000A0032;
pub const CONNECTION_CLOSE_OK: u32 = 0x000A0033;

pub const CHANNEL_OPEN: u32 = 0x0014000A;
pub const CHANNEL_OPEN_OK: u32 = 0x0014000B;
pub const CHANNEL_FLOW: u32 = 0x00140014;
pub const CHANNEL_FLOW_OK: u32 = 0x00140015;
pub const CHANNEL_CLOSE: u32 = 0x00140028;
pub const CHANNEL_CLOSE_OK: u32 = 0x00140029;

pub const EXCHANGE_DECLARE: u32 = 0x0028000A;
pub const EXCHANGE_DECLARE_OK: u32 = 0x0028000B;
pub const EXCHANGE_DELETE: u32 = 0x00280014;
pub const EXCHANGE_DELETE_OK: u32 = 0x00280015;

pub const QUEUE_DECLARE: u32 = 0x0032000A;
pub const QUEUE_DECLARE_OK: u32 = 0x0032000B;
pub const QUEUE_BIND: u32 = 0x00320014;
pub const QUEUE_BIND_OK: u32 = 0x00320015;
pub const QUEUE_PURGE: u32 = 0x0032001E;
pub const QUEUE_PURGE_OK: u32 = 0x0032001F;
pub const QUEUE_DELETE: u32 = 0x00320028;
pub const QUEUE_DELETE_OK: u32 = 0x00320029;
pub const QUEUE_UNBIND: u32 = 0x00320032;
pub const QUEUE_UNBIND_OK: u32 = 0x00320033;

pub const BASIC_QOS: u32 = 0x003C000A;
pub const BASIC_QOS_OK: u32 = 0x003C000B;
pub const BASIC_CONSUME: u32 = 0x003C0014;
pub const BASIC_CONSUME_OK: u32 = 0x003C0015;
pub const BASIC_CANCEL: u32 = 0x003C001E;
pub const BASIC_CANCEL_OK: u32 = 0x003C001F;
pub const BASIC_PUBLISH: u32 = 0x003C0028;
pub const BASIC_RETURN: u32 = 0x003C0032;
pub const BASIC_DELIVER: u32 = 0x003C003C;
pub const BASIC_GET: u32 = 0x003C0046;
pub const BASIC_GET_OK: u32 = 0x003C0047;
pub const BASIC_GET_EMPTY: u32 = 0x003C0048;
pub const BASIC_ACK: u32 = 0x003C0050;
pub const BASIC_REJECT: u32 = 0x003C005A;
pub const BASIC_RECOVER_ASYNC: u32 = 0x003C0064;
pub const BASIC_RECOVER: u32 = 0x003C006E;
pub const BASIC_RECOVER_OK: u32 = 0x003C006F;

pub const CONFIRM_SELECT: u32 = 0x0055000A;
pub const CONFIRM_SELECT_OK: u32 = 0x0055000B;

pub type Channel = u16;
pub type ClassMethod = u32;
pub type ClassId = u16;
pub type Weight = u16;

/// Represents an AMQP frame.
pub enum AMQPFrame {
    /// Header is to be sent to the server at first, announcing the AMQP version we support
    Header,
    /// Represents the AMQP RPC frames. Connection based calls have a channel number 0, otherwise
    /// channel is the current channel on which the frames are sent. The RPC arguments are
    /// represented in `MethodFrameArgs`.
    Method(Channel, ClassMethod, MethodFrameArgs),
    ContentHeader(ContentHeaderFrame),
    ContentBody(ContentBodyFrame),
    Heartbeat(Channel),
}

impl std::fmt::Debug for AMQPFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AMQPFrame::Header => write!(f, "Header"),
            AMQPFrame::Method(ch, cm, args) => write!(f, "Method(channel={}, {:08X}, {:?})", ch, cm, args),
            AMQPFrame::ContentHeader(ch) => write!(f, "ContentHeader({:?})", ch),
            AMQPFrame::ContentBody(cb) => write!(f, "ContentBody({:?})", cb),
            AMQPFrame::Heartbeat(_) => write!(f, "Heartbeat"),
        }
    }
}

/// Represents all types of method frame arguments.
#[derive(Debug)]
pub enum MethodFrameArgs {
    ConnectionStart(ConnectionStartArgs),
    ConnectionStartOk(ConnectionStartOkArgs),
    ConnectionTune(ConnectionTuneArgs),
    ConnectionTuneOk(ConnectionTuneOkArgs),
    ConnectionOpen(ConnectionOpenArgs),
    ConnectionOpenOk,
    ConnectionClose(ConnectionCloseArgs),
    ConnectionCloseOk,
    ChannelOpen,
    ChannelOpenOk,
    ChannelClose(ChannelCloseArgs),
    ChannelCloseOk,
    ExchangeDeclare(ExchangeDeclareArgs),
    ExchangeDeclareOk,
    ExchangeDelete(ExchangeDeleteArgs),
    ExchangeDeleteOk,
    QueueDeclare(QueueDeclareArgs),
    QueueDeclareOk(QueueDeclareOkArgs),
    QueueBind(QueueBindArgs),
    QueueBindOk,
    QueuePurge(QueuePurgeArgs),
    QueuePurgeOk(QueuePurgeOkArgs),
    QueueDelete(QueueDeleteArgs),
    QueueDeleteOk(QueueDeleteOkArgs),
    QueueUnbind(QueueUnbindArgs),
    QueueUnbindOk,
    BasicConsume(BasicConsumeArgs),
    BasicConsumeOk(BasicConsumeOkArgs),
    BasicCancel(BasicCancelArgs),
    BasicCancelOk(BasicCancelOkArgs),
    BasicGet(BasicGetArgs),
    BasicGetOk(BasicGetOkArgs),
    BasicGetEmpty,
    BasicPublish(BasicPublishArgs),
    BasicReturn(BasicReturnArgs),
    BasicDeliver(BasicDeliverArgs),
    BasicAck(BasicAckArgs),
    ConfirmSelect(ConfirmSelectArgs),
    ConfirmSelectOk,
}

bitflags! {
    pub struct HeaderPropertyFlags: u16 {
        const CLUSTER_ID       = 0b0000_0000_0000_0100;
        const APP_ID           = 0b0000_0000_0000_1000;
        const USER_ID          = 0b0000_0000_0001_0000;
        const MESSAGE_TYPE     = 0b0000_0000_0010_0000;
        const TIMESTAMP        = 0b0000_0000_0100_0000;
        const MESSAGE_ID       = 0b0000_0000_1000_0000;
        const EXPIRATION       = 0b0000_0001_0000_0000;
        const REPLY_TO         = 0b0000_0010_0000_0000;
        const CORRELATION_ID   = 0b0000_0100_0000_0000;
        const PRIORITY         = 0b0000_1000_0000_0000;
        const DELIVERY_MODE    = 0b0001_0000_0000_0000;
        const HEADERS          = 0b0010_0000_0000_0000;
        const CONTENT_ENCODING = 0b0100_0000_0000_0000;
        const CONTENT_TYPE     = 0b1000_0000_0000_0000;
    }
}

impl Default for HeaderPropertyFlags {
    fn default() -> Self {
        HeaderPropertyFlags::empty()
    }
}

// FIXME The property flags is 16 bits but a message can contain more than 16 properties.
// If the last bit of the flags is 0, there are more properties sent. I cannot see how, I need to
// see a good example.
#[derive(Debug, Default)]
pub struct ContentHeaderFrame {
    pub channel: Channel,
    pub class_id: ClassId,
    pub weight: Weight,
    pub body_size: u64,
    pub prop_flags: HeaderPropertyFlags,
    pub cluster_id: Option<String>,
    pub app_id: Option<String>,
    pub user_id: Option<String>,
    pub message_type: Option<String>,
    pub timestamp: Option<u64>,
    pub message_id: Option<String>,
    pub expiration: Option<String>,
    pub reply_to: Option<String>,
    pub correlation_id: Option<String>,
    pub priority: Option<u8>,
    pub delivery_mode: Option<u8>,
    pub headers: Option<FieldTable>,
    pub content_encoding: Option<String>,
    pub content_type: Option<String>,
}

impl ContentHeaderFrame {
    pub fn with_content_type(&mut self, content_type: String) -> &ContentHeaderFrame {
        self.content_type = Some(content_type);
        self.prop_flags.set(HeaderPropertyFlags::CONTENT_TYPE, true);
        self
    }
}

pub struct ContentBodyFrame {
    pub channel: Channel,
    // TODO here we need something which can be cloned cheap like Box or Rc. Sometimes we can move
    // out this from the struct and we can build a new struct. But we need to avoid the
    // byte-to-byte copy.
    pub body: Vec<u8>,
}

impl std::fmt::Debug for ContentBodyFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let body = String::from_utf8_lossy(&self.body[..std::cmp::min(64usize, self.body.len())]);

        f.write_fmt(format_args!(
            "ContentBodyFrame {{ channel: {}, body: \"{}\" }}",
            &self.channel, body
        ))
    }
}

/// Type alias for inner type of field value.
pub type FieldTable = HashMap<String, AMQPFieldValue>;

#[derive(Debug)]
pub enum AMQPValue {
    //    Bool(bool),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    SimpleString(String),
    LongString(String),
    EmptyFieldTable,
    FieldTable(Box<FieldTable>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum AMQPFieldValue {
    Bool(bool),
    //    SimpleString(String),
    LongString(String),
    EmptyFieldTable,
    FieldTable(Box<FieldTable>),
}

#[derive(Debug, Default)]
pub struct ConnectionStartArgs {
    pub version_major: u8,
    pub version_minor: u8,
    pub capabilities: Option<FieldTable>,
    pub properties: Option<FieldTable>,
    pub mechanisms: String,
    pub locales: String,
}

#[derive(Debug, Default)]
pub struct ConnectionStartOkArgs {
    pub capabilities: Option<FieldTable>,
    pub properties: Option<FieldTable>,
    pub mechanism: String,
    pub response: String,
    pub locale: String,
}

#[derive(Debug, Default)]
pub struct ConnectionTuneArgs {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
}

#[derive(Debug, Default)]
pub struct ConnectionTuneOkArgs {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
}

#[derive(Debug, Default)]
pub struct ConnectionOpenArgs {
    pub virtual_host: String,
    pub insist: bool,
}

#[derive(Debug, Default)]
pub struct ConnectionCloseArgs {
    pub code: u16,
    pub text: String,
    pub class_id: u16,
    pub method_id: u16,
}

#[derive(Debug, Default)]
pub struct ChannelCloseArgs {
    pub code: u16,
    pub text: String,
    pub class_id: u16,
    pub method_id: u16,
}

bitflags! {
    pub struct ExchangeDeclareFlags: u8 {
        const PASSIVE = 0b00000001;
        const DURABLE = 0b00000010;
        const AUTO_DELETE = 0b00000100;
        const INTERNAL = 0b00001000;
        const NO_WAIT = 0b00010000;
    }
}

impl Default for ExchangeDeclareFlags {
    fn default() -> Self {
        ExchangeDeclareFlags::empty()
    }
}

#[derive(Debug, Default)]
pub struct ExchangeDeclareArgs {
    pub exchange_name: String,
    pub exchange_type: String,
    pub flags: ExchangeDeclareFlags,
    pub args: Option<FieldTable>,
}

bitflags! {
    pub struct ExchangeDeleteFlags: u8 {
        const IF_UNUSED = 0b00000001;
        const NO_WAIT = 0b00000010;
    }
}

impl Default for ExchangeDeleteFlags {
    fn default() -> Self {
        ExchangeDeleteFlags::empty()
    }
}

#[derive(Debug, Default)]
pub struct ExchangeDeleteArgs {
    pub exchange_name: String,
    pub flags: ExchangeDeleteFlags,
}

bitflags! {
    pub struct QueueDeclareFlags: u8 {
        const PASSIVE = 0b00000001;
        const DURABLE = 0b00000010;
        const EXCLUSIVE = 0b00000100;
        const AUTO_DELETE = 0b00001000;
        const NO_WAIT = 0b00010000;
    }
}

impl Default for QueueDeclareFlags {
    fn default() -> Self {
        QueueDeclareFlags::empty()
    }
}

#[derive(Debug, Default)]
pub struct QueueDeclareArgs {
    pub name: String,
    pub flags: QueueDeclareFlags,
    pub args: Option<FieldTable>,
}

#[derive(Debug, Default)]
pub struct QueueDeclareOkArgs {
    pub name: String,
    pub message_count: u32,
    pub consumer_count: u32,
}

#[derive(Debug, Default)]
pub struct QueueBindArgs {
    pub queue_name: String,
    pub exchange_name: String,
    pub routing_key: String,
    pub no_wait: bool,
    pub args: Option<FieldTable>,
}

#[derive(Debug, Default)]
pub struct QueuePurgeArgs {
    pub queue_name: String,
    pub no_wait: bool,
}

#[derive(Debug, Default)]
pub struct QueuePurgeOkArgs {
    pub message_count: u32,
}

bitflags! {
    pub struct QueueDeleteFlags: u8 {
        const IF_UNUSED = 0b00000001;
        const IF_EMPTY = 0b00000010;
        const NO_WAIT = 0b00000100;
    }
}

impl Default for QueueDeleteFlags {
    fn default() -> Self {
        QueueDeleteFlags::empty()
    }
}

#[derive(Debug, Default)]
pub struct QueueDeleteArgs {
    pub queue_name: String,
    pub flags: QueueDeleteFlags,
}

#[derive(Debug, Default)]
pub struct QueueDeleteOkArgs {
    pub message_count: u32,
}

#[derive(Debug, Default)]
pub struct QueueUnbindArgs {
    pub queue_name: String,
    pub exchange_name: String,
    pub routing_key: String,
    pub args: Option<FieldTable>,
}

bitflags! {
    pub struct BasicConsumeFlags: u8 {
        const NO_LOCAL = 0b00000001;
        const NO_ACK = 0b00000010;
        const EXCLUSIVE = 0b00000100;
        const NO_WAIT = 0b00001000;
    }
}

impl Default for BasicConsumeFlags {
    fn default() -> Self {
        BasicConsumeFlags::empty()
    }
}

#[derive(Debug, Default)]
pub struct BasicConsumeArgs {
    pub queue: String,
    pub consumer_tag: String,
    pub flags: BasicConsumeFlags,
    pub args: Option<FieldTable>,
}

#[derive(Clone, Debug, Default)]
pub struct BasicConsumeOkArgs {
    pub consumer_tag: String,
}

#[derive(Clone, Debug, Default)]
pub struct BasicCancelArgs {
    pub consumer_tag: String,
    pub no_wait: bool,
}

#[derive(Clone, Debug, Default)]
pub struct BasicCancelOkArgs {
    pub consumer_tag: String,
}

bitflags! {
    pub struct BasicGetFlags: u8 {
        const NO_ACK = 0b00000001;
    }
}

impl Default for BasicGetFlags {
    fn default() -> Self {
        BasicGetFlags::empty()
    }
}

#[derive(Clone, Debug, Default)]
pub struct BasicGetArgs {
    pub queue: String,
    pub flags: BasicGetFlags,
}

bitflags! {
    pub struct BasicGetOkFlags: u8 {
        const REDELIVERED = 0b00000001;
    }
}

impl Default for BasicGetOkFlags {
    fn default() -> Self {
        BasicGetOkFlags::empty()
    }
}

#[derive(Clone, Debug, Default)]
pub struct BasicGetOkArgs {
    pub delivery_tag: u64,
    pub flags: BasicGetOkFlags,
    pub exchange_name: String,
    pub routing_key: String,
    pub message_count: u32,
}

bitflags! {
    pub struct BasicPublishFlags: u8 {
        const MANDATORY = 0b00000001;
        const IMMEDIATE = 0b00000010;
    }
}

impl Default for BasicPublishFlags {
    fn default() -> Self {
        BasicPublishFlags::empty()
    }
}

#[derive(Clone, Debug, Default)]
pub struct BasicPublishArgs {
    pub exchange_name: String,
    pub routing_key: String,
    pub flags: BasicPublishFlags,
}

#[derive(Clone, Debug, Default)]
pub struct BasicReturnArgs {
    pub reply_code: u16,
    pub reply_text: String,
    pub exchange_name: String,
    pub routing_key: String,
}

#[derive(Clone, Debug, Default)]
pub struct BasicDeliverArgs {
    pub consumer_tag: String,
    pub delivery_tag: u64,
    pub redelivered: bool,
    pub exchange_name: String,
    pub routing_key: String,
}

#[derive(Clone, Debug, Default)]
pub struct BasicAckArgs {
    pub delivery_tag: u64,
    pub multiple: bool,
}

#[derive(Clone, Debug, Default)]
pub struct ConfirmSelectArgs {
    pub no_wait: bool,
}

impl From<ContentHeaderFrame> for AMQPFrame {
    fn from(chf: ContentHeaderFrame) -> AMQPFrame {
        AMQPFrame::ContentHeader(chf)
    }
}

impl From<ContentBodyFrame> for AMQPFrame {
    fn from(cbf: ContentBodyFrame) -> AMQPFrame {
        AMQPFrame::ContentBody(cbf)
    }
}

/// Split class id and method id from `u32` combined code.
pub fn split_class_method(cm: u32) -> (u16, u16) {
    let method_id = (cm & 0x0000FFFF) as u16;
    let class_id = (cm >> 16) as u16;

    (class_id, method_id)
}

/// Combine class id and method id to a single `u32` value
pub fn unify_class_method(class: u16, method: u16) -> u32 {
    ((class as u32) << 16) | (method as u32)
}

pub fn heartbeat() -> AMQPFrame {
    AMQPFrame::Heartbeat(0)
}

pub fn connection_start(channel: Channel) -> AMQPFrame {
    let mut capabilities = FieldTable::new();

    capabilities.insert("publisher_confirms".into(), AMQPFieldValue::Bool(true));
    capabilities.insert("exchange_exchange_bindings".into(), AMQPFieldValue::Bool(true));
    capabilities.insert("basic.nack".into(), AMQPFieldValue::Bool(true));
    capabilities.insert("consumer_cancel_notify".into(), AMQPFieldValue::Bool(true));
    capabilities.insert("connection.blocked".into(), AMQPFieldValue::Bool(true));
    capabilities.insert("consumer_priorities".into(), AMQPFieldValue::Bool(true));
    capabilities.insert("authentication_failure_close".into(), AMQPFieldValue::Bool(true));
    capabilities.insert("per_consumer_qos".into(), AMQPFieldValue::Bool(true));
    capabilities.insert("direct_reply_to".into(), AMQPFieldValue::Bool(true));

    let mut server_properties = FieldTable::new();

    server_properties.insert(
        "capabilities".into(),
        AMQPFieldValue::FieldTable(Box::new(capabilities)),
    );
    server_properties.insert("product".into(), AMQPFieldValue::LongString("MetalMQ server".into()));
    server_properties.insert("version".into(), AMQPFieldValue::LongString("0.1.0".into()));

    AMQPFrame::Method(
        channel,
        CONNECTION_START,
        MethodFrameArgs::ConnectionStart(ConnectionStartArgs {
            version_major: 0,
            version_minor: 9,
            capabilities: None,
            properties: Some(server_properties),
            mechanisms: "PLAIN".into(),
            locales: "en_US".into(),
        }),
    )
}

// TODO here will be an Authentication enum with the different possibilities
pub fn connection_start_ok(username: &str, password: &str, capabilities: FieldTable) -> AMQPFrame {
    let mut client_properties = FieldTable::new();

    client_properties.insert("product".into(), AMQPFieldValue::LongString("metalmq-client".into()));
    client_properties.insert("platform".into(), AMQPFieldValue::LongString("Rust".into()));
    client_properties.insert(
        "capabilities".into(),
        AMQPFieldValue::FieldTable(Box::new(capabilities)),
    );
    // TODO get the version from the build vars or an external file
    client_properties.insert("version".into(), AMQPFieldValue::LongString("0.1.0".into()));

    let mut auth = vec![0x00];
    auth.extend_from_slice(username.as_bytes());
    auth.push(0x00);
    auth.extend_from_slice(password.as_bytes());

    let auth_string = String::from_utf8(auth).unwrap();

    AMQPFrame::Method(
        0,
        CONNECTION_START_OK,
        MethodFrameArgs::ConnectionStartOk(ConnectionStartOkArgs {
            capabilities: None,
            properties: Some(client_properties),
            mechanism: "PLAIN".into(),
            response: auth_string,
            locale: "en_US".into(),
        }),
    )
}

pub fn connection_tune(channel: Channel) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        CONNECTION_TUNE,
        MethodFrameArgs::ConnectionTune(ConnectionTuneArgs {
            channel_max: 2047,
            frame_max: 131_072,
            heartbeat: 60,
        }),
    )
}

pub fn connection_tune_ok(channel: Channel) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        CONNECTION_TUNE_OK,
        MethodFrameArgs::ConnectionTuneOk(ConnectionTuneOkArgs {
            channel_max: 2047,
            frame_max: 131_072,
            heartbeat: 60,
        }),
    )
}

pub fn connection_open(channel: Channel, virtual_host: &str) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        CONNECTION_OPEN,
        MethodFrameArgs::ConnectionOpen(ConnectionOpenArgs {
            virtual_host: virtual_host.to_string(),
            insist: true,
        }),
    )
}

pub fn connection_open_ok(channel: Channel) -> AMQPFrame {
    AMQPFrame::Method(channel, CONNECTION_OPEN_OK, MethodFrameArgs::ConnectionOpenOk)
}

pub fn connection_close(channel: Channel, code: u16, text: &str, class_id: u16, method_id: u16) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        CONNECTION_CLOSE,
        MethodFrameArgs::ConnectionClose(ConnectionCloseArgs {
            code,
            text: text.into(),
            class_id,
            method_id,
        }),
    )
}

pub fn connection_close_ok(channel: Channel) -> AMQPFrame {
    AMQPFrame::Method(channel, CONNECTION_CLOSE_OK, MethodFrameArgs::ConnectionCloseOk)
}

pub fn channel_open(channel: Channel) -> AMQPFrame {
    AMQPFrame::Method(channel, CHANNEL_OPEN, MethodFrameArgs::ChannelOpen)
}

pub fn channel_open_ok(channel: Channel) -> AMQPFrame {
    AMQPFrame::Method(channel, CHANNEL_OPEN_OK, MethodFrameArgs::ChannelOpenOk)
}

pub fn channel_close(channel: Channel, code: u16, text: &str, class_id: u16, method_id: u16) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        CHANNEL_CLOSE,
        MethodFrameArgs::ChannelClose(ChannelCloseArgs {
            code,
            text: text.into(),
            class_id,
            method_id,
        }),
    )
}

pub fn channel_close_ok(channel: Channel) -> AMQPFrame {
    AMQPFrame::Method(channel, CHANNEL_CLOSE_OK, MethodFrameArgs::ChannelCloseOk)
}

pub fn exchange_declare(
    channel: Channel,
    exchange_name: &str,
    exchange_type: &str,
    flags: Option<ExchangeDeclareFlags>,
    args: Option<FieldTable>,
) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        EXCHANGE_DECLARE,
        MethodFrameArgs::ExchangeDeclare(ExchangeDeclareArgs {
            exchange_name: exchange_name.to_string(),
            exchange_type: exchange_type.to_string(),
            flags: flags.unwrap_or_default(),
            args,
        }),
    )
}

pub fn exchange_declare_ok(channel: Channel) -> AMQPFrame {
    AMQPFrame::Method(channel, EXCHANGE_DECLARE_OK, MethodFrameArgs::ExchangeDeclareOk)
}

pub fn exchange_delete(channel: Channel, exchange_name: &str, flags: Option<ExchangeDeleteFlags>) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        EXCHANGE_DELETE,
        MethodFrameArgs::ExchangeDelete(ExchangeDeleteArgs {
            exchange_name: exchange_name.to_string(),
            flags: flags.unwrap_or_default(),
        }),
    )
}

pub fn exchange_delete_ok(channel: Channel) -> AMQPFrame {
    AMQPFrame::Method(channel, EXCHANGE_DELETE_OK, MethodFrameArgs::ExchangeDeleteOk)
}

pub fn queue_bind(
    channel: Channel,
    queue_name: &str,
    exchange_name: &str,
    routing_key: &str,
    args: Option<FieldTable>,
) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        QUEUE_BIND,
        MethodFrameArgs::QueueBind(QueueBindArgs {
            queue_name: queue_name.to_string(),
            exchange_name: exchange_name.to_string(),
            routing_key: routing_key.to_string(),
            no_wait: false,
            args,
        }),
    )
}

pub fn queue_bind_ok(channel: Channel) -> AMQPFrame {
    AMQPFrame::Method(channel, QUEUE_BIND_OK, MethodFrameArgs::QueueBindOk)
}

pub fn queue_purge(channel: Channel, queue_name: &str) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        QUEUE_PURGE,
        MethodFrameArgs::QueuePurge(QueuePurgeArgs {
            queue_name: queue_name.to_owned(),
            no_wait: false,
        }),
    )
}

pub fn queue_purge_ok(channel: Channel, message_count: u32) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        QUEUE_PURGE_OK,
        MethodFrameArgs::QueuePurgeOk(QueuePurgeOkArgs { message_count }),
    )
}

pub fn queue_delete(channel: Channel, queue_name: &str, flags: Option<QueueDeleteFlags>) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        QUEUE_DELETE,
        MethodFrameArgs::QueueDelete(QueueDeleteArgs {
            queue_name: queue_name.to_string(),
            flags: flags.unwrap_or_default(),
        }),
    )
}

pub fn queue_delete_ok(channel: Channel, message_count: u32) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        QUEUE_DELETE_OK,
        MethodFrameArgs::QueueDeleteOk(QueueDeleteOkArgs { message_count }),
    )
}

pub fn queue_unbind(
    channel: Channel,
    queue_name: &str,
    exchange_name: &str,
    routing_key: &str,
    args: Option<FieldTable>,
) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        QUEUE_UNBIND,
        MethodFrameArgs::QueueUnbind(QueueUnbindArgs {
            queue_name: queue_name.to_string(),
            exchange_name: exchange_name.to_string(),
            routing_key: routing_key.to_string(),
            args,
        }),
    )
}

pub fn queue_unbind_ok(channel: Channel) -> AMQPFrame {
    AMQPFrame::Method(channel, QUEUE_UNBIND_OK, MethodFrameArgs::QueueUnbindOk)
}

pub fn queue_declare(
    channel: Channel,
    queue_name: &str,
    flags: Option<QueueDeclareFlags>,
    args: Option<FieldTable>,
) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        QUEUE_DECLARE,
        MethodFrameArgs::QueueDeclare(QueueDeclareArgs {
            name: queue_name.to_string(),
            flags: flags.unwrap_or_default(),
            args,
        }),
    )
}

pub fn queue_declare_ok(channel: Channel, queue_name: String, message_count: u32, consumer_count: u32) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        QUEUE_DECLARE_OK,
        MethodFrameArgs::QueueDeclareOk(QueueDeclareOkArgs {
            name: queue_name,
            message_count,
            consumer_count,
        }),
    )
}

pub fn basic_consume(
    channel: Channel,
    queue_name: &str,
    consumer_tag: &str,
    flags: Option<BasicConsumeFlags>,
    args: Option<FieldTable>,
) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        BASIC_CONSUME,
        MethodFrameArgs::BasicConsume(BasicConsumeArgs {
            queue: queue_name.to_string(),
            consumer_tag: consumer_tag.to_string(),
            flags: flags.unwrap_or_default(),
            args,
        }),
    )
}

pub fn basic_consume_ok(channel: Channel, consumer_tag: &str) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        BASIC_CONSUME_OK,
        MethodFrameArgs::BasicConsumeOk(BasicConsumeOkArgs {
            consumer_tag: consumer_tag.to_string(),
        }),
    )
}

pub fn basic_cancel(channel: Channel, consumer_tag: &str, no_wait: bool) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        BASIC_CANCEL,
        MethodFrameArgs::BasicCancel(BasicCancelArgs {
            consumer_tag: consumer_tag.to_string(),
            no_wait,
        }),
    )
}

pub fn basic_cancel_ok(channel: Channel, consumer_tag: &str) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        BASIC_CANCEL_OK,
        MethodFrameArgs::BasicCancelOk(BasicCancelOkArgs {
            consumer_tag: consumer_tag.to_string(),
        }),
    )
}

pub fn basic_publish(
    channel: Channel,
    exchange_name: &str,
    routing_key: &str,
    flags: Option<BasicPublishFlags>,
) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        BASIC_PUBLISH,
        MethodFrameArgs::BasicPublish(BasicPublishArgs {
            exchange_name: exchange_name.to_string(),
            routing_key: routing_key.to_string(),
            flags: flags.unwrap_or_default(),
        }),
    )
}

pub fn basic_return(
    channel: Channel,
    reply_code: u16,
    reply_text: &str,
    exchange_name: &str,
    routing_key: &str,
) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        BASIC_RETURN,
        MethodFrameArgs::BasicReturn(BasicReturnArgs {
            reply_code,
            reply_text: reply_text.to_string(),
            exchange_name: exchange_name.to_string(),
            routing_key: routing_key.to_string(),
        }),
    )
}

pub fn basic_deliver(
    channel: Channel,
    consumer_tag: &str,
    delivery_tag: u64,
    redelivered: bool,
    exchange_name: &str,
    routing_key: &str,
) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        BASIC_DELIVER,
        MethodFrameArgs::BasicDeliver(BasicDeliverArgs {
            consumer_tag: consumer_tag.to_string(),
            delivery_tag,
            redelivered,
            exchange_name: exchange_name.to_string(),
            routing_key: routing_key.to_string(),
        }),
    )
}

pub fn basic_ack(channel: Channel, delivery_tag: u64, multiple: bool) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        BASIC_ACK,
        MethodFrameArgs::BasicAck(BasicAckArgs { delivery_tag, multiple }),
    )
}

pub fn confirm_select(channel: Channel, no_wait: bool) -> AMQPFrame {
    AMQPFrame::Method(
        channel,
        CONFIRM_SELECT,
        MethodFrameArgs::ConfirmSelect(ConfirmSelectArgs { no_wait }),
    )
}

pub fn confirm_select_ok(channel: Channel) -> AMQPFrame {
    AMQPFrame::Method(channel, CONFIRM_SELECT_OK, MethodFrameArgs::ConfirmSelectOk)
}

pub fn content_header(channel: Channel, size: u64) -> ContentHeaderFrame {
    ContentHeaderFrame {
        channel,
        class_id: 0x003C,
        weight: 0,
        body_size: size,
        prop_flags: HeaderPropertyFlags::empty(),
        ..Default::default()
    }
}

pub fn content_body(channel: Channel, payload: &[u8]) -> ContentBodyFrame {
    ContentBodyFrame {
        channel,
        body: payload.to_vec(),
    }
}
