use super::{AMQPFieldValue, AMQPFrame, FieldTable, MethodFrameArgs};

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

impl ConnectionStartArgs {
    pub fn new() -> Self {
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

        Self {
            version_major: 0,
            version_minor: 9,
            capabilities: None,
            properties: Some(server_properties),
            mechanisms: "PLAIN".into(),
            locales: "en_US".into(),
        }
    }

    pub fn frame(self) -> AMQPFrame {
        AMQPFrame::Method(0, super::CONNECTION_START, MethodFrameArgs::ConnectionStart(self))
    }
}

impl ConnectionStartOkArgs {
    pub fn new(username: &str, password: &str) -> Self {
        let mut caps = FieldTable::new();

        caps.insert("authentication_failure_close".to_string(), AMQPFieldValue::Bool(true));

        let mut client_properties = FieldTable::new();

        client_properties.insert("product".into(), AMQPFieldValue::LongString("metalmq-client".into()));
        client_properties.insert("platform".into(), AMQPFieldValue::LongString("Rust".into()));
        client_properties.insert("capabilities".into(), AMQPFieldValue::FieldTable(Box::new(caps)));
        // TODO get the version from the build vars or an external file
        client_properties.insert("version".into(), AMQPFieldValue::LongString("0.1.0".into()));

        let mut auth = vec![0x00];
        auth.extend_from_slice(username.as_bytes());
        auth.push(0x00);
        auth.extend_from_slice(password.as_bytes());

        let auth_string = String::from_utf8(auth).unwrap();

        Self {
            capabilities: None,
            properties: Some(client_properties),
            mechanism: "PLAIN".into(),
            response: auth_string,
            locale: "en_US".into(),
        }
    }

    pub fn frame(self) -> AMQPFrame {
        AMQPFrame::Method(0, super::CONNECTION_START_OK, MethodFrameArgs::ConnectionStartOk(self))
    }
}

impl ConnectionOpenArgs {
    pub fn virtual_host(mut self, virtual_host: &str) -> Self {
        self.virtual_host = virtual_host.to_string();
        self
    }

    pub fn frame(self) -> AMQPFrame {
        AMQPFrame::Method(0, super::CONNECTION_OPEN, super::MethodFrameArgs::ConnectionOpen(self))
    }
}

#[derive(Debug, Default)]
pub struct ConnectionCloseArgs {
    pub code: u16,
    pub text: String,
    pub class_id: u16,
    pub method_id: u16,
}

pub fn connection_tune() -> AMQPFrame {
    AMQPFrame::Method(
        0,
        super::CONNECTION_TUNE,
        MethodFrameArgs::ConnectionTune(ConnectionTuneArgs {
            channel_max: 2047,
            frame_max: 131_072,
            heartbeat: 60,
        }),
    )
}

pub fn connection_tune_ok() -> AMQPFrame {
    AMQPFrame::Method(
        0,
        super::CONNECTION_TUNE_OK,
        MethodFrameArgs::ConnectionTuneOk(ConnectionTuneOkArgs {
            channel_max: 2047,
            frame_max: 131_072,
            heartbeat: 60,
        }),
    )
}

pub fn connection_open_ok() -> AMQPFrame {
    AMQPFrame::Method(0, super::CONNECTION_OPEN_OK, MethodFrameArgs::ConnectionOpenOk)
}

pub fn connection_close(code: u16, text: &str, class_method: u32) -> AMQPFrame {
    let (class_id, method_id) = super::split_class_method(class_method);

    AMQPFrame::Method(
        0,
        super::CONNECTION_CLOSE,
        MethodFrameArgs::ConnectionClose(ConnectionCloseArgs {
            code,
            text: text.into(),
            class_id,
            method_id,
        }),
    )
}

pub fn connection_close_ok() -> AMQPFrame {
    AMQPFrame::Method(0, super::CONNECTION_CLOSE_OK, MethodFrameArgs::ConnectionCloseOk)
}
