#[cfg(test)]
mod tests {
    use crate::{
        client::ChannelError,
        exchange::{
            binding::Bindings,
            handler::{ExchangeCommand, ExchangeState},
            Exchange, ExchangeType,
        },
        message::{Message, MessageContent},
        queue::{handler, Queue},
        ErrorScope, RuntimeError,
    };
    use metalmq_codec::{codec::Frame, frame};
    use std::collections::HashMap;
    use tokio::sync::{mpsc, oneshot};

    async fn recv_timeout<T>(rx: &mut mpsc::Receiver<T>) -> Option<T> {
        let sleep = tokio::time::sleep(tokio::time::Duration::from_secs(1));
        tokio::pin!(sleep);

        tokio::select! {
            frame = rx.recv() => {
                frame
            }
            _ = &mut sleep => {
                return None;
            }
        }
    }

    #[tokio::test]
    async fn send_basic_return_on_mandatory_unroutable_message() {
        let (msg_tx, mut msg_rx) = mpsc::channel(1);
        let mut es = exchange_state_direct("x-name");

        let msg = Message {
            source_connection: "conn-id".to_string(),
            channel: 2,
            content: MessageContent {
                body: b"Okay".to_vec(),
                ..Default::default()
            },
            exchange: "x-name".to_string(),
            routing_key: "".to_string(),
            mandatory: true,
            immediate: false,
        };
        let cmd = ExchangeCommand::Message {
            message: msg,
            outgoing: msg_tx,
        };
        let res = es.handle_command(cmd).await;
        assert!(res.is_ok());

        match recv_timeout(&mut msg_rx).await {
            Some(Frame::Frame(_br)) => assert!(true),
            Some(Frame::Frames(fs)) => if let frame::AMQPFrame::Method(_ch, _cm, _args) = fs.get(0).unwrap() {},
            None => assert!(false, "Basic.Return frame is expected"),
        }
    }

    #[tokio::test]
    async fn cannot_bind_exclusive_queue_with_different_connection() {
        let mut es = exchange_state_direct("xchg-exclusive");

        let (tx, rx) = oneshot::channel();
        let (stx, mut srx) = mpsc::channel(1);
        let cmd = ExchangeCommand::QueueBind {
            conn_id: "123".to_string(),
            channel: 1,
            queue_name: "qqq".to_string(),
            routing_key: "routing".to_string(),
            args: None,
            sink: stx,
            result: tx,
        };

        tokio::spawn(async move {
            handler::start(Queue::default(), "111".to_string(), &mut srx).await;
        });

        let res = es.handle_command(cmd).await;
        assert!(res.is_ok());

        let res2 = rx.await;
        assert!(res2.is_ok());
        let res3 = res2.unwrap();
        assert!(res3.is_err());

        let err = res3.unwrap_err().downcast::<RuntimeError>().unwrap();
        assert_eq!(err.scope, ErrorScope::Channel);
        assert_eq!(err.code, ChannelError::AccessRefused as u16);
    }

    fn exchange_state_direct(name: &str) -> ExchangeState {
        ExchangeState {
            exchange: Exchange {
                name: name.to_string(),
                exchange_type: ExchangeType::Direct,
                durable: false,
                auto_delete: false,
                internal: false,
            },
            bindings: Bindings::Direct(vec![]),
            bound_queues: HashMap::new(),
        }
    }
}
