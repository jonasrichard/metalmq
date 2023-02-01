use tokio::sync::mpsc;

use crate::{
    client::{state::Connection, tests::to_runtime_error, ConnectionError},
    exchange::manager::ExchangeManagerSink,
    queue::manager::QueueManagerSink,
    tests::recv_timeout,
    Context,
};
use metalmq_codec::{codec::Frame, frame};

/// TestCase for System Under Test which spawns an exchange manager and a queue manager and can
/// test more integrated features like forwarding messages from exchanges to queues.
pub struct TestCase {
    em: ExchangeManagerSink,
    qm: QueueManagerSink,
}

impl TestCase {
    fn new() -> Self {
        let em = crate::exchange::manager::start();
        let qm = crate::queue::manager::start(em.clone());

        Self { em, qm }
    }

    fn new_client(&self) -> (Connection, mpsc::Receiver<Frame>) {
        let ctx = Context {
            exchange_manager: self.em.clone(),
            queue_manager: self.qm.clone(),
        };
        let (client_tx, client_rx) = mpsc::channel(1);

        (Connection::new(ctx, client_tx), client_rx)
    }
}

fn unpack_single_frame(f: Frame) -> frame::AMQPFrame {
    if let Frame::Frame(single_frame) = f {
        single_frame
    } else {
        panic!("Frame {f:?} is not a single frame");
    }
}

#[tokio::test]
async fn bind_queue_with_validation() {
    use frame::ExchangeDeclareArgs;

    let tc = TestCase::new();
    let (mut client, mut client_rx) = tc.new_client();

    // Normal exchange declaration sends back ExchangeDeclareOk
    let args = ExchangeDeclareArgs::default()
        .exchange_name("normal-exchange")
        .exchange_type("direct");
    client.exchange_declare(1u16, args).await.unwrap();

    let exchange_declare_ok = unpack_single_frame(recv_timeout(&mut client_rx).await.unwrap());

    assert!(matches!(exchange_declare_ok, frame::AMQPFrame::Method(_, _, _)));

    // Declaring reserved exchanges ends up in channel error
    let args = ExchangeDeclareArgs::default()
        .exchange_name("amq.reserved")
        .exchange_type("direct");
    client.exchange_declare(2u16, args).await.unwrap();

    let channel_error = unpack_single_frame(recv_timeout(&mut client_rx).await.unwrap());

    assert!(matches!(
        channel_error,
        frame::AMQPFrame::Method(
            2u16,
            _,
            frame::MethodFrameArgs::ChannelClose(frame::ChannelCloseArgs { code: 403, .. })
        )
    ));

    // Declaring invalid exchange e.g. empty name ends up in connection error
    let args = ExchangeDeclareArgs::default();
    let result = client.exchange_declare(3u16, args).await;

    let channel_error = to_runtime_error(result);

    assert_eq!(channel_error.code, ConnectionError::CommandInvalid as u16);
}
