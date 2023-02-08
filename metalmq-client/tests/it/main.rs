mod direct;
mod exchange;
mod fanout;
mod helper;
mod queue;
mod topic;

use metalmq_client::*;

pub fn unwrap_delivered_message(signal: ConsumerSignal) -> Message {
    match signal {
        ConsumerSignal::Delivered(msg) => msg,
        other => panic!("{other:?} is not a Deliver signal"),
    }
}
