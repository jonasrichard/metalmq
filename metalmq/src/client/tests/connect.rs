use metalmq_codec::frame;

use crate::{client::tests::TestContext, Result};

#[tokio::test]
async fn client_connect() -> Result<()> {
    let mut tc = TestContext::setup();

    tc.connection
        .handle_connection_open(frame::ConnectionOpenArgs {
            virtual_host: "/".into(),
            insist: false,
        })
        .await?;

    Ok(())
}
