use std::{net::UdpSocket, time::Duration};

use anyhow::Result;
use iroh::{endpoint::Connection, Endpoint};
use n0_future::task::AbortOnDropHandle;
use tokio::sync::mpsc;

use crate::{
    api::Store,
    get,
    provider::{
        events::{EventMask, EventSender, ProviderMessage, RequestMode},
        handle_connection,
    },
    store::mem::MemStore,
    ALPN,
};

const DEADLINE: Duration = Duration::from_secs(5);

async fn connected_endpoints() -> Result<(Endpoint, Endpoint, Connection, Connection)> {
    let server = Endpoint::builder(iroh::endpoint::presets::Minimal)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await?;
    let client = Endpoint::builder(iroh::endpoint::presets::Minimal)
        .bind()
        .await?;
    let address = server.addr();
    let (client_connection, server_connection) =
        tokio::join!(client.connect(address, ALPN), async {
            server.accept().await.expect("server must accept").await
        },);
    Ok((server, client, client_connection?, server_connection?))
}

async fn next_request(events: &mut mpsc::Receiver<ProviderMessage>) -> ProviderMessage {
    tokio::time::timeout(DEADLINE, events.recv())
        .await
        .expect("the concurrent request must be polled")
        .expect("provider events must remain open")
}

#[derive(Clone, Copy)]
enum StopConnection {
    AbortHandler,
    CloseClient,
}

async fn assert_pending_requests_drop_with_connection(stop: StopConnection) -> Result<()> {
    let (server, client, connection, server_connection) = connected_endpoints().await?;
    let socket = server
        .bound_sockets()
        .into_iter()
        .find(|address| address.is_ipv4())
        .unwrap();
    let store = MemStore::new();
    let tag = store.add_slice(b"pending provider stream").await?;
    let hash = tag.hash;
    let (events, mut event_rx) = EventSender::channel(
        8,
        EventMask {
            get: RequestMode::Intercept,
            ..EventMask::DEFAULT
        },
    );
    let provider = AbortOnDropHandle::new(tokio::spawn(handle_connection(
        server_connection,
        Store::clone(&store),
        events,
    )));
    let first_connection = connection.clone();
    let first = AbortOnDropHandle::new(tokio::spawn(async move {
        get::request::get_blob(first_connection, hash).await
    }));
    let second_connection = connection.clone();
    let second = AbortOnDropHandle::new(tokio::spawn(async move {
        get::request::get_blob(second_connection, hash).await
    }));
    let ProviderMessage::GetRequestReceived(first_gate) = next_request(&mut event_rx).await else {
        panic!("expected first request gate");
    };
    let ProviderMessage::GetRequestReceived(second_gate) = next_request(&mut event_rx).await else {
        panic!("expected second request gate");
    };
    assert!(!first.is_finished());
    assert!(!second.is_finished());
    assert_eq!(
        UdpSocket::bind(socket).unwrap_err().kind(),
        std::io::ErrorKind::AddrInUse
    );

    match stop {
        StopConnection::AbortHandler => provider.abort(),
        StopConnection::CloseClient => connection.close(0u32.into(), b"test connection close"),
    }
    let stopped = tokio::time::timeout(DEADLINE, provider).await?;
    match stop {
        StopConnection::AbortHandler => assert!(stopped.unwrap_err().is_cancelled()),
        StopConnection::CloseClient => stopped?,
    }
    // Sending would succeed if an unjoined stream task still owned its interception
    // receiver. No release is needed for the connection owner to drop both futures.
    assert!(first_gate.tx.send(Ok(())).await.is_err());
    assert!(second_gate.tx.send(Ok(())).await.is_err());
    server.close().await;
    drop(server);
    let _rebound = UdpSocket::bind(socket)
        .expect("connection teardown must release every stream socket holder");
    first.abort();
    second.abort();
    let _ = first.await;
    let _ = second.await;
    drop(connection);
    client.close().await;
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn aborting_provider_drops_all_pending_stream_futures_before_join_returns() -> Result<()> {
    assert_pending_requests_drop_with_connection(StopConnection::AbortHandler).await
}

#[tokio::test]
async fn closing_connection_drops_all_pending_stream_futures_before_return() -> Result<()> {
    assert_pending_requests_drop_with_connection(StopConnection::CloseClient).await
}

#[tokio::test]
async fn a_gated_stream_does_not_serialize_other_requests_on_the_connection() -> Result<()> {
    let (server, client, connection, server_connection) = connected_endpoints().await?;
    let store = MemStore::new();
    let first_tag = store.add_slice(b"first provider stream").await?;
    let second_tag = store.add_slice(b"second provider stream").await?;
    let first_hash = first_tag.hash;
    let second_hash = second_tag.hash;
    let (events, mut event_rx) = EventSender::channel(
        8,
        EventMask {
            get: RequestMode::Intercept,
            ..EventMask::DEFAULT
        },
    );
    let provider = AbortOnDropHandle::new(tokio::spawn(handle_connection(
        server_connection,
        Store::clone(&store),
        events,
    )));
    let first_connection = connection.clone();
    let first = AbortOnDropHandle::new(tokio::spawn(async move {
        get::request::get_blob(first_connection, first_hash).await
    }));
    let second_connection = connection.clone();
    let second = AbortOnDropHandle::new(tokio::spawn(async move {
        get::request::get_blob(second_connection, second_hash).await
    }));
    let ProviderMessage::GetRequestReceived(one) = next_request(&mut event_rx).await else {
        panic!("expected first request gate");
    };
    let ProviderMessage::GetRequestReceived(two) = next_request(&mut event_rx).await else {
        panic!("expected second request gate");
    };
    let (first_gate, second_gate) = if one.inner.request.hash == first_hash {
        (one, two)
    } else {
        (two, one)
    };
    assert_eq!(first_gate.inner.request.hash, first_hash);
    assert_eq!(second_gate.inner.request.hash, second_hash);

    second_gate.tx.send(Ok(())).await?;
    let second_data = tokio::time::timeout(DEADLINE, second).await???;
    assert_eq!(second_data.as_ref(), b"second provider stream");
    assert!(
        !first.is_finished(),
        "the first stream is still gated independently"
    );
    first_gate.tx.send(Ok(())).await?;
    let first_data = tokio::time::timeout(DEADLINE, first).await???;
    assert_eq!(first_data.as_ref(), b"first provider stream");

    connection.close(0u32.into(), b"test complete");
    tokio::time::timeout(DEADLINE, provider).await??;
    server.close().await;
    client.close().await;
    store.shutdown().await?;
    Ok(())
}
