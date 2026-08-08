#[test]
fn signature_payload_is_sorted_and_uses_wire_values() {
    let parameters = Parameters::from([
        ("timestamp".to_owned(), json!(1_700_000_000_000_u64)),
        ("apiKey".to_owned(), json!("public-key")),
        ("recvWindow".to_owned(), json!(5000)),
        ("symbol".to_owned(), json!("BTCUSDT")),
    ]);

    assert_eq!(
        signature_payload(&parameters).unwrap(),
        "apiKey=public-key&recvWindow=5000&symbol=BTCUSDT&timestamp=1700000000000"
    );
}

#[test]
fn signature_payload_rejects_nested_values() {
    let parameters = Parameters::from([("bad".to_owned(), json!({"nested": true}))]);

    assert_eq!(
        signature_payload(&parameters).unwrap_err().kind(),
        ExchangeErrorKind::InvalidRequest
    );
}

#[test]
fn fixed_transport_slots_validate_the_full_request_id() {
    let mut pending: PendingSlots = std::array::from_fn(|_| None);
    let (reply, _response) = oneshot::channel();
    pending[transport_slot(1)] = Some(PendingRequest {
        id: 1,
        deadline: tokio::time::Instant::now() + Duration::from_secs(1),
        method: "order.status",
        mode: ResponseMode::Direct,
        parameters: Parameters::new(),
        clock_retried: false,
        reply,
    });

    assert_eq!(transport_slot(1), transport_slot(257));
    assert!(take_pending(&mut pending, 257).is_none());
    assert_eq!(pending[transport_slot(1)].as_ref().unwrap().id, 1);
    assert_eq!(take_pending(&mut pending, 1).unwrap().id, 1);
}

#[test]
fn request_deadline_is_assigned_synchronously_before_enqueue() {
    let (commands, mut receiver) = spsc_channel(1);
    let mut client = WsApiClient {
        commands,
        response_timeout: Duration::from_millis(250),
    };
    let before = tokio::time::Instant::now();
    let response = client.request_value("order.status", Parameters::new(), ResponseMode::Direct);
    let after = tokio::time::Instant::now();
    let command = receiver.try_pop().unwrap();

    assert!(command.deadline >= before + Duration::from_millis(250));
    assert!(command.deadline <= after + Duration::from_millis(250));
    drop(response);
}

#[test]
fn transport_request_ids_skip_zero_across_wraparound() {
    let mut next = u64::MAX;
    assert_eq!(next_transport_id(&mut next), u64::MAX);
    assert_eq!(next, 1);
    assert_eq!(next_transport_id(&mut next), 1);
    next = 0;
    assert_eq!(next_transport_id(&mut next), 1);
}

#[test]
fn full_pending_capacity_preserves_all_resident_requests() {
    let mut pending: PendingSlots = std::array::from_fn(|_| None);
    let mut responses = Vec::new();
    for id in 1..=TRANSPORT_ID_SLOTS as u64 {
        let (reply, response) = oneshot::channel();
        pending[transport_slot(id)] = Some(PendingRequest {
            id,
            deadline: tokio::time::Instant::now() + Duration::from_secs(1),
            method: "order.status",
            mode: ResponseMode::Direct,
            parameters: Parameters::new(),
            clock_retried: false,
            reply,
        });
        responses.push(response);
    }

    assert_eq!(pending_count(&pending), TRANSPORT_ID_SLOTS);
    assert!(pending.iter().all(Option::is_some));
    for (index, request) in pending.iter().enumerate() {
        let id = request.as_ref().unwrap().id;
        assert_eq!(transport_slot(id), index);
    }
    drop(responses);
}

#[test]
fn closed_and_expired_queued_commands_are_inactive() {
    let (closed_reply, closed_response) = oneshot::channel();
    drop(closed_response);
    let closed = Command {
        method: "order.status",
        parameters: Parameters::new(),
        mode: ResponseMode::Direct,
        clock_retried: false,
        deadline: tokio::time::Instant::now() + Duration::from_secs(1),
        reply: closed_reply,
    };
    assert!(command_is_inactive(&closed));

    let (expired_reply, expired_response) = oneshot::channel();
    let expired = Command {
        method: "order.status",
        parameters: Parameters::new(),
        mode: ResponseMode::Direct,
        clock_retried: false,
        deadline: tokio::time::Instant::now() - Duration::from_millis(1),
        reply: expired_reply,
    };
    assert!(command_is_inactive(&expired));
    finish_inactive(expired);
    assert_eq!(
        expired_response.blocking_recv().unwrap().unwrap_err().kind(),
        ExchangeErrorKind::Timeout
    );
}

#[test]
fn expired_response_is_rejected_before_processing_or_retry() {
    let mut pending: PendingSlots = std::array::from_fn(|_| None);
    let (reply, response) = oneshot::channel();
    pending[transport_slot(7)] = Some(PendingRequest {
        id: 7,
        deadline: tokio::time::Instant::now() - Duration::from_millis(1),
        method: "order.status",
        mode: ResponseMode::Direct,
        parameters: Parameters::new(),
        clock_retried: false,
        reply,
    });
    let request = take_pending(&mut pending, 7).unwrap();
    assert!(remaining(request.deadline).is_none());
    request.reply.send(Err(response_timeout_error())).unwrap();
    assert_eq!(
        response.blocking_recv().unwrap().unwrap_err().kind(),
        ExchangeErrorKind::Timeout
    );
}

#[test]
fn pong_write_timeout_is_bounded_by_earliest_request_deadline() {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(20);
    let timeout = write_timeout(Some(deadline), Duration::from_secs(1));
    assert!(timeout <= Duration::from_millis(20));
    assert_eq!(write_timeout(None, Duration::from_secs(1)), Duration::from_secs(1));
}

#[tokio::test]
async fn bounded_write_timeout_cannot_outlive_request_deadline() {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(20);
    let result = tokio::time::timeout(remaining(deadline).unwrap(), async {
        std::future::pending::<()>().await;
    })
    .await;
    assert!(result.is_err());
    assert!(remaining(deadline).is_none());
}

#[tokio::test]
async fn worker_deadline_cleanup_releases_only_expired_slots() {
    let now = tokio::time::Instant::now();
    let mut pending: PendingSlots = std::array::from_fn(|_| None);
    let (expired_reply, expired_response) = oneshot::channel();
    let (live_reply, mut live_response) = oneshot::channel();
    pending[transport_slot(1)] = Some(PendingRequest {
        id: 1,
        deadline: now,
        method: "order.status",
        mode: ResponseMode::Direct,
        parameters: Parameters::new(),
        clock_retried: false,
        reply: expired_reply,
    });
    pending[transport_slot(2)] = Some(PendingRequest {
        id: 2,
        deadline: now + Duration::from_secs(1),
        method: "order.status",
        mode: ResponseMode::Direct,
        parameters: Parameters::new(),
        clock_retried: false,
        reply: live_reply,
    });

    expire_pending(&mut pending, now);

    assert_eq!(
        expired_response.await.unwrap().unwrap_err().kind(),
        ExchangeErrorKind::Timeout
    );
    assert!(pending[transport_slot(1)].is_none());
    assert_eq!(pending[transport_slot(2)].as_ref().unwrap().id, 2);
    assert!(live_response.try_recv().is_err());
}

#[test]
fn connection_reset_drains_every_fixed_slot() {
    let mut pending: PendingSlots = std::array::from_fn(|_| None);
    let (first_reply, first_response) = oneshot::channel();
    let (second_reply, second_response) = oneshot::channel();
    for (id, reply) in [(1, first_reply), (256, second_reply)] {
        pending[transport_slot(id)] = Some(PendingRequest {
            id,
            deadline: tokio::time::Instant::now() + Duration::from_secs(1),
            method: "order.status",
            mode: ResponseMode::Direct,
            parameters: Parameters::new(),
            clock_retried: false,
            reply,
        });
    }

    fail_pending(
        &mut pending,
        ExchangeError::new(ExchangeErrorKind::StateConflict, "transport ID collision"),
    );

    assert_eq!(
        first_response.blocking_recv().unwrap().unwrap_err().kind(),
        ExchangeErrorKind::StateConflict
    );
    assert_eq!(
        second_response.blocking_recv().unwrap().unwrap_err().kind(),
        ExchangeErrorKind::StateConflict
    );
    assert!(pending.iter().all(Option::is_none));
}

#[tokio::test]
async fn worker_owns_response_timeout_and_reuses_connection_after_cleanup() {
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_address = http_listener.local_addr().unwrap();
    let http_server = tokio::spawn(serve_time_once(http_listener));

    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_address = ws_listener.local_addr().unwrap();
    let ws_server = tokio::spawn(async move {
        let (connection, _) = ws_listener.accept().await.unwrap();
        let mut socket = accept_async(connection).await.unwrap();
        let Message::Text(first) = socket.next().await.unwrap().unwrap() else {
            panic!("expected first request");
        };
        let first: Value = serde_json::from_str(first.as_ref()).unwrap();
        let Message::Text(second) = socket.next().await.unwrap().unwrap() else {
            panic!("expected second request");
        };
        let second: Value = serde_json::from_str(second.as_ref()).unwrap();
        assert_ne!(first["id"], second["id"]);
        socket
            .send(Message::Text(
                json!({
                    "id": second["id"],
                    "status": 200,
                    "result": order_result("NEW", "0.000")
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
    });

    let config = test_config(http_address, ws_address, Duration::from_millis(75));
    let credentials = test_credentials();
    let rest = RestClient::new(&config, credentials.clone()).unwrap();
    let mut network = NetworkRuntime::new(crate::network::NetworkRole::Trading, &config).unwrap();
    let mut client = WsApiClient::new(&config, credentials, rest, &mut network).unwrap();
    let symbol = Symbol::new("BTCUSDT").unwrap();
    let client_order_id = ClientOrderId::new(1).unwrap();

    let first = client.query_order(&symbol, &client_order_id).await.unwrap_err();
    assert_eq!(first.kind(), ExchangeErrorKind::Timeout);
    let second = client.query_order(&symbol, &client_order_id).await.unwrap();
    assert_eq!(second.status, "NEW");

    http_server.await.unwrap();
    ws_server.await.unwrap();
}

#[tokio::test]
async fn clock_retry_reserves_a_new_id_and_keeps_the_original_deadline() {
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_address = http_listener.local_addr().unwrap();
    let http_server = tokio::spawn(serve_time_connections(http_listener, 2));

    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_address = ws_listener.local_addr().unwrap();
    let ws_server = tokio::spawn(async move {
        let (connection, _) = ws_listener.accept().await.unwrap();
        let mut socket = accept_async(connection).await.unwrap();
        let Message::Text(first) = socket.next().await.unwrap().unwrap() else {
            panic!("expected initial request");
        };
        let first: Value = serde_json::from_str(first.as_ref()).unwrap();
        socket
            .send(Message::Text(
                json!({
                    "id": first["id"],
                    "status": 400,
                    "error": {"code": -1021, "msg": "timestamp outside recvWindow"}
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();

        let Message::Text(retry) = socket.next().await.unwrap().unwrap() else {
            panic!("expected clock retry");
        };
        let retry: Value = serde_json::from_str(retry.as_ref()).unwrap();
        assert_ne!(first["id"], retry["id"]);
        socket
            .send(Message::Text(
                json!({
                    "id": retry["id"],
                    "status": 200,
                    "result": order_result("NEW", "0.000")
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
    });

    let config = test_config(http_address, ws_address, Duration::from_secs(1));
    let credentials = test_credentials();
    let rest = RestClient::new(&config, credentials.clone()).unwrap();
    let mut network = NetworkRuntime::new(crate::network::NetworkRole::Trading, &config).unwrap();
    let mut client = WsApiClient::new(&config, credentials, rest, &mut network).unwrap();
    let symbol = Symbol::new("BTCUSDT").unwrap();
    let client_order_id = ClientOrderId::new(1).unwrap();

    assert_eq!(
        client
            .query_order(&symbol, &client_order_id)
            .await
            .unwrap()
            .status,
        "NEW"
    );

    http_server.await.unwrap();
    ws_server.await.unwrap();
}

#[tokio::test]
async fn connection_failure_drains_pending_then_next_command_reconnects() {
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_address = http_listener.local_addr().unwrap();
    let http_server = tokio::spawn(serve_time_once(http_listener));

    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_address = ws_listener.local_addr().unwrap();
    let ws_server = tokio::spawn(async move {
        let (first_connection, _) = ws_listener.accept().await.unwrap();
        let mut first_socket = accept_async(first_connection).await.unwrap();
        assert!(matches!(
            first_socket.next().await.unwrap().unwrap(),
            Message::Text(_)
        ));
        first_socket.close(None).await.unwrap();

        let (second_connection, _) = ws_listener.accept().await.unwrap();
        let mut second_socket = accept_async(second_connection).await.unwrap();
        let Message::Text(text) = second_socket.next().await.unwrap().unwrap() else {
            panic!("expected request after reconnect");
        };
        let request: Value = serde_json::from_str(text.as_ref()).unwrap();
        second_socket
            .send(Message::Text(
                json!({
                    "id": request["id"],
                    "status": 200,
                    "result": order_result("NEW", "0.000")
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
    });

    let config = test_config(http_address, ws_address, Duration::from_secs(1));
    let credentials = test_credentials();
    let rest = RestClient::new(&config, credentials.clone()).unwrap();
    let mut network = NetworkRuntime::new(crate::network::NetworkRole::Trading, &config).unwrap();
    let mut client = WsApiClient::new(&config, credentials, rest, &mut network).unwrap();
    let symbol = Symbol::new("BTCUSDT").unwrap();
    let client_order_id = ClientOrderId::new(1).unwrap();

    assert_eq!(
        client
            .query_order(&symbol, &client_order_id)
            .await
            .unwrap_err()
            .kind(),
        ExchangeErrorKind::Network
    );
    assert_eq!(
        client
            .query_order(&symbol, &client_order_id)
            .await
            .unwrap()
            .status,
        "NEW"
    );

    http_server.await.unwrap();
    ws_server.await.unwrap();
}

#[tokio::test]
async fn sends_all_order_mutations_over_one_signed_websocket_connection() {
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_address = http_listener.local_addr().unwrap();
    let http_server = tokio::spawn(async move {
        let (mut connection, _) = http_listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut chunk = [0_u8; 1024];
        loop {
            let count = connection.read(&mut chunk).await.unwrap();
            assert!(count > 0);
            request.extend_from_slice(&chunk[..count]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        assert!(
            String::from_utf8(request)
                .unwrap()
                .starts_with("GET /fapi/v1/time HTTP/1.1\r\n")
        );
        let body = r#"{"serverTime":1700000000000}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        connection.write_all(response.as_bytes()).await.unwrap();
    });

    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_address = ws_listener.local_addr().unwrap();
    let ws_server = tokio::spawn(async move {
        let (connection, _) = ws_listener.accept().await.unwrap();
        let mut socket = accept_async(connection).await.unwrap();
        for (index, method) in [
            "order.place",
            "order.cancel",
            "order.status",
            "order.status",
            "openOrders.cancelAll",
        ]
        .into_iter()
        .enumerate()
        {
            let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
                panic!("expected a text WebSocket API request");
            };
            let request: Value = serde_json::from_str(text.as_ref()).unwrap();
            assert_eq!(request["method"], method);
            let id = request["id"].as_u64().unwrap();
            assert_valid_test_signature(&request["params"]);

            if method == "order.cancel" {
                socket
                    .send(Message::Text(
                        json!({
                            "id": id,
                            "status": 400,
                            "error": {"code": -2011, "msg": "Unknown order sent."}
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .unwrap();
                continue;
            }
            let result = match (method, index) {
                ("order.place", _) => json!({
                    "symbol": "BTCUSDT",
                    "clientOrderId": "1",
                    "orderId": 42
                }),
                ("order.status", 2) => order_result("CANCELED", "0.000"),
                ("order.status", 3) => order_result("FILLED", "0.001"),
                ("openOrders.cancelAll", _) => json!({
                    "code": 200,
                    "msg": "The operation of cancel all open order is done."
                }),
                _ => unreachable!(),
            };
            socket
                .send(Message::Text(
                    json!({"id": id, "status": 200, "result": result})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
        }
    });

    let config = BinanceUsdmConfig::new(
        format!("http://{http_address}"),
        "ws://127.0.0.1:1",
        format!("ws://{ws_address}/ws-fapi/v1"),
        Duration::from_secs(5),
        Duration::from_secs(2),
        Duration::from_secs(60),
        Duration::from_secs(240),
    )
    .unwrap();
    let credentials = BinanceCredentials::new(
        SecretString::new("test-key".to_owned()),
        SecretString::new(crate::config::TEST_PRIVATE_KEY_PEM.to_owned()),
    )
    .unwrap();
    let rest = RestClient::new(&config, credentials.clone()).unwrap();
    let mut network = NetworkRuntime::new(crate::network::NetworkRole::Trading, &config).unwrap();
    let mut client = WsApiClient::new(&config, credentials, rest, &mut network).unwrap();
    let symbol = Symbol::new("BTCUSDT").unwrap();
    let client_order_id = ClientOrderId::new(1).unwrap();
    let intent = OrderIntent::post_only(
        symbol,
        client_order_id,
        Side::Buy,
        PriceTicks::new(640_001).unwrap(),
        QuantityLots::new(1).unwrap(),
    );

    let ack = client.place_order(&test_spec(), &intent).await.unwrap();
    assert_eq!(ack.order_id, 42);
    let canceled = client
        .cancel_order(&symbol, &client_order_id)
        .await
        .unwrap();
    assert_eq!(canceled.unwrap().status, "CANCELED");
    let queried = client.query_order(&symbol, &client_order_id).await.unwrap();
    assert_eq!(queried.status, "FILLED");
    client.cancel_all(&symbol).await.unwrap();

    http_server.await.unwrap();
    ws_server.await.unwrap();
}

async fn serve_time_once(listener: TcpListener) {
    serve_time_connections(listener, 1).await;
}

async fn serve_time_connections(listener: TcpListener, count: usize) {
    for _ in 0..count {
        serve_time_connection(&listener).await;
    }
}

async fn serve_time_connection(listener: &TcpListener) {
    let (mut connection, _) = listener.accept().await.unwrap();
    let mut request = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let count = connection.read(&mut chunk).await.unwrap();
        assert!(count > 0);
        request.extend_from_slice(&chunk[..count]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    assert!(
        String::from_utf8(request)
            .unwrap()
            .starts_with("GET /fapi/v1/time HTTP/1.1\r\n")
    );
    let body = r#"{"serverTime":1700000000000}"#;
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    connection.write_all(response.as_bytes()).await.unwrap();
}

fn test_config(
    http_address: std::net::SocketAddr,
    ws_address: std::net::SocketAddr,
    request_timeout: Duration,
) -> BinanceUsdmConfig {
    BinanceUsdmConfig::new(
        format!("http://{http_address}"),
        "ws://127.0.0.1:1",
        format!("ws://{ws_address}/ws-fapi/v1"),
        Duration::from_secs(5),
        request_timeout,
        Duration::from_secs(60),
        Duration::from_secs(240),
    )
    .unwrap()
}

fn test_credentials() -> BinanceCredentials {
    BinanceCredentials::new(
        SecretString::new("test-key".to_owned()),
        SecretString::new(crate::config::TEST_PRIVATE_KEY_PEM.to_owned()),
    )
    .unwrap()
}

fn order_result(status: &str, executed_quantity: &str) -> Value {
    json!({
        "symbol": "BTCUSDT",
        "clientOrderId": "1",
        "orderId": 42,
        "side": "BUY",
        "price": "64000.1",
        "origQty": "0.001",
        "executedQty": executed_quantity,
        "status": status
    })
}

fn assert_valid_test_signature(value: &Value) {
    let mut parameters: Parameters = value
        .as_object()
        .unwrap()
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    let signature = parameters
        .remove("signature")
        .unwrap()
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(parameters["apiKey"], "test-key");
    let payload = signature_payload(&parameters).unwrap();
    let credentials = BinanceCredentials::new(
        SecretString::new("test-key".to_owned()),
        SecretString::new(crate::config::TEST_PRIVATE_KEY_PEM.to_owned()),
    )
    .unwrap();
    assert_eq!(signature, sign_payload(&payload, credentials.signing_key()));
}
