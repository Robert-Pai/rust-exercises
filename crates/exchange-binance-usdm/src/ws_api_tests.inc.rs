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
