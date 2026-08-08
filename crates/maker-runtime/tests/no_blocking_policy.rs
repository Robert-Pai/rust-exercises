use std::{fs, path::Path};

const COVERED_FILES: &[&str] = &[
    "crates/maker-engine/src/engine.rs",
    "crates/maker-ports/src/latest_bbo.rs",
    "crates/maker-ports/src/order_updates.rs",
    "crates/exchange-binance-usdm/src/adapter.rs",
    "crates/exchange-binance-usdm/src/network.rs",
    "crates/exchange-binance-usdm/src/rest.rs",
    "crates/exchange-binance-usdm/src/websocket.rs",
    "crates/exchange-binance-usdm/src/ws_api.rs",
    "apps/maker-cli/src/strategy_runtime.rs",
];

const FORBIDDEN: &[(&str, &str)] = &[
    ("Mutex", "lock-based mutex"),
    ("RwLock", "lock-based read/write lock"),
    ("Condvar", "blocking condition variable"),
    ("tokio::sync::mpsc", "multi-producer channel"),
    ("tokio::sync::watch", "watch channel"),
    ("tokio::sync::broadcast", "broadcast channel"),
    ("sync_channel", "blocking synchronous channel"),
    ("ArcSwap", "shared ArcSwap state"),
    ("std::thread::sleep", "blocking thread sleep"),
    ("println!", "direct standard-output write"),
    ("eprintln!", "direct standard-error write"),
    ("tracing::", "direct tracing dispatch"),
    ("debug!", "direct tracing formatting"),
    ("info!", "direct tracing formatting"),
    ("warn!", "direct tracing formatting"),
    ("error!", "direct tracing formatting"),
];

#[test]
fn covered_runtime_paths_reject_blocking_project_primitives() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap();
    let mut violations = Vec::new();
    for relative in COVERED_FILES {
        let source = fs::read_to_string(root.join(relative)).unwrap();
        for (needle, label) in FORBIDDEN {
            if source.contains(needle) {
                violations.push(format!("{relative}: {label} ({needle})"));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "covered runtime source-policy violations:\n{}",
        violations.join("\n")
    );
}
