use base64::{Engine, engine::general_purpose::STANDARD};
use ed25519_dalek::{Signer, SigningKey};

pub(crate) fn encode_query(parameters: &[(String, String)]) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    serializer.extend_pairs(parameters.iter().map(|(key, value)| (key, value)));
    serializer.finish()
}

pub(crate) fn sign_payload(payload: &str, signing_key: &SigningKey) -> String {
    STANDARD.encode(signing_key.sign(payload.as_bytes()).to_bytes())
}

pub(crate) fn build_signed_query(
    mut parameters: Vec<(String, String)>,
    recv_window_ms: u64,
    timestamp_ms: u64,
    signing_key: &SigningKey,
) -> String {
    parameters.push(("recvWindow".to_owned(), recv_window_ms.to_string()));
    parameters.push(("timestamp".to_owned(), timestamp_ms.to_string()));
    let unsigned_query = encode_query(&parameters);
    let signature = sign_payload(&unsigned_query, signing_key);
    parameters.push(("signature".to_owned(), signature));
    encode_query(&parameters)
}

#[cfg(test)]
mod tests {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use ed25519_dalek::{Signature, Verifier};
    use secrecy::SecretString;

    use super::*;
    use crate::config::{BinanceCredentials, TEST_PRIVATE_KEY_PEM};

    fn credentials() -> BinanceCredentials {
        BinanceCredentials::new(
            SecretString::new("test-key".to_owned()),
            SecretString::new(TEST_PRIVATE_KEY_PEM.to_owned()),
        )
        .unwrap()
    }

    #[test]
    fn percent_encodes_and_preserves_parameter_order() {
        let query = encode_query(&[
            ("symbol".to_owned(), "BTC/USDT".to_owned()),
            ("client".to_owned(), "maker 1".to_owned()),
        ]);

        assert_eq!(query, "symbol=BTC%2FUSDT&client=maker+1");
    }

    #[test]
    fn produces_verifiable_ed25519_base64_signature() {
        let query = "symbol=BTCUSDT&recvWindow=5000&timestamp=1700000000000";
        let credentials = credentials();
        let encoded = sign_payload(query, credentials.signing_key());
        let bytes = STANDARD.decode(encoded).unwrap();
        let signature = Signature::from_slice(&bytes).unwrap();

        credentials
            .signing_key()
            .verifying_key()
            .verify(query.as_bytes(), &signature)
            .unwrap();
    }

    #[test]
    fn appends_recv_window_timestamp_and_signature() {
        let credentials = credentials();
        let signed = build_signed_query(
            vec![("symbol".to_owned(), "BTCUSDT".to_owned())],
            5_000,
            1_700_000_000_000,
            credentials.signing_key(),
        );

        assert!(
            signed.starts_with("symbol=BTCUSDT&recvWindow=5000&timestamp=1700000000000&signature=")
        );
        assert_eq!(signed.matches("signature=").count(), 1);
        assert!(signed.contains("%3D"), "Base64 padding must be URL encoded");
    }
}
