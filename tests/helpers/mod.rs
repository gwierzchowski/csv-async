#[cfg(not(feature = "tokio"))]
mod helpers_async_std;
#[cfg(feature = "tokio")]
mod helpers_tokio;

#[cfg(not(feature = "tokio"))]
pub use helpers_async_std::*;
#[cfg(feature = "tokio")]
pub use helpers_tokio::*;

#[allow(dead_code)]
pub fn custom_error_message(err: &csv_async::Error) -> String {
    match err.kind() {
        csv_async::ErrorKind::Io(e) => {
            format!("IO Error: {e}")
        },
        csv_async::ErrorKind::Seek => {
            String::from("Seek error")
        },
        csv_async::ErrorKind::UnequalLengths { pos, expected_len, len } => {
            format!("Unequal lengths: position = {pos:?}, expected_len = {expected_len}, len = {len}")
        },
        csv_async::ErrorKind::Utf8 { pos, err } => {
            format!("Invalid UTF8: position = {pos:?}, err = {err}")
        },
        #[cfg(feature = "with_serde")]
        csv_async::ErrorKind::Serialize(msg) => {
            format!("Serialize error: {msg}")
        },
        #[cfg(feature = "with_serde")]
        csv_async::ErrorKind::Deserialize { pos, err } => {
            let field = err.field();
            let msg = match err.kind() {
                csv_async::DeserializeErrorKind::InvalidUtf8(e) => {
                    format!("Invalid UTF8: {e}")
                },
                csv_async::DeserializeErrorKind::Message(msg) => msg.clone(),
                csv_async::DeserializeErrorKind::ParseBool(e) => {
                    format!("Error parsing boolean: {e}")
                }
                csv_async::DeserializeErrorKind::ParseFloat(e) => {
                    format!("Error parsing float: {e}")
                }
                csv_async::DeserializeErrorKind::ParseInt(e) => {
                    format!("Error parsing integer: {e}")
                }
                csv_async::DeserializeErrorKind::UnexpectedEndOfRow => {
                    String::from("Row has too few fields")
                }
                csv_async::DeserializeErrorKind::Unsupported(e) => {
                    format!("Unsupported type: {e}")
                }
            };
            format!("Deserialize error: position = {pos:?}, field = {field:?}: {msg}")
        },
        _ => String::from("Other error")
    }
}
