use jsonrpsee::core::Error;
use jsonrpsee::types::error::{CallError, ErrorCode, ErrorObject};

#[allow(dead_code)]
pub fn not_supported() -> Error {
    Error::Call(CallError::Custom(ErrorObject::borrowed(
        ErrorCode::MethodNotFound.code(),
        &"Not supported",
        None,
    )))
}
