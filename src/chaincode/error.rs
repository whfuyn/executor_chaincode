use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChaincodeError {
    #[error("this error haven't implemented")]
    Unimplemented,
}