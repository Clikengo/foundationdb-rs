pub mod directory;
pub mod node;
pub mod directory_subspace;

pub use directory::*;
use std::io;
use std::fmt::{self, Display};
use crate::error;


#[derive(Debug)]
pub enum DirectoryError {
    CannotOpenRoot,
    LayerMismatch,
    NotExist,
    Message(String),
    Version(String),
    IoError(io::Error),
    FdbError(error::FdbError)
}

impl From<io::Error> for DirectoryError {
    fn from(err: io::Error) -> Self {
        DirectoryError::IoError(err)
    }
}

impl From<error::FdbError> for DirectoryError {
    fn from(err: error::FdbError) -> Self {
        DirectoryError::FdbError(err)
    }
}

impl Display for DirectoryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DirectoryError::CannotOpenRoot => write!(f, "Cannot open root directory"),
            DirectoryError::LayerMismatch => write!(f, "Layer mismatch"),
            DirectoryError::NotExist => write!(f, "Directory does not exist"),
            DirectoryError::Version(s) => s.fmt(f),
            DirectoryError::Message(s) => s.fmt(f),
            DirectoryError::IoError(err) => err.fmt(f),
            DirectoryError::FdbError(err) => err.fmt(f),
        }
    }
}


