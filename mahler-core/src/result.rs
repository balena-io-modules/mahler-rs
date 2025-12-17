//! Alias over [`core::result::Result`]

use crate::error::Error;

pub type Result<T> = std::result::Result<T, Error>;
