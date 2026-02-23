use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Portable, Serialize};
use std::{array::TryFromSliceError, num::TryFromIntError};

use crate::header::LedgerNameError;

//
pub trait FlagMask {
    fn mask(&self) -> u8;
}
//
#[derive(
    Archive,
    Serialize,
    CheckBytes,
    Deserialize,
    Portable,
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
)]
#[rkyv(compare(PartialEq))]
//
//
#[repr(transparent)]
pub struct BitMask<T>(u8, std::marker::PhantomData<T>);
//
//
impl<T: FlagMask> BitMask<T> {
    pub fn new() -> Self {
        Self(0, std::marker::PhantomData)
    }

    pub fn set(&mut self, flag: &T, on: bool) {
        let m = flag.mask();
        if on {
            self.0 |= m;
        } else {
            self.0 &= !m;
        }
    }

    pub fn is_set(&self, flag: T) -> bool {
        (self.0 & flag.mask()) != 0
    }
}
//
impl<T: FlagMask> ArchivedBitMask<T> {
    pub fn new() -> Self {
        Self(0, std::marker::PhantomData)
    }

    pub fn set(&mut self, flag: &T, on: bool) {
        let m = flag.mask();
        if on {
            self.0 |= m;
        } else {
            self.0 &= !m;
        }
    }

    pub fn is_set(&self, flag: T) -> bool {
        (self.0 & flag.mask()) != 0
    }
}

//
// ####### ERRORS
//

#[derive(thiserror::Error, Debug)]
pub enum DatastoreError {
    //
    #[error("{0}")]
    Error(String),
    //
    #[error("rkyv err: {0}")]
    RkyvError(#[from] rkyv::rancor::Error),
    //
    #[error("PageError: {0}")]
    PageError(#[from] PageError),
    //
    #[error("io err:{0}")]
    StdIoError(#[from] std::io::Error),
    //
    #[error("scan data err:{0}")]
    ScanDataDirError(String),
    //
    #[error("LedgerNameError: {0}")]
    LedgerNameError(#[from] LedgerNameError),
}

#[derive(thiserror::Error, Debug)]
pub enum PageError {
    //
    #[error("{0}")]
    Error(String),
    //
    #[error("rkyv err: {0}")]
    RkyvError(#[from] rkyv::rancor::Error),
    //
    #[error("PageIdOutOfBounds")]
    PageIdOutOfBounds,
    //
    #[error("RowIdOutOfBounds")]
    RowIdOutOfBounds,
    //
    #[error("RowNotFound")]
    RowNotFound,
    //
    #[error("NoSpace")]
    NoSpace,
    //
    #[error("TryFromSliceError: {0}")]
    StdTryFromSliceError(#[from] TryFromSliceError),
    //
    #[error("TryFromIntError: {0}")]
    StdTryFromIntError(#[from] TryFromIntError),
    //
    #[error("LedgerNameError: {0}")]
    LedgerNameError(#[from] LedgerNameError),
}

impl From<String> for DatastoreError {
    fn from(value: String) -> Self {
        tracing::error!("{value}");
        Self::Error(value)
    }
}
impl From<&str> for DatastoreError {
    fn from(value: &str) -> Self {
        tracing::error!("{value}");
        Self::Error(value.to_string())
    }
}
