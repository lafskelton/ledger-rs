use crate::{
    ledger::PAGE_HEADER_SZ,
    utils::{BitMask, FlagMask},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Portable, Serialize, seal::Seal};
use std::fmt;
use std::str::FromStr;
//
//
// ###### PAGE HEADER ######
//
//
#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Portable, CheckBytes)]
#[rkyv(compare(PartialEq))]
#[repr(C)]
pub struct PageHeader {
    pub page_type: u8,
    pub flags: BitMask<HeaderFlags>,
    pub _padding: [u8; 2],
    //
    slot_count_u32: [u8; 4], // num_rows
    //
    free_start_u32: [u8; 4], // End of slot array
    free_end_u32: [u8; 4],   // Start of cell heap
}

unsafe impl rkyv::traits::NoUndef for PageHeader {}

impl PageHeader {
    pub fn new(inital_flags: Option<Vec<HeaderFlags>>, page_size: u32) -> Self {
        let mut flags = BitMask::new();
        //
        if let Some(flagiter) = inital_flags {
            for f in flagiter {
                flags.set(&f, true);
            }
        }
        //
        Self {
            page_type: 0,
            flags,
            _padding: [0; 2],
            slot_count_u32: 0_u32.to_le_bytes(),
            free_start_u32: PAGE_HEADER_SZ.to_le_bytes(),
            free_end_u32: page_size.to_le_bytes(),
        }
    }
    //
    #[inline(always)]
    pub fn slot_count(&self) -> u32 {
        u32::from_le_bytes(self.slot_count_u32)
    }
    #[inline(always)]
    pub fn set_slot_count(&mut self, v: u32) {
        self.slot_count_u32 = v.to_le_bytes();
    }
    #[inline(always)]
    pub fn free_start(&self) -> u32 {
        u32::from_le_bytes(self.free_start_u32)
    }
    #[inline(always)]
    pub fn set_free_start(&mut self, v: u32) {
        self.free_start_u32 = v.to_le_bytes();
    }
    #[inline(always)]
    pub fn free_end(&self) -> u32 {
        u32::from_le_bytes(self.free_end_u32)
    }
    #[inline(always)]
    pub fn set_free_end(&mut self, v: u32) {
        self.free_end_u32 = v.to_le_bytes();
    }
    //
    #[inline(always)]
    pub fn serialize(&self) -> Result<rkyv::util::AlignedVec, rkyv::rancor::Error> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self)
    }
    #[inline(always)]
    pub fn deserialize(f: &rkyv::util::AlignedVec) -> Result<Self, rkyv::rancor::Error> {
        rkyv::from_bytes::<Self, rkyv::rancor::Error>(f)
    }
    #[inline(always)]
    pub fn access(f: &[u8]) -> Result<&Self, rkyv::rancor::Error> {
        rkyv::access::<Self, rkyv::rancor::Error>(f)
    }
    #[inline(always)]
    pub fn access_mut<'a>(f: &'a mut [u8]) -> Result<Seal<'a, Self>, rkyv::rancor::Error> {
        rkyv::access_mut::<Self, rkyv::rancor::Error>(f)
    }
    //
    #[inline(always)]
    pub unsafe fn access_unchecked(f: &[u8]) -> &Self {
        unsafe { rkyv::access_unchecked::<Self>(f) }
    }
    #[inline(always)]
    pub unsafe fn access_unchecked_mut<'a>(f: &'a mut [u8]) -> Seal<'a, Self> {
        unsafe { rkyv::access_unchecked_mut::<Self>(f) }
    }
    //
    #[inline(always)]
    pub fn free_space(&self) -> u32 {
        self.free_end() - self.free_start()
    }
    //
    #[inline(always)]
    pub unsafe fn free_space_unchecked(&self) -> u32 {
        unsafe {
            let end = (self.free_end_u32.as_ptr() as *const u32).read_unaligned();
            let start = (self.free_start_u32.as_ptr() as *const u32).read_unaligned();
            end.saturating_sub(start).min(32_768)
        }
    }
}
//
#[derive(Archive, Deserialize, Clone, CheckBytes, Serialize, Debug, PartialEq, Portable)]
#[rkyv(compare(PartialEq), derive(Debug))]
#[repr(u8)]
pub enum HeaderFlags {
    Private,
    DeleteMe,
}
impl FlagMask for HeaderFlags {
    fn mask(&self) -> u8 {
        match self {
            HeaderFlags::Private => 1 << 0,
            HeaderFlags::DeleteMe => 1 << 1,
        }
    }
}
//
//
// ###### LEDGER HEADER ######
//
//
#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Portable, CheckBytes)]
#[rkyv(compare(PartialEq))]
#[repr(C)]
pub struct LedgerHeader {
    pub flags: BitMask<HeaderFlags>,
    //
    ledger_name: LedgerName,
    ledger_description: LedgerDescription,
    //
    num_pages_u32: [u8; 4],
    //
    rows_per_page_u32: [u8; 4],
    //
    page_cursor_u32: [u8; 4],
    //
    pub _padding: [u8; 14],
}

unsafe impl rkyv::traits::NoUndef for LedgerHeader {}

impl LedgerHeader {
    pub fn new(
        ledger_name: LedgerName,
        ledger_description: LedgerDescription,
        inital_flags: Option<Vec<HeaderFlags>>,
    ) -> Self {
        let mut flags = BitMask::new();
        //
        if let Some(flagiter) = inital_flags {
            for f in flagiter {
                flags.set(&f, true);
            }
        }
        //
        Self {
            flags,
            //
            ledger_name,
            ledger_description,
            //
            rows_per_page_u32: 0_u32.to_le_bytes(),
            //
            num_pages_u32: 0_u32.to_le_bytes(),
            //
            page_cursor_u32: 0_u32.to_le_bytes(),
            //
            _padding: [0; 14],
        }
    }

    #[inline(always)]
    pub fn name(&self) -> String {
        self.ledger_name.to_string()
    }
    //
    #[inline(always)]
    pub fn rows_per_page(&self) -> u32 {
        u32::from_le_bytes(self.rows_per_page_u32)
    }
    #[inline(always)]
    pub fn set_rows_per_page(&mut self, v: u32) {
        self.rows_per_page_u32 = v.to_le_bytes();
    }
    //
    #[inline(always)]
    pub fn num_pages(&self) -> u32 {
        u32::from_le_bytes(self.num_pages_u32)
    }
    #[inline(always)]
    pub fn set_num_pages(&mut self, v: u32) {
        self.num_pages_u32 = v.to_le_bytes();
    }
    //
    #[inline(always)]
    pub fn page_cursor(&self) -> u32 {
        u32::from_le_bytes(self.page_cursor_u32)
    }
    #[inline(always)]
    pub fn set_page_cursor(&mut self, v: u32) {
        self.page_cursor_u32 = v.to_le_bytes();
    }
    //
    #[inline(always)]
    pub fn inc_page_cursor(&mut self) {
        unsafe {
            let ptr = self.page_cursor_u32.as_mut_ptr() as *mut u32;
            let val = ptr.read_unaligned();
            ptr.write_unaligned(val.wrapping_add(1));
        }
    }
    #[inline(always)]
    pub fn dec_page_cursor(&mut self) {
        unsafe {
            let ptr = self.page_cursor_u32.as_mut_ptr() as *mut u32;
            let val = ptr.read_unaligned();
            ptr.write_unaligned(val.wrapping_sub(1));
        }
    }
    //
    #[inline(always)]
    pub fn num_rows(&self, rows_per_page: u32) -> u32 {
        ((self.num_pages()) * rows_per_page) + self.page_cursor()
    }
    //
    #[inline(always)]
    pub fn serialize(&self) -> Result<rkyv::util::AlignedVec, rkyv::rancor::Error> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self)
    }
    #[inline(always)]
    pub fn deserialize(f: &[u8]) -> Result<Self, rkyv::rancor::Error> {
        rkyv::from_bytes::<Self, rkyv::rancor::Error>(f)
    }
    #[inline(always)]
    pub fn access(f: &[u8]) -> Result<&Self, rkyv::rancor::Error> {
        rkyv::access::<Self, rkyv::rancor::Error>(f)
    }
    #[inline(always)]
    pub fn access_mut<'a>(f: &'a mut [u8]) -> Result<Seal<'a, Self>, rkyv::rancor::Error> {
        rkyv::access_mut::<Self, rkyv::rancor::Error>(f)
    }
    #[inline(always)]
    pub unsafe fn access_unchecked(f: &[u8]) -> &Self {
        unsafe { rkyv::access_unchecked::<Self>(f) }
    }
    #[inline(always)]
    pub unsafe fn access_unchecked_mut<'a>(f: &'a mut [u8]) -> Seal<'a, Self> {
        unsafe { rkyv::access_unchecked_mut::<Self>(f) }
    }
    //
}
//
//
// ###### LEDGER DEFINITIONS ######
//
// ### LEDGER NAME
//
#[derive(
    Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Portable, CheckBytes, Eq, Hash,
)]
#[rkyv(compare(PartialEq))]
#[repr(C)]
pub struct LedgerName([u8; 9]);

#[derive(Debug)]
pub enum LedgerNameError {
    InvalidBase64,
    InvalidLength(usize),
}

impl std::fmt::Display for LedgerNameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBase64 => write!(f, "String is not valid URL-safe Base64"),
            Self::InvalidLength(len) => write!(f, "Decoded data is {} bytes, expected 9", len),
        }
    }
}

impl std::error::Error for LedgerNameError {}

impl LedgerName {
    pub const BLANK: Self = Self([0; 9]);

    // pub fn generate() -> Self {
    //     let mut rng = rand::rng();
    //     let random_bytes: [u8; 9] = rng.random();
    //     LedgerName::new(random_bytes)
    // }

    pub fn new(data: [u8; 9]) -> Self {
        Self(data)
    }

    pub fn from_string(s: &str) -> Result<Self, LedgerNameError> {
        s.parse()
    }

    pub fn to_string(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.0)
    }

    pub fn as_bytes(&self) -> &[u8; 9] {
        &self.0
    }
}

impl fmt::Display for LedgerName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl FromStr for LedgerName {
    type Err = LedgerNameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let decoded = URL_SAFE_NO_PAD
            .decode(s)
            .map_err(|_| LedgerNameError::InvalidBase64)?;

        let bytes: [u8; 9] = decoded
            .try_into()
            .map_err(|v: Vec<u8>| LedgerNameError::InvalidLength(v.len()))?;

        Ok(Self(bytes))
    }
}

impl From<[u8; 9]> for LedgerName {
    fn from(bytes: [u8; 9]) -> Self {
        Self(bytes)
    }
}

impl From<LedgerName> for [u8; 9] {
    fn from(name: LedgerName) -> [u8; 9] {
        name.0
    }
}

impl From<&str> for LedgerName {
    fn from(input: &str) -> Self {
        let mut name: LedgerName = Self([b' '; 9]);
        //
        let input_bytes = input.as_bytes();
        let len = input_bytes.len().min(9);
        //
        name.0[..len].copy_from_slice(&input_bytes[..len]);
        //
        name
    }
}

//
// ### LEDGER DESCRIPTION
//

#[derive(
    Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Portable, CheckBytes, Eq, Hash,
)]
#[rkyv(compare(PartialEq))]
#[repr(C)]
pub struct LedgerDescription([u8; 32]);

#[derive(Debug)]
pub enum LedgerDescriptionError {
    InvalidBase64,
    InvalidLength(usize),
}

impl std::fmt::Display for LedgerDescriptionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBase64 => write!(f, "string not valid URL-safe Base64"),
            Self::InvalidLength(len) => write!(f, "decoded data is {} bytes, wants 32", len),
        }
    }
}

impl std::error::Error for LedgerDescriptionError {}

impl LedgerDescription {
    pub const BLANK: Self = Self([0; 32]);

    // pub fn generate() -> Self {
    //     Self(rand::rng().random())
    // }

    pub fn new(input: &str) -> Self {
        let mut desc = Self([b' '; 32]);
        //
        let input_bytes = input.as_bytes();
        let len = input_bytes.len().min(32);
        //
        desc.0[..len].copy_from_slice(&input_bytes[..len]);
        //
        desc
    }

    pub fn to_string(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.0)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for LedgerDescription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl FromStr for LedgerDescription {
    type Err = LedgerDescriptionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let decoded = URL_SAFE_NO_PAD
            .decode(s)
            .map_err(|_| LedgerDescriptionError::InvalidBase64)?;

        let bytes: [u8; 32] = decoded
            .try_into()
            .map_err(|v: Vec<u8>| LedgerDescriptionError::InvalidLength(v.len()))?;

        Ok(Self(bytes))
    }
}

impl From<[u8; 32]> for LedgerDescription {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl From<LedgerDescription> for [u8; 32] {
    fn from(name: LedgerDescription) -> [u8; 32] {
        name.0
    }
}

impl TryFrom<&str> for LedgerDescription {
    type Error = LedgerDescriptionError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}
