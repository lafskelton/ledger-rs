// // #### UTILS
use crate::header::PageHeader;
use crate::ledger::PAGE_HEADER_SZ;
use crate::utils::PageError;
use bytecheck::CheckBytes;
use rkyv::api::high::HighValidator;
use rkyv::traits::NoUndef;
use rkyv::{Archive, Deserialize, Portable, Serialize, access_unchecked, seal::Seal};
use rkyv::{access, access_mut, access_unchecked_mut};
use std::marker::PhantomData;
//
#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Portable, CheckBytes)]
#[rkyv(compare(PartialEq))]
//
#[repr(C)]
pub struct SlottedPage<T, const PAGE_SZ: usize, const ROWS_PER_PAGE: usize> {
    pub data: [u8; PAGE_SZ],
    phantom: PhantomData<T>,
}

#[inline(always)]
pub const fn page_sz<T>() -> u32
where
    T: PageSchema,
{
    PAGE_HEADER_SZ + (size_of::<T>() as u32 * T::ROWS_PER_PAGE as u32)
}
//
pub trait PageSchema {
    const ROWS_PER_PAGE: usize;
    const PAGE_SZ: usize;
    //
    fn to_bytes(&self) -> Result<rkyv::util::AlignedVec, rkyv::rancor::Error>;
    fn from_bytes(f: &rkyv::util::AlignedVec) -> Result<Self, rkyv::rancor::Error>
    where
        Self: Sized;

    fn deleted_row(page_row_n: usize) -> &'static [u8];
}
//
unsafe impl<T, const PAGE_SZ: usize, const ROWS_PER_PAGE: usize> NoUndef
    for SlottedPage<T, PAGE_SZ, ROWS_PER_PAGE>
{
}
//
impl<T, const PAGE_SZ: usize, const ROWS_PER_PAGE: usize> SlottedPage<T, PAGE_SZ, ROWS_PER_PAGE>
where
    T: Archive
        + Portable
        + PageSchema
        + for<'b> bytecheck::CheckBytes<
            bytecheck::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'b>,
                    rkyv::validation::shared::SharedValidator,
                >,
                bytecheck::rancor::Error,
            >,
        >,
{
    const ROW_SZ: u32 = size_of::<T>() as u32;
    //
    pub fn new() -> Result<Self, PageError> {
        //
        let mut data = [0_u8; PAGE_SZ];
        //
        let header: PageHeader = PageHeader::new(None, PAGE_SZ as u32);
        //
        let header_bytes = header.serialize()?;
        data[0..PAGE_HEADER_SZ as usize].copy_from_slice(&header_bytes);
        //
        //
        Ok(Self {
            data,
            phantom: PhantomData,
        })
    }

    // #### HEADER

    #[inline(always)]
    pub fn access_header<'a>(&'a self) -> Result<&'a PageHeader, PageError> {
        Ok(PageHeader::access(&self.data[0..PAGE_HEADER_SZ as usize])?)
    }
    #[inline(always)]
    pub unsafe fn access_header_unchecked<'a>(&'a self) -> &'a PageHeader {
        unsafe { PageHeader::access_unchecked(&self.data[0..PAGE_HEADER_SZ as usize]) }
    }
    #[inline(always)]
    fn access_header_mut<'a>(&'a mut self) -> Result<Seal<'a, PageHeader>, PageError> {
        Ok(PageHeader::access_mut(
            &mut self.data[0..PAGE_HEADER_SZ as usize],
        )?)
    }
    #[inline(always)]
    pub unsafe fn access_header_unchecked_mut<'a>(&'a mut self) -> Seal<'a, PageHeader> {
        unsafe { PageHeader::access_unchecked_mut(&mut self.data[0..PAGE_HEADER_SZ as usize]) }
    }

    // #### ROWS
    pub fn insert_row(&mut self, object: &T) -> Result<u32, PageError> {
        //
        let mut header = self.access_header_mut()?;
        //
        if header.free_space() <= Self::ROW_SZ {
            return Err(PageError::NoSpace);
        }
        //
        let aligned_start = header.free_end() - Self::ROW_SZ;
        //
        if aligned_start < header.free_start() + 4 {
            return Err(PageError::NoSpace);
        }
        //
        let slot_count = header.slot_count();
        let slot_id: usize = _get_slot_id(slot_count as usize);
        header.set_free_start((slot_id + 4) as u32);
        header.set_free_end(aligned_start);
        header.set_slot_count(slot_count + 1);
        //
        self.data[slot_id..slot_id + 4].copy_from_slice(&aligned_start.to_le_bytes());
        //
        self.data[aligned_start as usize..(aligned_start + Self::ROW_SZ) as usize]
            .copy_from_slice(&object.to_bytes()?);
        //
        Ok(slot_count)
    }
    //
    pub unsafe fn insert_row_unchecked(&mut self, object: &T) -> u32 {
        //
        let mut header = unsafe { self.access_header_unchecked_mut() };
        //
        if header.free_space() <= Self::ROW_SZ {
            panic!("maybe don't use unchecked :)");
        }
        //
        let aligned_start = header.free_end() - Self::ROW_SZ;
        //
        if aligned_start < header.free_start() + 4 {
            panic!("maybe don't use unchecked :)");
        }
        //
        let slot_count = header.slot_count();
        let slot_id: usize = _get_slot_id(slot_count as usize);
        header.set_free_start((slot_id + 4) as u32);
        header.set_free_end(aligned_start);
        header.set_slot_count(slot_count + 1);
        //
        self.data[slot_id..slot_id + 4].copy_from_slice(&aligned_start.to_le_bytes());
        //
        self.data[aligned_start as usize..(aligned_start + Self::ROW_SZ) as usize]
            .copy_from_slice(&object.to_bytes().unwrap());
        //
        slot_count
    }
    //
    pub unsafe fn set_row_deleted(&mut self, page_row_n: usize) -> Result<(), PageError> {
        //
        let header = unsafe { self.access_header_unchecked() };
        let Some(data_offset) = _offset(page_row_n, header.slot_count() as usize, &self.data)
        else {
            return Err(PageError::RowIdOutOfBounds);
        };
        //
        self.data[data_offset..data_offset + Self::ROW_SZ as usize]
            .copy_from_slice(&T::deleted_row(page_row_n));
        //
        Ok(())
    }
    //
    //
    pub fn access_row<'a>(&'a self, page_row_n: usize) -> Result<Option<&'a T>, PageError>
    where
        T: Archive + Portable + PageSchema,
    {
        //
        let header = self.access_header()?;
        // let data_offset = unsafe { _offset_unchecked(page_row_n, &self.data) };
        let Some(data_offset) = _offset(page_row_n, header.slot_count() as usize, &self.data)
        else {
            return Err(PageError::RowNotFound);
        };
        //
        let sz = size_of::<T>();
        Ok(Some(access::<T, rkyv::rancor::Error>(
            &self.data[data_offset..data_offset + sz],
        )?))
    }
    //
    #[inline(always)]
    pub unsafe fn access_row_unchecked<'a>(&'a self, page_row_n: usize) -> &'a T {
        //
        let data_offset = unsafe { _offset_unchecked(page_row_n, &self.data) };
        //
        unsafe { access_unchecked::<T>(&self.data[data_offset..data_offset + size_of::<T>()]) }
    }
    //
    pub fn access_row_mut<'a>(
        &'a mut self,
        page_row_n: usize,
    ) -> Result<Option<Seal<'a, T>>, PageError>
    where
        T: for<'b> CheckBytes<HighValidator<'b, rkyv::rancor::Error>>,
    {
        //
        let header = self.access_header()?;
        let Some(data_offset) = _offset(page_row_n, header.slot_count() as usize, &self.data)
        else {
            return Err(PageError::RowNotFound);
        };
        //
        let sz = size_of::<T>();
        Ok(Some(access_mut::<T, rkyv::rancor::Error>(
            &mut self.data[data_offset..data_offset + sz],
        )?))
    }
    //
    #[inline(always)]
    pub unsafe fn access_row_unchecked_mut<'a>(&'a mut self, page_row_n: usize) -> Seal<'a, T> {
        //
        let data_offset = unsafe { _offset_unchecked(page_row_n, &self.data) };
        //
        unsafe {
            access_unchecked_mut::<T>(&mut self.data[data_offset..data_offset + size_of::<T>()])
        }
    }

    #[inline(always)]
    pub fn free_space(&self) -> Result<u32, PageError> {
        Ok(self.access_header()?.free_space())
    }

    #[inline(always)]
    pub unsafe fn free_space_unchecked(&self) -> u32 {
        unsafe { self.access_header_unchecked().free_space_unchecked() }
    }

    #[inline(always)]
    pub const fn row_sz() -> usize {
        size_of::<T>() + 4
    }
    #[inline(always)]
    pub const fn max_rows_per_page() -> usize {
        (PAGE_SZ as u32 - PAGE_HEADER_SZ) as usize / (size_of::<T>() + 4)
    }
}

// #### UTILS

#[inline(always)]
unsafe fn _offset_unchecked(page_row_n: usize, data: &[u8]) -> usize {
    unsafe {
        let slot_offset = _get_slot_id(page_row_n);
        let ptr = data.as_ptr().add(slot_offset) as *const u32;
        ptr.read_unaligned() as usize
    }
}
//
#[inline(always)]
fn _offset(page_row_n: usize, num_rows: usize, data: &[u8]) -> Option<usize> {
    if page_row_n >= num_rows {
        tracing::error!("row out of bounds");
        return None;
    }
    //
    let slot_offset = _get_slot_id(page_row_n);
    //
    if slot_offset + 4 > data.len() {
        tracing::error!("data slice too small for slot reading");
        return None;
    }
    //
    let bytes: [u8; 4] = data[slot_offset..slot_offset + 4]
        .try_into()
        .expect("slice with incorrect length");
    //
    Some(u32::from_ne_bytes(bytes) as usize)
}
//
#[inline(always)]
const fn _get_slot_id(row_n: usize) -> usize {
    PAGE_HEADER_SZ as usize + (row_n as usize * 4)
}
