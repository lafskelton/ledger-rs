use crate::header::{LedgerDescription, LedgerHeader, LedgerName, PageHeader};
use crate::page::{PageSchema, SlottedPage, page_sz};
use crate::utils::{DatastoreError, PageError};
use memmap2::MmapMut;
use rkyv::traits::NoUndef;
use rkyv::{Archive, Portable, access_unchecked, access_unchecked_mut};
use rkyv::{access, access_mut, seal::Seal};
use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::path::Path;

//
pub const PAGE_HEADER_SZ: u32 = size_of::<PageHeader>() as u32;
//
// DATA LEDGER
//
pub struct DataLedgerStore<T, const PAGESZ: usize, const ROWS_PER_PAGE: usize> {
    //
    file: File,
    mmap: MmapMut,
    //
    phantom: PhantomData<T>,
}
//

//
impl<T, const PAGESZ: usize, const ROWS_PER_PAGE: usize> DataLedgerStore<T, PAGESZ, ROWS_PER_PAGE>
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
    SlottedPage<T, PAGESZ, ROWS_PER_PAGE>: NoUndef + std::marker::Unpin,
{
    const PAGE_SIZE: u32 = page_sz::<T>();
    pub const LEDGER_HEADER_SZ: u32 = size_of::<LedgerHeader>() as u32;
    //
    pub fn open<P>(
        folder_path: P,
        ledger_name: LedgerName,
        ledger_description: LedgerDescription,
    ) -> Result<Self, DatastoreError>
    where
        P: AsRef<Path>,
    {
        let ledger_path = folder_path.as_ref().join(ledger_name.to_string());
        //
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&ledger_path)?;
        //
        let new_file = file.metadata()?.len() == 0;
        if new_file {
            tracing::debug!("creating new ledger: {}", ledger_name);
            // new empty file
            file.set_len((Self::PAGE_SIZE + Self::LEDGER_HEADER_SZ) as u64)?;
        }
        //
        let mut mmap = unsafe { MmapMut::map_mut(&file)? }; // maps the file to virtual memory and makes an ASSUMPTION that the OS or any other process will not mutate this file while it mapped!
        //
        let mut ledger_header: LedgerHeader;
        if new_file {
            ledger_header = LedgerHeader::new(ledger_name, ledger_description, None);
            ledger_header.set_num_pages(1);
            //
            let ledger_header_bytes = ledger_header.serialize()?;
            //
            mmap[0..Self::LEDGER_HEADER_SZ as usize].copy_from_slice(&ledger_header_bytes);
            //
            let page = SlottedPage::<T, PAGESZ, ROWS_PER_PAGE>::new()?;
            mmap[Self::LEDGER_HEADER_SZ as usize
                ..(Self::LEDGER_HEADER_SZ + Self::PAGE_SIZE) as usize]
                .copy_from_slice(page.data.as_slice());
            //
            //
        } else {
            let ledger_header = LedgerHeader::access(&mmap[0..Self::LEDGER_HEADER_SZ as usize])?;
            tracing::debug!(
                "opening existing ledger: {}. pages: {} rows: {}",
                ledger_name,
                ledger_header.num_pages(),
                ledger_header.num_rows(T::ROWS_PER_PAGE as u32)
            );
        }
        //
        Ok(Self {
            //
            file,
            mmap,
            phantom: PhantomData,
        })
    }
    //
    //  ###### HEADER MANAGEMENT ######
    //
    #[inline(always)]
    pub fn access_header<'a>(&'a self) -> Result<&'a LedgerHeader, DatastoreError> {
        Ok(LedgerHeader::access(
            &self.mmap[0..Self::LEDGER_HEADER_SZ as usize],
        )?)
    }
    #[inline(always)]
    pub fn access_header_mut<'a>(&'a mut self) -> Result<Seal<'a, LedgerHeader>, DatastoreError> {
        Ok(LedgerHeader::access_mut(
            &mut self.mmap[0..Self::LEDGER_HEADER_SZ as usize],
        )?)
    }
    #[inline(always)]
    pub unsafe fn access_header_unchecked<'a>(&'a self) -> &'a LedgerHeader {
        unsafe { LedgerHeader::access_unchecked(&self.mmap[0..Self::LEDGER_HEADER_SZ as usize]) }
    }
    #[inline(always)]
    pub unsafe fn access_header_unchecked_mut<'a>(&'a mut self) -> &'a mut LedgerHeader {
        unsafe {
            LedgerHeader::access_unchecked_mut(&mut self.mmap[0..Self::LEDGER_HEADER_SZ as usize])
                .unseal()
        }
    }
    //
    //  ###### PAGE MANAGEMENT ######
    //
    pub fn clone_page(
        &self,
        page_id: usize,
    ) -> Result<SlottedPage<T, PAGESZ, ROWS_PER_PAGE>, DatastoreError> {
        if page_id >= self.total_pages()? {
            return Err(PageError::PageIdOutOfBounds.into());
        }

        let start = Self::_get_page_data_start(page_id);
        let end = start + Self::PAGE_SIZE as usize;

        let file_bytes = &self.mmap[start..end];

        let mut page = SlottedPage::new()?;
        page.data.copy_from_slice(file_bytes);

        Ok(page)
    }
    //
    pub fn access_page<'a>(
        &'a self,
        page_id: usize,
    ) -> Result<&'a SlottedPage<T, PAGESZ, ROWS_PER_PAGE>, DatastoreError> {
        if page_id >= self.total_pages()? {
            println!("total pages: {}", self.total_pages()?);
            return Err(PageError::PageIdOutOfBounds.into());
        }
        //
        let start = Self::_get_page_data_start(page_id);
        let end = start + Self::PAGE_SIZE as usize;
        //
        let page_bytes = &self.mmap[start..end];
        //
        let page: &'a SlottedPage<T, PAGESZ, ROWS_PER_PAGE> =
            access::<SlottedPage<T, PAGESZ, ROWS_PER_PAGE>, rkyv::rancor::Error>(page_bytes)?;
        //
        Ok(page)
    }
    //
    #[inline(always)]
    pub unsafe fn access_page_unchecked<'a>(
        &'a self,
        page_id: usize,
    ) -> &'a SlottedPage<T, PAGESZ, ROWS_PER_PAGE>
    where
        SlottedPage<T, PAGESZ, ROWS_PER_PAGE>: NoUndef + std::marker::Unpin,
    {
        let start = Self::_get_page_data_start(page_id);
        let end = start + Self::PAGE_SIZE as usize;
        //
        unsafe { access_unchecked::<SlottedPage<T, PAGESZ, ROWS_PER_PAGE>>(&self.mmap[start..end]) }
    }
    //
    //
    pub fn access_page_mut<'a>(
        &'a mut self,
        page_id: usize,
    ) -> Result<&'a mut SlottedPage<T, PAGESZ, ROWS_PER_PAGE>, DatastoreError>
    where
        SlottedPage<T, PAGESZ, ROWS_PER_PAGE>: NoUndef + std::marker::Unpin,
    {
        if page_id >= self.total_pages()? {
            return Err(PageError::PageIdOutOfBounds.into());
        }
        //
        let start = Self::_get_page_data_start(page_id);
        let end = start + Self::PAGE_SIZE as usize;
        //
        let page_bytes = &mut self.mmap[start..end];
        //
        let page =
            access_mut::<SlottedPage<T, PAGESZ, ROWS_PER_PAGE>, rkyv::rancor::Error>(page_bytes)?;
        //
        Ok(page.unseal())
    }
    //
    #[inline(always)]
    pub unsafe fn access_page_unchecked_mut<'a>(
        &'a mut self,
        page_id: usize,
    ) -> &'a mut SlottedPage<T, PAGESZ, ROWS_PER_PAGE>
    where
        SlottedPage<T, PAGESZ, ROWS_PER_PAGE>: NoUndef + std::marker::Unpin,
    {
        let start = Self::_get_page_data_start(page_id);
        let end = start + Self::PAGE_SIZE as usize;
        //
        unsafe {
            access_unchecked_mut::<SlottedPage<T, PAGESZ, ROWS_PER_PAGE>>(
                &mut self.mmap[start..end],
            )
        }
        .unseal()
    }
    //
    pub fn write_page(
        &mut self,
        page_id: usize,
        page: &SlottedPage<T, PAGESZ, ROWS_PER_PAGE>,
    ) -> Result<(), DatastoreError> {
        if page_id >= self.total_pages()? {
            return Err(PageError::PageIdOutOfBounds.into());
        }
        //
        let start = Self::_get_page_data_start(page_id);
        self.mmap[start..start + Self::PAGE_SIZE as usize].copy_from_slice(&page.data);
        self.mmap.flush_range(start, Self::PAGE_SIZE as usize)?;
        //
        Ok(())
    }
    //
    //
    pub fn allocate_new_page(&mut self) -> Result<usize, DatastoreError> {
        let mut header_mut = self.access_header_mut()?;
        let num_pages: usize = header_mut.num_pages() as usize;
        header_mut.set_num_pages(num_pages as u32 + 1);
        //
        let new_file_len =
            Self::LEDGER_HEADER_SZ as usize + (num_pages + 1) * Self::PAGE_SIZE as usize;
        self.file.set_len(new_file_len as u64)?;
        //
        let new_page = SlottedPage::<T, PAGESZ, ROWS_PER_PAGE>::new()?;
        //
        let start = Self::LEDGER_HEADER_SZ as usize + (num_pages) * Self::PAGE_SIZE as usize;
        //
        self.mmap = unsafe { MmapMut::map_mut(&self.file)? };
        self.mmap[start..new_file_len].copy_from_slice(&new_page.data);
        //
        tracing::info!("allocated new page ({})", num_pages);
        //
        Ok(num_pages)
    }

    #[inline(always)]
    pub fn sync_all(&self) -> std::io::Result<()> {
        self.mmap.flush()
    }

    #[inline(always)]
    fn total_pages(&self) -> Result<usize, DatastoreError> {
        Ok(self.access_header()?.num_pages() as usize)
    }

    #[inline]
    pub fn num_rows(&self) -> Result<u32, DatastoreError> {
        let num_pages: u32 = self.access_header()?.num_pages();
        let last_page: &SlottedPage<T, PAGESZ, ROWS_PER_PAGE> =
            self.access_page(num_pages as usize - 1)?;
        let num_rows: u32 = (num_pages - 1)
            * SlottedPage::<T, PAGESZ, ROWS_PER_PAGE>::max_rows_per_page() as u32
            + last_page.access_header()?.slot_count();
        //
        Ok(num_rows)
    }

    #[inline(always)]
    const fn _get_page_data_start(page_id: usize) -> usize {
        Self::LEDGER_HEADER_SZ as usize + page_id * Self::PAGE_SIZE as usize
    }

    //
    // ###### ROW OPERATIONS ######
    //

    pub fn insert(&mut self, value: &T) -> Result<u32, DatastoreError> {
        let header = match self.access_header() {
            Ok(header) => header,
            Err(e) => {
                tracing::error!("couldn't access page header. error:\n{e}");
                return Err(e);
            }
        };
        let page_id = header.page_cursor();
        //
        let page = match self.access_page_mut(page_id as usize) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("couldn't access_mut page: {}. error:\n{e}", page_id);
                return Err(e);
            }
        };
        //
        let next_page_id = match page.insert_row(value) {
            //
            Ok(page_row_n) => {
                return Ok(RowQuery::<T, PAGESZ, ROWS_PER_PAGE>::to_row_id(
                    page_id, page_row_n,
                ));
            }
            //
            Err(PageError::NoSpace) => {
                //
                match self.allocate_new_page() {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::error!(
                            "couldn't allocate_new_page for page #{}. error:\n{e}",
                            page_id
                        );
                        return Err(e);
                    }
                };
                //
                let header_mut: &mut LedgerHeader = match self.access_header_mut() {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::error!(
                            "couldn't access_header_mut for page #{}. error:\n{e}",
                            page_id
                        );
                        return Err(e);
                    }
                }
                .unseal();
                //
                header_mut.inc_page_cursor();
                //
                header_mut.page_cursor()
            }
            //
            Err(e) => return Err(e.into()),
        };
        //
        //
        let new_page = match self.access_page_mut(next_page_id as usize) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(
                    "couldn't access_page_mut for new page #{}. error:\n{e}",
                    page_id
                );
                return Err(e);
            }
        };
        let row_n = match new_page.insert_row(value) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("couldn't insert_row for new page #{}. error:\n{e}", page_id);
                return Err(super::utils::DatastoreError::PageError(e));
            }
        };
        let row_id = RowQuery::<T, PAGESZ, ROWS_PER_PAGE>::to_row_id(next_page_id, row_n);
        //
        Ok(row_id)
    }

    // pub unsafe fn insert_unchecked(&mut self, value: &T) -> u32 {
    //     todo!();
    //     // 
    //     unsafe {
    //         let header = self.access_header_unchecked();
    //         let page_id = header.page_cursor();
    //         //
    //         let free_space = self
    //             .access_page_unchecked(page_id as usize)
    //             .free_space_unchecked();
    //         // //
    //         if free_space < size_of::<T>() as u32 + 4 {
    //             //
    //             // self.allocate_new_page();

    //             //
    //         };
    //         //
    //         let page = self.access_page_unchecked_mut(page_id as usize);
    //         //
    //         let row_n = page.insert_row_unchecked(value);
    //         //
    //         let row_id = RowQuery::<T, PAGESZ, ROWS_PER_PAGE>::to_row_id(page_id, row_n);
    //         //
    //         row_id
    //     }
    // }

    //
    pub fn access_row(&self, row_id: u32) -> Result<Option<&T>, DatastoreError> {
        let query: RowQuery<T, PAGESZ, ROWS_PER_PAGE> =
            RowQuery::<T, PAGESZ, ROWS_PER_PAGE>::from_row_id(row_id);
        //
        let page: &SlottedPage<T, PAGESZ, ROWS_PER_PAGE> =
            match self.access_page(query.page_id as usize) {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!("couldn't access page #{}. error:\n{}", query.page_id, e);
                    return Err(e);
                }
            };
        //
        let row = match page.access_row(query.page_row_n as usize) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("couldn't access page #{}. error:\n{}", query.page_id, e);
                return Err(super::utils::DatastoreError::PageError(e));
            }
        };
        //
        Ok(row)
    }
    //
    pub unsafe fn access_row_unchecked(&self, row_id: u32) -> &T {
        let query: RowQuery<T, PAGESZ, ROWS_PER_PAGE> =
            RowQuery::<T, PAGESZ, ROWS_PER_PAGE>::from_row_id(row_id);
        //
        unsafe {
            self.access_page_unchecked(query.page_id as usize)
                .access_row_unchecked(query.page_row_n as usize)
        }
    }
    //
    pub fn access_row_mut(&mut self, row_id: u32) -> Result<Option<Seal<'_, T>>, DatastoreError> {
        //
        let query: RowQuery<T, PAGESZ, ROWS_PER_PAGE> =
            RowQuery::<T, PAGESZ, ROWS_PER_PAGE>::from_row_id(row_id);
        //
        let page: &mut SlottedPage<T, PAGESZ, ROWS_PER_PAGE> =
            self.access_page_mut(query.page_id as usize)?;
        //
        Ok(page.access_row_mut(query.page_row_n as usize)?)
    }
    //
    //
    //
    pub unsafe fn access_row_unchecked_mut<'a>(&'a mut self, row_id: u32) -> &'a mut T
    where
        T: rkyv::traits::NoUndef + std::marker::Unpin,
    {
        //
        let query: RowQuery<T, PAGESZ, ROWS_PER_PAGE> =
            RowQuery::<T, PAGESZ, ROWS_PER_PAGE>::from_row_id(row_id);
        //
        let page: &mut SlottedPage<T, PAGESZ, ROWS_PER_PAGE> =
            unsafe { self.access_page_unchecked_mut(query.page_id as usize) };
        //
        unsafe {
            page.access_row_unchecked_mut(query.page_row_n as usize)
                .unseal()
        }
    }
}

// ### ROW QUERY

#[derive(Debug)]
pub struct RowQuery<T, const PAGESZ: usize, const ROWS_PER_PAGE: usize> {
    pub page_id: u32,
    pub page_row_n: u32,
    //
    phantom: PhantomData<T>,
}

impl<T, const PAGESZ: usize, const ROWS_PER_PAGE: usize> RowQuery<T, PAGESZ, ROWS_PER_PAGE>
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
    #[inline(always)]
    pub const fn to_row_id(page_id: u32, row_n: u32) -> u32 {
        (page_id * SlottedPage::<T, PAGESZ, ROWS_PER_PAGE>::max_rows_per_page() as u32) + row_n
    }
    //
    #[inline(always)]
    pub const fn from_row_id(row_id: u32) -> RowQuery<T, PAGESZ, ROWS_PER_PAGE> {
        let max_rows = SlottedPage::<T, PAGESZ, ROWS_PER_PAGE>::max_rows_per_page() as u32;
        let page_id = row_id / max_rows;
        let page_row_n = row_id % max_rows;
        //
        RowQuery::<T, PAGESZ, ROWS_PER_PAGE> {
            page_id,
            page_row_n,
            phantom: PhantomData,
        }
    }
}
