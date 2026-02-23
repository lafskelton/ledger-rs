use ledger_rs::{page::PageSchema, utils::DatastoreError};
use ledger_rs_macros::ledger;
use rand::seq::SliceRandom;
use std::path::Path;

#[ledger]
pub struct MyRowType {
    pub val_u8: u8,
    pub val_u16: u16,
    pub val_u32: u32,
    pub val_u64: u64,
    pub val_u128: u128,
    //
    pub val_i8: i8,
    pub val_i16: i16,
    pub val_i32: i32,
    pub val_i64: i64,
    pub val_i128: i128,
    //
    pub val_f32: f32,
    pub val_f64: f64,
    //
    #[max_len(32)]
    pub val_string: String,
    //
    pub val_bytes: [u8; 32],
}

#[ledger]
pub struct FileManifest {
    pub id: u32,
    //
    #[max_len(32)]
    pub title: String,
    //
    #[max_len(32)]
    pub location: String,
}

const TEST_LEN: u32 = 1234567;

pub fn main() -> Result<(), DatastoreError> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    //
    let mut ledger = FileManifest::create_ledger(Path::new("./"), "Documents", "My documents")?;
    //
    for n in 0..TEST_LEN {
        let row_id = ledger.insert(&FileManifest::new(
            n,
            &format!("document #{n}"),
            &format!("/home/ubuntu/Documents/document_{n}.txt"),
        ))?;
        if row_id != n {
            panic!("row doesn't match {} != {}", row_id, n);
        }
    }
    //
    let mut numbers: Vec<u32> = (0..TEST_LEN).collect();
    let mut rng = rand::rng();
    numbers.shuffle(&mut rng);
    //
    for n in 0..TEST_LEN {
        //
        let Some(row) = ledger.access_row(n)? else {
            panic!("row not found!");
        };
        //
        if row.id() != n {
            panic!("row doesn't match");
        }
        //
    }
    //
    //
    Ok(())
}
