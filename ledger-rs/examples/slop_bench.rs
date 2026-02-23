use colored::*;
use ledger_rs::page::PageSchema;
use ledger_rs::utils::DatastoreError;
use ledger_rs_macros::ledger;
use rand::seq::SliceRandom;
use std::hint::black_box;
use std::path::Path;
use std::time::{Duration, Instant};

/**
 *  CERTIFIED SLOP BROUGHT TO YOU BY:
 *      GEMINI 3.1 PRO   
 */

const TEST_LEN: usize = 123_4567;

#[ledger]
pub struct FileManifest {
    pub id: u32,
    #[max_len(32)]
    pub title: String,
    #[max_len(32)]
    pub location: String,
}

// Slop-tier statistical analysis helper
fn print_stats(name: &str, times: &mut [Duration], total_duration: Duration, num_records: usize) {
    times.sort_unstable();
    let count = times.len() as f64;
    let ops_sec = count / total_duration.as_secs_f64();

    let p50 = times[(count * 0.50) as usize];
    let p90 = times[(count * 0.90) as usize];
    let p99 = times[(count * 0.99) as usize];
    let p999 = times[(count * 0.999) as usize];
    let max = times.last().unwrap_or(&Duration::ZERO);

    let record_size = std::mem::size_of::<FileManifest>();
    let est_size_mb = ((record_size + 4) * num_records) / 1_048_576; // 1024 * 1024

    println!("{}", format!("📊 {} STATS", name).cyan().bold());
    println!(
        "  {} Record Size: {}",
        "📦".yellow(),
        format!("{} bytes", record_size).cyan()
    );
    println!(
        "  {} Est. Ledger: {}",
        "💽".yellow(),
        format!("~{} MB\n", est_size_mb).bright_magenta().bold()
    );
    println!(
        "  {} Throughput: {}",
        "⚡".yellow(),
        format!("{:>10.2} ops/sec", ops_sec).green().bold()
    );
    println!(
        "  {} Total Time: {}",
        "⏱️ ".yellow(),
        format!("{:>10.2?}", total_duration).white()
    );
    println!(
        "  {} Latencies:  p50: {:>8?} | p90: {:>8?} | p99: {:>8?} | p99.9: {:>8?} | Max: {:>8?}",
        "📈".yellow(),
        p50,
        p90,
        p99,
        p999,
        max
    );
    println!();
}

pub fn main() -> Result<(), DatastoreError> {
    let ledger_dir = Path::new("./");
    let ledger_name = "Documents";
    let ledger_path = ledger_dir.join(ledger_name);

    println!(
        "{}",
        "==================================================".bright_blue()
    );
    println!(
        "{}",
        "🚀 INITIATING SLOP HYPER-BENCHMARK SUITE v9000 🚀"
            .bright_magenta()
            .bold()
    );
    println!(
        "{}",
        "==================================================\n".bright_blue()
    );

    // 1. Clean up previous runs
    if ledger_path.exists() {
        print!("{} Nuking old ledger state... ", "🔥".red());
        std::fs::remove_file(&ledger_path).map_err(|e| DatastoreError::Error(e.to_string()))?;
        println!("{}", "DONE".green());
    }

    let mut ledger = FileManifest::create_ledger(ledger_dir, ledger_name, "My documents")?;
    let num_records = TEST_LEN;

    // Pre-allocate massive vectors so memory reallocation doesn't contaminate our timings
    let mut insert_latencies = Vec::with_capacity(num_records);
    let mut read_latencies = Vec::with_capacity(num_records);
    let mut mut_latencies = Vec::with_capacity(num_records);

    // --- BENCHMARK: SEQUENTIAL INSERTION ---
    println!(
        "{}",
        format!(
            "Starting sequential insertion of {} records...",
            num_records
        )
        .blue()
    );
    let global_start_insert = Instant::now();

    for n in 0..num_records as u32 {
        // Build strings outside the core timer to isolate ledger overhead
        let title = format!("document #{n}");
        let loc = format!("/home/ubuntu/Documents/document_{n}.txt");
        let manifest = FileManifest::new(n, &title, &loc);

        let iter_start = Instant::now();
        // black_box ensures the compiler doesn't get clever and skip operations
        let _row_id = black_box(ledger.insert(black_box(&manifest))?);
        insert_latencies.push(iter_start.elapsed());
    }

    // Track disk sync separately to see how much the OS was hiding from us
    let sync_start = Instant::now();
    ledger
        .sync_all()
        .map_err(|e| DatastoreError::Error(e.to_string()))?;
    let sync_duration = sync_start.elapsed();
    let total_insert_duration = global_start_insert.elapsed();

    println!("{} Disk sync cost: {:?}", "💾".bright_blue(), sync_duration);
    print_stats(
        "INSERTION",
        &mut insert_latencies,
        total_insert_duration,
        num_records,
    );

    // --- PREPARE RANDOM READS ---
    print!(
        "{} Generating cryptographic-grade chaos entropy (shuffling)... ",
        "🎲".magenta()
    );
    let mut numbers: Vec<u32> = (0..num_records as u32).collect();
    let mut rng = rand::rng();
    numbers.shuffle(&mut rng);
    println!("{}", "DONE\n".green());

    // --- BENCHMARK: RANDOM ACCESS ---
    println!(
        "{}",
        format!(
            "Starting chaotic random access of {} records...",
            num_records
        )
        .blue()
    );
    let global_start_read = Instant::now();

    for &n in &numbers {
        let iter_start = Instant::now();

        let Some(row) = black_box(ledger.access_row(black_box(n))?) else {
            panic!("💀 Row {} vanished into the ether!", n);
        };

        if row.id() != n {
            panic!(
                "💀 Cosmic ray bitflip detected! Expected {}, got {}",
                n,
                row.id()
            );
        }

        read_latencies.push(iter_start.elapsed());
    }

    let total_read_duration = global_start_read.elapsed();
    print_stats(
        "RANDOM READ",
        &mut read_latencies,
        total_read_duration,
        num_records as usize,
    );

    // --- BENCHMARK: RANDOM MUTATION ---
    println!(
        "{}",
        format!(
            "Starting chaotic random mutations of {} records...",
            num_records
        )
        .blue()
    );
    let global_start_mut = Instant::now();

    for &n in &numbers {
        let iter_start = Instant::now();

        // Getting the mutable seal counts as a write access for the page
        let Some(row_seal) = black_box(ledger.access_row_mut(black_box(n))?) else {
            panic!("💀 Mutable row {} was swallowed by the void!", n);
        };
        let row: &mut FileManifest = row_seal.unseal();
        // 
        row.set_title("title changed!")?;
        // 
        mut_latencies.push(iter_start.elapsed());
    }

    // Force sync again to see the cost of flushing the dirtied pages
    let mut_sync_start = Instant::now();
    ledger
        .sync_all()
        .map_err(|e| DatastoreError::Error(e.to_string()))?;
    let mut_sync_duration = mut_sync_start.elapsed();
    let total_mut_duration = global_start_mut.elapsed();

    println!(
        "{} Disk sync cost (post-mutation): {:?}",
        "💾".bright_blue(),
        mut_sync_duration
    );
    print_stats(
        "RANDOM MUTATION",
        &mut mut_latencies,
        total_mut_duration,
        num_records as usize,
    );

    println!(
        "{}",
        "🎉 BENCHMARK COMPLETE. GO GET A COFFEE. 🎉"
            .bright_green()
            .bold()
    );
    Ok(())
}
