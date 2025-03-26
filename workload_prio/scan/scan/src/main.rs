#![allow(warnings, unused)]
use chrono::NaiveDate;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use rand::Rng;
use scylla::frame::value::CqlTimestamp;
use scylla::frame::value::ValueList;
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::errors::QueryError;
use scylla::statement::Consistency;
use scylla::load_balancing::DefaultPolicy;
use scylla::transport::ExecutionProfile;
use scylla::transport::retry_policy::DefaultRetryPolicy;
use scylla::transport::Compression;
use scylla::IntoTypedRows;
use scylla::{Session, SessionBuilder};
use std::env;
use std::error::Error;
use std::process;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::{thread, time};
use tokio::sync::Semaphore;
use tracing::info;
use uuid::Uuid;

fn help() {
    println!("usage: <host> <dc>");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Simple argparse
    tracing_subscriber::fmt::init();


    let args: Vec<String> = env::args().collect();

    let mut host = "127.0.0.1";
    let mut dc = "datacenter1";
    let mut usr = "scylla";
    let mut pwd = "scylla";
    let mut factor = "4";

    match args.len() {
        1 => {
            println!("Using default values. Host: {}, DC: {}, Username: {}, Password: ********", host, dc, usr);
        }
        2 => {
            host = &args[1];
        }
        3 => {
            host = &args[1];
            dc = &args[2];
        }
        5 => {
            host = &args[1];
            dc = &args[2];
            usr = &args[3];
            pwd = &args[4];
        }
        6 => {
            host = &args[1];
            dc = &args[2];
            usr = &args[3];
            pwd = &args[4];
            factor = &args[5];
        }
        _ => {
            help();
        }
    }

    let ks = "keyspace1";
    let table = "standard1";

    // Initiate cluster session
    println!("Connecting to {} ...", host);
    let default_policy = DefaultPolicy::builder()
        .prefer_datacenter(dc.to_string())
        .token_aware(true)
        .permit_dc_failover(false)
        .build();

    let profile = ExecutionProfile::builder()
        .load_balancing_policy(default_policy)
        .request_timeout(Some(Duration::from_secs(1)))
        .build();

    let handle = profile.into_handle();

    let session: Session = SessionBuilder::new()
        .known_node(host)
        .default_execution_profile_handle(handle)
        // .compression(Some(Compression::Lz4))
        .user(usr, pwd)
        .build()
        .await?;
    let session = Arc::new(session);

    println!("Connected successfully! Policy: TokenAware(DCAware())");

    // Set-up full table scan token ranges, shards and nodes

    let min_token = -(i128::pow(2, 63) - 1);
    let max_token = (i128::pow(2, 63) - 1);
    println!("Min token: {} \nMax token: {}", min_token, max_token);

    let num_nodes = 1;
    let cores_per_node = 14;

    // Parallelism is number of nodes * shards * 300% (ensure we keep them busy)
    let PARALLEL = (num_nodes * cores_per_node) * <i32 as FromStr>::from_str(factor).unwrap(); // * cores_per_node * 1);

    // Subrange is a division applied to the token ring. How many queries we'll send in total ?
    // The key here is balance. 256 vNodes may get scanned quickly in a sparse table with few data,
    // but take a long time (and timeout) in a table with lots of data.
    //
    // To find the ideal number, set PARALLEL=1 and time how long it takes for each subrange scan.
    let SUBRANGE = 256 * 1024;

    println!(
        "Max Parallel queries: {}\nToken-ring Subranges:{}",
        PARALLEL, SUBRANGE
    );

    // How many tokens are there?
    let total_token = max_token - min_token;
    println!("Total tokens: {}", total_token);

    // Chunk size determines the number of iterations needed to query the whole token ring
    let chunk_size = total_token / SUBRANGE;
    println!("Number of iterations: {}", chunk_size);

    // Prepare Statement - use LocalQuorum
    let stmt = format!("SELECT key AS total FROM {}.{} WHERE token(key) >= ? AND token(key) <= ? BYPASS CACHE", ks, table);
    let mut ps: PreparedStatement = session.prepare(stmt).await?;
    ps.set_consistency(Consistency::LocalQuorum);

    // Retry policy
    ps.set_retry_policy(Some(Arc::new(DefaultRetryPolicy::new())));

    println!();

    // 1. Spawn a new semaphore
    let sem = Arc::new(Semaphore::new((PARALLEL) as usize));

    let mut count: i64 = 0;

    // Initiate querying the token ring
    let large_string = " ".repeat(u32::MAX as usize);
    println!("Past large alloc!");
    for x in (min_token..max_token).step_by((chunk_size as usize)) {
        let session = session.clone();
        let ps = ps.clone();
        let permit = sem.clone().acquire_owned().await;

        let mut v = vec![(x as i64), ((x + chunk_size - 1) as i64)];
        // Debug: Run this [ cargo run 172.17.0.2 north ks bla| egrep '^.[0-9][0-9][0-9][0-9][0-9]' | wc -l ]
        // This will return exact SUBRANGES numbers ;-)
        // println!("Querying: {} to {}", v[0], v[1]);
        tokio::task::spawn(async move {
            let now = SystemTime::now();
            match session.execute(&ps, (&v[0], &large_string)).await {
                Ok(query_result) => {
                if let Some(rows) = query_result.rows {
                    for row in rows {
                        count += 1;
                    }
                    match now.elapsed() {
                        Ok(elapsed) => {
                            println!("Processed {} rows in {} ms", count, elapsed.as_millis());
                        }
                        Err(e) => {
                           println!("Error: {e:?}");
                        }
                   }
               }
            },
            Err(err) => {
                match err {
                    QueryError::TooManyOrphanedStreamIds(msg) => {
                        println!("{err}");
                    },
                    _ => {}
                }
            }
            }
            let _permit = permit;
        });
    }

    // Wait for all in-flight requests to finish
    for _ in 0..PARALLEL {
        sem.acquire().await.unwrap().forget();
    }

    // Print final metrics
    let metrics = session.get_metrics();
    println!("Queries requested: {}", metrics.get_queries_num());
    println!("Iter queries requested: {}", metrics.get_queries_iter_num());
    println!("Errors occured: {}", metrics.get_errors_num());
    println!("Iter errors occured: {}", metrics.get_errors_iter_num());
    println!("Average latency: {}", metrics.get_latency_avg_ms().unwrap());
    println!(
        "99.9 latency percentile: {}",
        metrics.get_latency_percentile_ms(99.9).unwrap()
    );

    Ok(())
}
