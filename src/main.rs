use std::time::Instant;

use anyhow::anyhow;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};

use rusoto_s3::{GetObjectRequest, S3Client, S3};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncBufReadExt;

/// This is a made-up example. Requests come into the runtime as unicode
/// strings in json format, which can map to any structure that implements `serde::Deserialize`
/// The runtime pays no attention to the contents of the request payload.
#[derive(Deserialize)]
struct Request {
    bucket: String,
    key: String,
}

/// This is a made-up example of what a response structure may look like.
/// There is no restriction on what it can be. The runtime requires responses
/// to be serialized into json. The runtime pays no attention
/// to the contents of the response payload.
#[derive(Serialize)]
struct Response {
    req_id: String,
    msg: String,
}

/// This is the main body for the function.
/// Write your code inside it.
/// There are some code example in the following URLs:
/// - https://github.com/awslabs/aws-lambda-rust-runtime/tree/main/examples
/// - https://github.com/aws-samples/serverless-rust-demo/
async fn function_handler(event: LambdaEvent<Request>) -> Result<Response, Error> {
    let bucket = &event.payload.bucket;
    let key = &event.payload.key;

    let started_at = Instant::now();

    let client = S3Client::new(rusoto_core::Region::EuWest1);

    // Initiate a GetObject request to S3.
    let output = client
        .get_object(GetObjectRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            ..Default::default()
        })
        .await?;

    let Some(body) = output.body else {
        return Err(anyhow!("No body found in S3 response").into())
    };

    // Begin streaming the contents down, decompressing on the fly, and
    // iterating over each chunk split by newlines.

    let body = body.into_async_read();
    let body = tokio::io::BufReader::new(body);

    let decoder = async_compression::tokio::bufread::ZstdDecoder::new(body);
    let reader = tokio::io::BufReader::new(decoder);

    let mut lines = reader.lines();
    let mut num_log_events = 0;
    // For each line we encounter while asynchronously streaming down the
    // S3 data, parse the JSON object.
    while let Some(line) = lines.next_line().await? {
        let _value = serde_json::from_str(&line)?;
        num_log_events += 1;
        if num_log_events % 1000 == 0 {
            println!("num_log_events={}", num_log_events);
        }
    }

    let msg = format!(
        "elapsed={:?} num_log_events={}",
        started_at.elapsed(),
        num_log_events
    );

    let resp = Response {
        req_id: event.context.request_id,
        msg,
    };

    Ok(resp)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    run(service_fn(function_handler)).await
}
