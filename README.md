# Rust + AWS Lambda + S3

## Release

```sh
cargo lambda build \
  --release \
  --arm64
```

## Deploy
```sh
cargo lambda deploy \
  --profile dev \
  --region us-west-2 \
  --timeout 45 \
  rust-lambda-s3-test
  ```