//! `FileIo` over object storage (S3 / MinIO) and the local filesystem.
//!
//! Inspection runs on a blocking thread; reads block on the async object store client through the
//! runtime handle captured at construction.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use integrity_iceberg::{FileIo, ReadError};
use object_store::ObjectStoreExt;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path;

use crate::config::StorageConfig;

/// Reads `s3://`, `s3a://`, `file://` and plain local paths.
#[derive(Debug)]
pub struct ObjectStoreIo {
    config: StorageConfig,
    runtime: tokio::runtime::Handle,
    buckets: Mutex<HashMap<String, Arc<AmazonS3>>>,
}

impl ObjectStoreIo {
    /// Uses `runtime` for the async S3 client.
    pub fn new(config: StorageConfig, runtime: tokio::runtime::Handle) -> Self {
        Self {
            config,
            runtime,
            buckets: Mutex::new(HashMap::new()),
        }
    }

    fn bucket(&self, name: &str) -> Result<Arc<AmazonS3>, ReadError> {
        let mut buckets = self
            .buckets
            .lock()
            .map_err(|_| ReadError::Io("storage client poisoned".into()))?;
        if let Some(b) = buckets.get(name) {
            return Ok(Arc::clone(b));
        }
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(name)
            .with_allow_http(self.config.allow_http);
        if let Some(e) = &self.config.endpoint {
            builder = builder.with_endpoint(e);
        }
        if let Some(r) = &self.config.region {
            builder = builder.with_region(r);
        }
        if let Some(k) = &self.config.access_key_id {
            builder = builder.with_access_key_id(k);
        }
        if let Some(s) = &self.config.secret_access_key {
            builder = builder.with_secret_access_key(s);
        }
        let store = Arc::new(builder.build().map_err(|e| ReadError::Io(e.to_string()))?);
        buckets.insert(name.to_owned(), Arc::clone(&store));
        Ok(store)
    }
}

impl FileIo for ObjectStoreIo {
    fn read(&self, location: &str) -> Result<Bytes, ReadError> {
        let s3 = location
            .strip_prefix("s3://")
            .or_else(|| location.strip_prefix("s3a://"));
        if let Some(rest) = s3 {
            let (bucket, key) = rest
                .split_once('/')
                .ok_or_else(|| ReadError::Io(format!("no key in {location}")))?;
            let store = self.bucket(bucket)?;
            let path = Path::parse(key).map_err(|e| ReadError::Io(e.to_string()))?;
            return self
                .runtime
                .block_on(async move { store.get(&path).await?.bytes().await })
                .map_err(|e| ReadError::Io(e.to_string()));
        }
        // `file:///tmp/x`, Hadoop's `file:/tmp/x`, or a plain path.
        let local = location
            .strip_prefix("file://")
            .or_else(|| location.strip_prefix("file:"))
            .unwrap_or(location);
        std::fs::read(local)
            .map(Bytes::from)
            .map_err(|e| ReadError::Io(format!("{location}: {e}")))
    }
}
