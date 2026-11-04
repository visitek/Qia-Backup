//! Object storage: a small trait, a local filesystem implementation for tests,
//! and an AWS S3 implementation with SigV4 signing.

use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::core::crypto::{hmac_sha256, sha256_hex};
use crate::core::engine::CancelToken;
use crate::core::error::{BackupError, IoContext, Result};
use crate::core::throttle::{ThrottledReader, UploadLimiter};

#[derive(Debug, Clone)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    pub etag: Option<String>,
}

/// One page of a listing. `next` is the continuation token to pass to the next
/// call, or `None` when the prefix has been exhausted.
#[derive(Debug, Clone)]
pub struct ObjectPage {
    pub objects: Vec<ObjectInfo>,
    pub next: Option<String>,
}

pub trait BlobStore: Send + Sync {
    fn put(&self, key: &str, data: &[u8]) -> Result<()>;
    fn get(&self, key: &str) -> Result<Vec<u8>>;
    fn get_opt(&self, key: &str) -> Result<Option<Vec<u8>>>;
    fn head(&self, key: &str) -> Result<Option<ObjectInfo>>;
    fn delete(&self, key: &str) -> Result<()>;
    fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>>;

    /// Return one page of objects under `prefix`. The default implementation is
    /// for small/local stores; S3 overrides it so a multi-million-object backup
    /// set can be scanned without holding the whole listing in memory.
    fn list_page(
        &self,
        prefix: &str,
        continuation: Option<&str>,
        max_keys: u32,
    ) -> Result<ObjectPage> {
        let objects = self.list(prefix)?;
        let start = continuation
            .and_then(|token| token.parse::<usize>().ok())
            .unwrap_or(0)
            .min(objects.len());
        let limit = max_keys.max(1) as usize;
        let end = start.saturating_add(limit).min(objects.len());
        let next = (end < objects.len()).then(|| end.to_string());
        Ok(ObjectPage {
            objects: objects[start..end].to_vec(),
            next,
        })
    }

    /// Read `length` bytes starting at `offset`. The default implementation
    /// reads the whole object; S3 uses an HTTP Range request so packed blobs
    /// do not require downloading the entire pack.
    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
        let raw = self.get(key)?;
        let start = offset as usize;
        let end = start
            .checked_add(length as usize)
            .ok_or_else(|| BackupError::Codec("blob range overflow".into()))?;
        if end > raw.len() {
            return Err(BackupError::Store(format!(
                "range {offset}..{end} exceeds object {key} size {}",
                raw.len()
            )));
        }
        Ok(raw[start..end].to_vec())
    }

    /// Return the first object under `prefix`, if any. This is used for cheap
    /// connectivity checks that must not enumerate a multi-million-object set.
    fn list_first(&self, prefix: &str) -> Result<Option<ObjectInfo>> {
        Ok(self.list(prefix)?.into_iter().next())
    }

    /// Verify bucket access without listing the whole backup set.
    fn check_access(&self) -> Result<()> {
        self.list_first("").map(|_| ())
    }

    /// Delete every object under `prefix`. The default implementation uses
    /// `list`; S3 overrides it to page through the results so very large
    /// backup sets never hit the in-memory list cap.
    fn delete_prefix(&self, prefix: &str) -> Result<usize> {
        let objects = self.list(prefix)?;
        let mut deleted = 0usize;
        for object in objects {
            self.delete(&object.key)?;
            deleted += 1;
        }
        Ok(deleted)
    }

    fn describe(&self) -> String;

    /// Shared upload limiter attached to this store, if any. The upload pool
    /// uses it to report the real aggregate wire rate instead of estimating it
    /// from per-worker samples (which over-reports when concurrency changes).
    fn upload_limiter(&self) -> Option<Arc<UploadLimiter>> {
        None
    }
}

// ---------------------------------------------------------------------------
// Local store (used by tests and the `folder` fallback in development)
// ---------------------------------------------------------------------------

pub struct LocalStore {
    root: PathBuf,
}

impl LocalStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root).at(&root)?;
        Ok(Self { root })
    }

    fn path(&self, key: &str) -> Result<PathBuf> {
        let rel = key.trim_start_matches('/');
        let candidate = self.root.join(rel);
        // Path traversal guard: the canonical parent must remain under root.
        if rel.split('/').any(|part| part == "..") {
            return Err(BackupError::Store(format!("invalid key: {key}")));
        }
        Ok(candidate)
    }
}

impl BlobStore for LocalStore {
    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        let path = self.path(key)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).at(parent)?;
        }
        let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
        std::fs::write(&tmp, data).at(&tmp)?;
        std::fs::rename(&tmp, &path).at(&path)?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        let path = self.path(key)?;
        std::fs::read(&path).at(path)
    }

    fn get_opt(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let path = self.path(key)?;
        match std::fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(BackupError::io(path, e)),
        }
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
        let path = self.path(key)?;
        let mut file = std::fs::File::open(&path).at(&path)?;
        file.seek(SeekFrom::Start(offset)).at(&path)?;
        let mut buffer = vec![0u8; length as usize];
        file.read_exact(&mut buffer).at(&path)?;
        Ok(buffer)
    }

    fn head(&self, key: &str) -> Result<Option<ObjectInfo>> {
        let path = self.path(key)?;
        match std::fs::metadata(&path) {
            Ok(meta) => Ok(Some(ObjectInfo {
                key: key.to_string(),
                size: meta.len(),
                etag: None,
            })),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(BackupError::io(path, e)),
        }
    }

    fn delete(&self, key: &str) -> Result<()> {
        let path = self.path(key)?;
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(BackupError::io(path, e)),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>> {
        let mut out = Vec::new();
        if !self.root.exists() {
            return Ok(out);
        }
        for entry in walkdir::WalkDir::new(&self.root)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if !entry.file_type().is_file() {
                continue;
            }
            let rel = match entry.path().strip_prefix(&self.root) {
                Ok(rel) => rel.to_string_lossy().replace('\\', "/"),
                Err(_) => continue,
            };
            if !rel.starts_with(prefix) {
                continue;
            }
            out.push(ObjectInfo {
                key: rel,
                size: entry.metadata().map(|m| m.len()).unwrap_or(0),
                etag: None,
            });
        }
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    fn describe(&self) -> String {
        format!("local:{}", self.root.display())
    }
}

// ---------------------------------------------------------------------------
// S3
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("access_key_id", &self.access_key_id)
            .field(
                "secret_access_key",
                &if self.secret_access_key.is_empty() {
                    ""
                } else {
                    "<redacted>"
                },
            )
            .finish()
    }
}

impl S3Config {
    pub fn from_app(config: &crate::core::model::AppConfig) -> Self {
        Self {
            bucket: config.bucket.trim().to_string(),
            region: config.region.trim().to_string(),
            access_key_id: config.access_key_id.trim().to_string(),
            secret_access_key: config.secret_access_key.clone(),
        }
    }
}

pub struct S3Store {
    config: S3Config,
    client: reqwest::blocking::Client,
    /// Offset in seconds applied to the local clock when signing requests.
    /// S3 rejects signatures whose `x-amz-date` is more than 15 minutes away
    /// from its own clock (`RequestTimeTooSkewed`). If that happens we adopt
    /// the server's clock from the error response and retry, so a Mac whose
    /// clock has drifted can still back up without manual intervention.
    clock_offset_secs: AtomicI64,
    /// Optional shared upload throttle applied while the request body streams.
    throttle: Option<UploadThrottle>,
}

struct UploadThrottle {
    limiter: Arc<UploadLimiter>,
    cancel: CancelToken,
}

#[derive(Debug, Clone)]
struct RequestTarget {
    url: String,
    host: String,
    canonical_uri: String,
    canonical_query: String,
}

/// A completed S3 request. Headers are kept because `HEAD` responses carry
/// the object size in `Content-Length` and no body at all.
struct S3Response {
    status: u16,
    body: Vec<u8>,
    headers: reqwest::header::HeaderMap,
}

impl S3Store {
    pub fn new(config: S3Config) -> Result<Self> {
        if config.bucket.is_empty() {
            return Err(BackupError::S3("bucket is empty".into()));
        }
        if config.region.is_empty() {
            return Err(BackupError::S3("region is empty".into()));
        }
        let client = reqwest::blocking::Client::builder()
            .connect_timeout(Duration::from_secs(20))
            .timeout(Duration::from_secs(600))
            .user_agent(format!("backup/{}", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| BackupError::S3(format!("HTTP client: {e}")))?;
        Ok(Self {
            config,
            client,
            clock_offset_secs: AtomicI64::new(0),
            throttle: None,
        })
    }

    /// Attach a shared, dynamically adjustable upload limiter. The limiter is
    /// consulted as the PUT body is read, so changing it affects in-flight
    /// requests rather than only the next job.
    pub fn with_upload_throttle(
        mut self,
        limiter: Arc<UploadLimiter>,
        cancel: CancelToken,
    ) -> Self {
        self.throttle = Some(UploadThrottle { limiter, cancel });
        self
    }

    pub fn config(&self) -> &S3Config {
        &self.config
    }

    /// The clock used for SigV4 signing: local time plus any correction
    /// learned from an S3 `RequestTimeTooSkewed` response.
    fn signing_now(&self) -> DateTime<Utc> {
        let offset = self.clock_offset_secs.load(Ordering::Relaxed);
        Utc::now() + chrono::Duration::seconds(offset)
    }

    /// Adopt `server_time` as the reference clock for signing. Returns true if
    /// the offset actually changed, so the caller can retry the request.
    fn adopt_server_clock(&self, server_time: DateTime<Utc>) -> bool {
        let offset = server_time.signed_duration_since(Utc::now());
        // Ignore implausible values (for example a broken proxy or a cached
        // response) rather than moving the signing clock far into the past or
        // future. One day is far beyond both the AWS tolerance and any useful
        // correction for a drifting system clock.
        if offset.num_seconds().abs() > 86_400 {
            return false;
        }
        let previous = self
            .clock_offset_secs
            .swap(offset.num_seconds(), Ordering::Relaxed);
        previous != offset.num_seconds()
    }

    fn target(&self, key: &str, query: &[(String, String)]) -> Result<RequestTarget> {
        let escaped_key = uri_encode_path(key);
        // AWS only supports virtual-hosted-style bucket addressing.
        let host = format!(
            "{}.s3.{}.amazonaws.com",
            self.config.bucket, self.config.region
        );
        let scheme = String::from("https");
        let path = format!("/{escaped_key}");
        let canonical_query = canonical_query(query);
        let query_suffix = if canonical_query.is_empty() {
            String::new()
        } else {
            format!("?{canonical_query}")
        };
        let url = format!("{scheme}://{host}{path}{query_suffix}");
        Ok(RequestTarget {
            url,
            host,
            canonical_uri: path,
            canonical_query,
        })
    }

    fn sign(
        &self,
        method: &str,
        target: &RequestTarget,
        payload_hash: &str,
        extra_headers: &[(String, String)],
    ) -> Result<Vec<(String, String)>> {
        self.sign_at(
            method,
            target,
            payload_hash,
            extra_headers,
            self.signing_now(),
        )
    }

    fn sign_at(
        &self,
        method: &str,
        target: &RequestTarget,
        payload_hash: &str,
        extra_headers: &[(String, String)],
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<(String, String)>> {
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date = now.format("%Y%m%d").to_string();

        let mut headers: Vec<(String, String)> = vec![
            ("host".to_string(), target.host.clone()),
            ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        for (k, v) in extra_headers {
            headers.push((k.to_ascii_lowercase(), v.clone()));
        }
        headers.sort_by(|a, b| a.0.cmp(&b.0));

        let canonical_headers: String = headers
            .iter()
            .map(|(k, v)| format!("{}:{}\n", k, v.trim()))
            .collect();
        let signed_headers = headers
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>()
            .join(";");

        let canonical_request = format!(
            "{method}\n{}\n{}\n{}\n{}\n{}",
            target.canonical_uri,
            target.canonical_query,
            canonical_headers,
            signed_headers,
            payload_hash
        );
        let scope = format!("{date}/{}/s3/aws4_request", self.config.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            sha256_hex(canonical_request.as_bytes())
        );
        let secret = format!("AWS4{}", self.config.secret_access_key);
        let k_date = hmac_sha256(secret.as_bytes(), &[date.as_bytes()]);
        let k_region = hmac_sha256(&k_date, &[self.config.region.as_bytes()]);
        let k_service = hmac_sha256(&k_region, &[b"s3"]);
        let k_signing = hmac_sha256(&k_service, &[b"aws4_request"]);
        let signature = hex::encode(hmac_sha256(&k_signing, &[string_to_sign.as_bytes()]));

        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.config.access_key_id
        );
        let mut out = headers;
        out.push(("authorization".to_string(), authorization));
        Ok(out)
    }

    fn execute(
        &self,
        method: &str,
        key: &str,
        query: &[(String, String)],
        body: Option<&[u8]>,
        extra_headers: &[(String, String)],
    ) -> Result<S3Response> {
        let target = self.target(key, query)?;
        let body_bytes = body.unwrap_or(&[]);
        let payload_hash = sha256_hex(body_bytes);

        let method_name = method;
        let method = reqwest::Method::from_bytes(method.as_bytes())
            .map_err(|e| BackupError::S3(format!("invalid method: {e}")))?;

        let mut last_error = String::new();
        // Bounded retries with exponential backoff. This absorbs short Wi-Fi
        // drops and S3 throttling; longer outages abort safely and the
        // scheduler retries the backup later.
        const MAX_ATTEMPTS: usize = 6;
        for attempt in 0..MAX_ATTEMPTS {
            if self
                .throttle
                .as_ref()
                .is_some_and(|throttle| throttle.cancel.is_cancelled())
            {
                return Err(BackupError::Cancelled);
            }
            // Sign every attempt: retries must use a fresh `x-amz-date`, and a
            // clock correction learned from a previous response has to be
            // applied to the next signature.
            let headers = self.sign(method_name, &target, &payload_hash, extra_headers)?;
            let mut req = self.client.request(method.clone(), &target.url);
            for (k, v) in &headers {
                req = req.header(k, v);
            }
            if let Some(body) = body {
                if let Some(throttle) = &self.throttle {
                    let reader = ThrottledReader::new(
                        Cursor::new(body.to_vec()),
                        throttle.limiter.clone(),
                        throttle.cancel.clone(),
                    );
                    req = req.body(reqwest::blocking::Body::sized(
                        reader,
                        body.len() as u64,
                    ));
                } else {
                    req = req.body(body.to_vec());
                }
            }
            match req.send() {
                Ok(response) => {
                    let status = response.status().as_u16();
                    let response_headers = response.headers().clone();
                    let response_date = response_headers
                        .get(reqwest::header::DATE)
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| DateTime::parse_from_rfc2822(value).ok())
                        .map(|value| value.with_timezone(&Utc));
                    let bytes = match response.bytes() {
                        Ok(bytes) => bytes.to_vec(),
                        Err(e) => {
                            // The connection can also drop while reading the
                            // response body, after the request was accepted.
                            last_error = format!("reading response: {e}");
                            if attempt + 1 < MAX_ATTEMPTS {
                                let delay = 400u64.saturating_mul(1 << attempt).min(8_000);
                                std::thread::sleep(Duration::from_millis(delay));
                                continue;
                            }
                            return Err(BackupError::S3(last_error));
                        }
                    };
                    // S3 rejects signatures that are more than 15 minutes
                    // away from its clock. Adopt the server's time from the
                    // error and retry immediately with a corrected signature
                    // instead of failing the whole backup.
                    if status == 403
                        && let Some(server_time) = clock_skew_server_time(&bytes, response_date)
                        && self.adopt_server_clock(server_time)
                    {
                        last_error = format!(
                            "HTTP {status}: S3 clock differs from the local clock; \
                             retrying with the server time"
                        );
                        if attempt + 1 < MAX_ATTEMPTS {
                            continue;
                        }
                        return Err(BackupError::S3(last_error));
                    }
                    if matches!(status, 408 | 425 | 429) || (500..600).contains(&status) {
                        last_error = format!(
                            "HTTP {status}: {}",
                            String::from_utf8_lossy(&bytes[..bytes.len().min(512)])
                        );
                        if attempt + 1 < MAX_ATTEMPTS {
                            let delay = 400u64.saturating_mul(1 << attempt).min(8_000);
                            std::thread::sleep(Duration::from_millis(delay));
                            continue;
                        }
                        return Err(BackupError::S3(last_error));
                    }
                    return Ok(S3Response {
                        status,
                        body: bytes,
                        headers: response_headers,
                    });
                }
                Err(e) => {
                    last_error = format!("network error: {e}");
                    if attempt + 1 < MAX_ATTEMPTS {
                        let delay = 400u64.saturating_mul(1 << attempt).min(8_000);
                        std::thread::sleep(Duration::from_millis(delay));
                        continue;
                    }
                }
            }
        }
        Err(BackupError::S3(last_error))
    }

    fn describe_error(&self, operation: &str, key: &str, status: u16, body: &[u8]) -> BackupError {
        let text = String::from_utf8_lossy(&body[..body.len().min(2048)]).to_string();
        BackupError::S3(format!(
            "{operation} {key} failed (HTTP {status}) at {}: {}",
            self.describe(),
            text.trim()
        ))
    }

    pub fn test_connection(&self) -> Result<()> {
        self.fetch_page("", None, 1)?;
        Ok(())
    }

    fn fetch_page(
        &self,
        prefix: &str,
        continuation: Option<&str>,
        max_keys: u32,
    ) -> Result<ListPage> {
        let mut query = vec![
            ("list-type".to_string(), "2".to_string()),
            ("max-keys".to_string(), max_keys.to_string()),
        ];
        if !prefix.is_empty() {
            query.push(("prefix".to_string(), prefix.to_string()));
        }
        if let Some(token) = continuation {
            query.push(("continuation-token".to_string(), token.to_string()));
        }
        let response = self.execute("GET", "", &query, None, &[])?;
        if response.status != 200 {
            return Err(self.describe_error("list", prefix, response.status, &response.body));
        }
        let text = String::from_utf8_lossy(&response.body);
        let parsed: ListBucketResult = quick_xml::de::from_str(&text)
            .map_err(|e| BackupError::S3(format!("parsing ListObjectsV2: {e}")))?;
        Ok(ListPage {
            objects: parsed
                .contents
                .into_iter()
                .map(|c| ObjectInfo {
                    key: c.key,
                    size: c.size,
                    etag: c.etag,
                })
                .collect(),
            next: parsed.next_continuation_token,
            is_truncated: parsed.is_truncated,
        })
    }
}

impl BlobStore for S3Store {
    fn list_page(
        &self,
        prefix: &str,
        continuation: Option<&str>,
        max_keys: u32,
    ) -> Result<ObjectPage> {
        let page = self.fetch_page(prefix, continuation, max_keys.clamp(1, 1000))?;
        Ok(ObjectPage {
            objects: page.objects,
            next: page.next.filter(|token| !token.is_empty()),
        })
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        let response = self.execute("PUT", key, &[], Some(data), &[])?;
        if !(200..300).contains(&response.status) {
            return Err(self.describe_error("PUT", key, response.status, &response.body));
        }
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        match self.get_opt(key)? {
            Some(data) => Ok(data),
            None => Err(BackupError::S3(format!(
                "object not found: {key} ({})",
                self.describe()
            ))),
        }
    }

    fn get_opt(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let response = self.execute("GET", key, &[], None, &[])?;
        match response.status {
            200 => Ok(Some(response.body)),
            404 => Ok(None),
            _ => Err(self.describe_error("GET", key, response.status, &response.body)),
        }
    }

    fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
        if length == 0 {
            return Ok(Vec::new());
        }
        let end = offset
            .checked_add(length)
            .and_then(|v| v.checked_sub(1))
            .ok_or_else(|| BackupError::Store("blob range overflow".into()))?;
        let headers = vec![("range".to_string(), format!("bytes={offset}-{end}"))];
        let response = self.execute("GET", key, &[], None, &headers)?;
        match response.status {
            206 => {
                if response.body.len() as u64 != length {
                    return Err(BackupError::S3(format!(
                        "GET {key} returned {} bytes for range {offset}..{end}, expected {length}",
                        response.body.len()
                    )));
                }
                Ok(response.body)
            }
            200 => {
                // Some S3-compatible endpoints ignore Range. Fall back to
                // slicing the full object rather than failing the restore.
                let start = offset as usize;
                let finish = start
                    .checked_add(length as usize)
                    .ok_or_else(|| BackupError::Store("blob range overflow".into()))?;
                if finish > response.body.len() {
                    return Err(self.describe_error(
                        "GET range",
                        key,
                        response.status,
                        &response.body,
                    ));
                }
                Ok(response.body[start..finish].to_vec())
            }
            404 => Err(BackupError::S3(format!(
                "object not found: {key} ({})",
                self.describe()
            ))),
            _ => Err(self.describe_error("GET range", key, response.status, &response.body)),
        }
    }

    fn list_first(&self, prefix: &str) -> Result<Option<ObjectInfo>> {
        let page = self.fetch_page(prefix, None, 1)?;
        Ok(page.objects.into_iter().next())
    }

    fn check_access(&self) -> Result<()> {
        self.test_connection()
    }

    fn delete_prefix(&self, prefix: &str) -> Result<usize> {
        // Page through the listing and delete each page in parallel. This keeps
        // memory flat and avoids S3Store::list's 1M-object safety cap, which
        // a full OS backup can easily exceed.
        let mut token: Option<String> = None;
        let mut deleted = 0usize;
        loop {
            let page = self.fetch_page(prefix, token.as_deref(), 1000)?;
            const WORKERS: usize = 8;
            for batch in page.objects.chunks(WORKERS) {
                let results: Vec<Result<()>> = std::thread::scope(|scope| {
                    let handles: Vec<_> = batch
                        .iter()
                        .map(|object| {
                            let key = object.key.clone();
                            scope.spawn(move || self.delete(&key))
                        })
                        .collect();
                    handles
                        .into_iter()
                        .map(|handle| {
                            handle.join().unwrap_or_else(|_| {
                                Err(BackupError::Store("delete worker panicked".into()))
                            })
                        })
                        .collect()
                });
                for result in results {
                    result?;
                }
                deleted += batch.len();
            }
            if !page.is_truncated {
                break;
            }
            match page.next {
                Some(next) if !next.is_empty() => token = Some(next),
                _ => break,
            }
        }
        Ok(deleted)
    }

    fn head(&self, key: &str) -> Result<Option<ObjectInfo>> {
        let response = self.execute("HEAD", key, &[], None, &[])?;
        match response.status {
            200 => Ok(Some(object_info_from_headers(key, &response.headers))),
            404 => Ok(None),
            _ => Err(self.describe_error("HEAD", key, response.status, &response.body)),
        }
    }

    fn delete(&self, key: &str) -> Result<()> {
        let response = self.execute("DELETE", key, &[], None, &[])?;
        if (200..300).contains(&response.status) || response.status == 404 {
            Ok(())
        } else {
            Err(self.describe_error("DELETE", key, response.status, &response.body))
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let page = self.fetch_page(prefix, token.as_deref(), 1000)?;
            out.extend(page.objects);
            if !page.is_truncated {
                break;
            }
            match page.next {
                Some(next) if !next.is_empty() => token = Some(next),
                _ => break,
            }
            if out.len() > 1_000_000 {
                return Err(BackupError::S3("listing exceeded 1,000,000 objects".into()));
            }
        }
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    fn describe(&self) -> String {
        format!("s3://{}/{}", self.config.bucket, self.config.region)
    }

    fn upload_limiter(&self) -> Option<Arc<UploadLimiter>> {
        self.throttle
            .as_ref()
            .map(|throttle| Arc::clone(&throttle.limiter))
    }
}

#[derive(Debug, Deserialize)]
struct ListBucketResult {
    #[serde(rename = "Contents", default)]
    contents: Vec<ListContents>,
    #[serde(rename = "NextContinuationToken", default)]
    next_continuation_token: Option<String>,
    #[serde(rename = "IsTruncated", default)]
    is_truncated: bool,
}

#[derive(Debug, Deserialize)]
struct ListContents {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "Size", default)]
    size: u64,
    #[serde(rename = "ETag", default)]
    etag: Option<String>,
}

struct ListPage {
    objects: Vec<ObjectInfo>,
    next: Option<String>,
    is_truncated: bool,
}

/// Build object metadata from a `HEAD` response. `Content-Length` is the
/// object size; returning a placeholder zero here makes callers reject valid
/// packed objects whose record references a non-zero byte range.
fn object_info_from_headers(key: &str, headers: &reqwest::header::HeaderMap) -> ObjectInfo {
    let size = headers
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(0);
    let etag = headers
        .get(reqwest::header::ETAG)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string());
    ObjectInfo {
        key: key.to_string(),
        size,
        etag,
    }
}

/// If `body` is S3's `RequestTimeTooSkewed` error, return the server time it
/// reported (falling back to the response `Date` header). Returns `None` for
/// any other error, so unrelated 403s never move the signing clock.
fn clock_skew_server_time(
    body: &[u8],
    response_date: Option<DateTime<Utc>>,
) -> Option<DateTime<Utc>> {
    let text = std::str::from_utf8(body).ok()?;
    if !text.contains("RequestTimeTooSkewed") {
        return None;
    }
    if let Some((_, rest)) = text.split_once("<ServerTime>")
        && let Some((value, _)) = rest.split_once("</ServerTime>")
        && let Ok(parsed) = DateTime::parse_from_rfc3339(value.trim())
    {
        return Some(parsed.with_timezone(&Utc));
    }
    response_date
}

fn canonical_query(query: &[(String, String)]) -> String {
    let mut pairs: Vec<(String, String)> = query
        .iter()
        .map(|(k, v)| (uri_encode(k, true), uri_encode(v, true)))
        .collect();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn uri_encode_path(path: &str) -> String {
    path.split('/')
        .map(|segment| uri_encode(segment, true))
        .collect::<Vec<_>>()
        .join("/")
}

/// RFC 3986 percent encoding, with unreserved characters left untouched.
pub fn uri_encode(input: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(input.len());
    for &b in input.as_bytes() {
        let unreserved = b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~');
        if unreserved || (b == b'/' && !encode_slash) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

pub fn new_store(config: &crate::core::model::AppConfig) -> Result<Box<dyn BlobStore>> {
    Ok(Box::new(S3Store::new(S3Config::from_app(config))?))
}

/// Build an S3 store whose PUT bodies are limited by a shared, live-updatable
/// upload throttle. Backups use this so Settings/Dashboard changes apply to the
/// current run; read-only commands can keep using [`new_store`].
pub fn new_store_with_upload_throttle(
    config: &crate::core::model::AppConfig,
    limiter: Arc<UploadLimiter>,
    cancel: CancelToken,
) -> Result<Box<dyn BlobStore>> {
    Ok(Box::new(
        S3Store::new(S3Config::from_app(config))?
            .with_upload_throttle(limiter, cancel),
    ))
}

/// Verify an object list looks like a backup set root (used by tests/debug UI).
pub fn contains_backup_config(objects: &[ObjectInfo], root: &str) -> bool {
    objects
        .iter()
        .any(|o| o.key == format!("{root}/backupconfig.json"))
}

pub fn normalize_key(root: &str, relative: &str) -> String {
    let relative = relative.trim_start_matches('/');
    format!("{}/{}", root.trim_end_matches('/'), relative)
}

pub fn parse_year_month(path: &Path) -> Option<(i32, u32)> {
    let name = path.file_name()?.to_str()?;
    let mut parts = name.split('-');
    let year = parts.next()?.parse().ok()?;
    let month = parts.next()?.parse().ok()?;
    Some((year, month))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_encoding() {
        assert_eq!(uri_encode("a b+c", true), "a%20b%2Bc");
        assert_eq!(uri_encode_path("/a b/c"), "/a%20b/c");
        assert_eq!(uri_encode("~._-", true), "~._-");
    }

    #[test]
    fn canonical_query_sorts_and_encodes() {
        let q = vec![
            ("prefix".to_string(), "a b".to_string()),
            ("list-type".to_string(), "2".to_string()),
        ];
        assert_eq!(canonical_query(&q), "list-type=2&prefix=a%20b");
    }

    #[test]
    fn parses_list_objects_v2_xml() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bucket</Name><Prefix>root/</Prefix><KeyCount>2</KeyCount><MaxKeys>1000</MaxKeys>
  <IsTruncated>true</IsTruncated><NextContinuationToken>tok</NextContinuationToken>
  <Contents><Key>root/a</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>&quot;e1&quot;</ETag><Size>12</Size><StorageClass>STANDARD</StorageClass></Contents>
  <Contents><Key>root/b</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>&quot;e2&quot;</ETag><Size>34</Size><StorageClass>STANDARD</StorageClass></Contents>
</ListBucketResult>"#;
        let parsed: ListBucketResult = quick_xml::de::from_str(xml).unwrap();
        assert!(parsed.is_truncated);
        assert_eq!(parsed.next_continuation_token.as_deref(), Some("tok"));
        assert_eq!(parsed.contents.len(), 2);
        assert_eq!(parsed.contents[0].key, "root/a");
        assert_eq!(parsed.contents[0].size, 12);
    }

    #[test]
    fn sigv4_matches_aws_reference_vector() {
        use chrono::TimeZone;
        let store = S3Store::new(S3Config {
            bucket: "test-bucket".into(),
            region: "us-east-1".into(),
            access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
        })
        .unwrap();
        let target = store.target("test.txt", &[]).unwrap();
        assert_eq!(target.host, "test-bucket.s3.us-east-1.amazonaws.com");
        let payload = crate::core::crypto::sha256_hex(b"hello");
        let when = chrono::Utc.with_ymd_and_hms(2013, 5, 24, 0, 0, 0).unwrap();
        let headers = store.sign_at("PUT", &target, &payload, &[], when).unwrap();
        let auth = headers
            .iter()
            .find(|(k, _)| k == "authorization")
            .map(|(_, v)| v.clone())
            .unwrap();
        // Generated with botocore's S3SigV4Auth using the same request,
        // credentials, region and fixed timestamp.
        assert_eq!(
            auth,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=d717e625459e73ca59996a42c07b4ac75570844f087bec7536dcd4bdde17b18f"
        );
    }

    #[test]
    fn head_metadata_uses_content_length() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_LENGTH,
            reqwest::header::HeaderValue::from_static("7818576"),
        );
        headers.insert(
            reqwest::header::ETAG,
            reqwest::header::HeaderValue::from_static("\"abc\""),
        );
        let info = object_info_from_headers("blobpacks/test.blobpack", &headers);
        assert_eq!(info.size, 7_818_576);
        assert_eq!(info.etag.as_deref(), Some("\"abc\""));
    }

    #[test]
    fn reads_server_time_from_clock_skew_error() {
        let body = br#"<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>RequestTimeTooSkewed</Code><Message>clock</Message>
<RequestTime>20261102T173223Z</RequestTime><ServerTime>2026-11-02T17:54:09Z</ServerTime>
<MaxAllowedSkewMilliseconds>900000</MaxAllowedSkewMilliseconds></Error>"#;
        let server = clock_skew_server_time(body, None).unwrap();
        assert_eq!(server.to_rfc3339(), "2026-11-02T17:54:09+00:00");
    }

    #[test]
    fn clock_skew_helper_ignores_unrelated_403_errors() {
        let body = b"<Error><Code>AccessDenied</Code><ServerTime>2026-11-02T17:54:09Z</ServerTime></Error>";
        assert!(clock_skew_server_time(body, None).is_none());
    }

    #[test]
    fn adopted_server_clock_is_used_for_signing() {
        let store = S3Store::new(S3Config {
            bucket: "test-bucket".into(),
            region: "us-east-1".into(),
            access_key_id: "key".into(),
            secret_access_key: "secret".into(),
        })
        .unwrap();
        let server = Utc::now() + chrono::Duration::seconds(1_800);
        assert!(store.adopt_server_clock(server));
        let corrected = store.signing_now();
        let drift = corrected.signed_duration_since(Utc::now()).num_seconds();
        assert!((drift - 1_800).abs() <= 5, "unexpected drift {drift}");
    }

    #[test]
    fn implausible_server_clock_is_rejected() {
        let store = S3Store::new(S3Config {
            bucket: "test-bucket".into(),
            region: "us-east-1".into(),
            access_key_id: "key".into(),
            secret_access_key: "secret".into(),
        })
        .unwrap();
        let far_future = Utc::now() + chrono::Duration::days(2);
        assert!(!store.adopt_server_clock(far_future));
        assert_eq!(
            store
                .signing_now()
                .signed_duration_since(Utc::now())
                .num_seconds()
                .abs(),
            0
        );
    }

    #[test]
    fn local_store_roundtrip() {
        let dir = std::env::temp_dir().join(format!("backup-local-test-{}", uuid::Uuid::new_v4()));
        let store = LocalStore::new(&dir).unwrap();
        store.put("a/b.txt", b"hello").unwrap();
        assert_eq!(store.get("a/b.txt").unwrap(), b"hello");
        assert_eq!(store.get_opt("a/missing").unwrap(), None);
        assert_eq!(store.list("a/").unwrap().len(), 1);
        assert!(store.list("a/").unwrap()[0].key.contains("a/b.txt"));
        store.delete("a/b.txt").unwrap();
        assert!(store.list("a/").unwrap().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn local_store_pages_listings_without_duplicates() {
        let dir = std::env::temp_dir().join(format!("backup-page-test-{}", uuid::Uuid::new_v4()));
        let store = LocalStore::new(&dir).unwrap();
        for i in 0..5 {
            store.put(&format!("pre/{i}.bin"), b"x").unwrap();
        }

        let mut seen = Vec::new();
        let mut token = None;
        loop {
            let page = store.list_page("pre/", token.as_deref(), 2).unwrap();
            seen.extend(page.objects.into_iter().map(|object| object.key));
            match page.next {
                Some(next) => token = Some(next),
                None => break,
            }
        }
        seen.sort();
        assert_eq!(
            seen,
            (0..5).map(|i| format!("pre/{i}.bin")).collect::<Vec<_>>()
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn local_range_and_delete_prefix() {
        let dir = std::env::temp_dir().join(format!("backup-range-test-{}", uuid::Uuid::new_v4()));
        let store = LocalStore::new(&dir).unwrap();
        store.put("a/one", b"0123456789").unwrap();
        store.put("a/two", b"abc").unwrap();
        assert_eq!(store.get_range("a/one", 2, 3).unwrap(), b"234");
        assert!(store.get_range("a/one", 8, 5).is_err());
        assert_eq!(store.delete_prefix("a/").unwrap(), 2);
        assert!(store.list("a/").unwrap().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn local_store_rejects_traversal() {
        let dir = std::env::temp_dir().join(format!("backup-local-test-{}", uuid::Uuid::new_v4()));
        let store = LocalStore::new(&dir).unwrap();
        assert!(store.put("../escape", b"x").is_err());
        std::fs::remove_dir_all(&dir).ok();
    }
}
