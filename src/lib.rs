//! Qia Backup: a Time Machine / APFS-snapshot backup engine for AWS S3 and Backblaze B2.
//!
//! The data format uses a content-addressed backup-set layout: LZ4 blob
//! packs, SHA-256 blob identifiers, buzhash content-defined chunking, binary
//! node/tree metadata, and optional client-side encryption.

pub mod cli;
pub mod core;

pub use core::error::{BackupError, Result};
