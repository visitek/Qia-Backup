//! Core backup engine (cross-platform). The Tauri desktop shell lives in
//! `app.rs` and is compiled only on macOS.

pub mod chunker;
pub mod codec;
pub mod compression;
pub mod config;
pub mod content;
pub mod cost;
pub mod crypto;
pub mod diff;
pub mod edit;
pub mod engine;
pub mod error;
pub mod index;
pub mod logging;
pub mod model;
pub mod partial;
pub mod power;
pub mod preflight;
pub mod record;
pub mod recover;
pub mod restore;
pub mod retention;
pub mod scheduler;
pub mod secrets;
pub mod settings;
pub mod snapshot;
pub mod status;
pub mod store;
pub mod upload;
pub mod versions;

pub use error::{BackupError, Result};
