//! Rough storage cost estimates for the stored backup set.
//!
//! AWS bills Intelligent-Tiering per object and per access tier. A LIST request
//! only exposes the total stored bytes and object sizes, not the current access
//! tier of each object, so this module deliberately reports a range based on a
//! documented tier mix. B2 uses a flat rate per decimal TB. Both estimates use
//! the current blob index and exclude noncurrent versions and account-wide
//! allowances. They are planning aids, not provider bills.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::core::error::{BackupError, Result};
use crate::core::index::BlobIndex;
use crate::core::model::{AppConfig, StorageProvider};
use crate::core::record;
use crate::core::store::BlobStore;

/// AWS does not monitor/tier objects below this size.
pub const MIN_TIERED_OBJECT_SIZE: u64 = 128 * 1024;

/// S3 Intelligent-Tiering list prices in EU (Ireland), USD.
pub const IT_FREQUENT_PER_GB_MONTH: f64 = 0.023;
pub const IT_INFREQUENT_PER_GB_MONTH: f64 = 0.0125;
pub const IT_ARCHIVE_INSTANT_PER_GB_MONTH: f64 = 0.004;
/// Per monitored object per month ($0.0025 per 1,000 objects).
pub const IT_MONITORING_PER_OBJECT_MONTH: f64 = 0.000_002_5;

/// Assumed steady-state distribution of cold backup data.
pub const ASSUMED_FREQUENT: f64 = 0.10;
pub const ASSUMED_INFREQUENT: f64 = 0.20;
pub const ASSUMED_ARCHIVE_INSTANT: f64 = 0.70;

const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
const DECIMAL_TB: f64 = 1_000_000_000_000.0;

/// B2 pay-as-you-go storage price, USD per decimal TB held for 30 days.
/// Verified at https://www.backblaze.com/cloud-storage/pricing on 2026-11-03.
pub const B2_PER_TB_MONTH: f64 = 6.95;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageEstimate {
    pub storage_provider: StorageProvider,
    pub bytes: u64,
    pub objects: u64,
    pub monitored_objects: u64,
    pub prefix: String,
    pub region: String,
    /// AWS blends `ASSUMED_*` tiers and monitoring; B2 uses its flat storage rate.
    pub monthly_estimate: f64,
    /// AWS all-Archive Instant Access cost; B2 equals the flat estimate.
    pub monthly_low: f64,
    /// AWS all-Frequent Access cost; B2 equals the flat estimate.
    pub monthly_high: f64,
    /// AWS all-Frequent Access cost. Zero for B2 because access tiers do not apply.
    pub monthly_frequent: f64,
    /// All stored bytes at the Infrequent Access rate.
    pub monthly_infrequent: f64,
    /// All stored bytes at the Archive Instant Access rate.
    pub monthly_archive_instant: f64,
    /// Per-object monitoring and automation charge.
    pub monthly_monitoring: f64,
    pub assumed_tier_mix: String,
    pub pricing_basis: String,
    pub note: String,
    /// Logical bytes in the source for the current/last backup, if known.
    pub source_bytes: Option<u64>,
    /// Rough projected stored bytes once that source is fully backed up.
    pub projected_bytes_low: Option<u64>,
    pub projected_bytes_high: Option<u64>,
    /// Monthly cost at those projected sizes using this provider's pricing.
    pub projected_monthly_low: Option<f64>,
    pub projected_monthly_high: Option<f64>,
    pub projection_basis: String,
}

/// Projected stored size is shown as a range of the logical source size.
/// Dedupe, compression, sparse files and APFS clones can push the actual value
/// below the low end, so this is deliberately a planning aid, not a promise.
const PROJECTION_LOW: f64 = 0.5;
const PROJECTION_HIGH: f64 = 1.0;

impl StorageEstimate {
    /// Fill the projected fields from the logical size of the source tree.
    pub fn project_from_source(&mut self, source_bytes: u64) {
        if source_bytes == 0 {
            return;
        }
        let low = ((source_bytes as f64 * PROJECTION_LOW) as u64).max(self.bytes);
        let high = ((source_bytes as f64 * PROJECTION_HIGH) as u64).max(low);
        let low_objects = scale_count(self.objects, low, self.bytes);
        let high_objects = scale_count(self.objects, high, self.bytes);
        let low_monitored = scale_count(self.monitored_objects, low, self.bytes);
        let high_monitored = scale_count(self.monitored_objects, high, self.bytes);
        let low_est = estimate_for_provider(
            self.storage_provider,
            low,
            low_objects,
            low_monitored,
            self.prefix.clone(),
            self.region.clone(),
        );
        let high_est = estimate_for_provider(
            self.storage_provider,
            high,
            high_objects,
            high_monitored,
            self.prefix.clone(),
            self.region.clone(),
        );
        self.source_bytes = Some(source_bytes);
        self.projected_bytes_low = Some(low);
        self.projected_bytes_high = Some(high);
        self.projected_monthly_low = Some(low_est.monthly_estimate);
        self.projected_monthly_high = Some(high_est.monthly_estimate);
        let pricing_basis = match self.storage_provider {
            StorageProvider::AwsS3 => {
                "object counts scaled from the current index and the same assumed tier mix"
            }
            StorageProvider::BackblazeB2 => {
                "the same flat B2 storage rate before the account's free allowance"
            }
        };
        self.projection_basis = format!(
            "Rough projection: {:.0}–{:.0}% of the logical source size, with {pricing_basis}. \
             Actual stored size depends on dedupe, compression, sparse files and APFS clones.",
            PROJECTION_LOW * 100.0,
            PROJECTION_HIGH * 100.0
        );
    }
}

fn scale_count(count: u64, projected_bytes: u64, current_bytes: u64) -> u64 {
    if current_bytes == 0 {
        return count;
    }
    ((count as u128 * projected_bytes as u128) / current_bytes as u128) as u64
}

/// Estimate monthly storage from the blob index, optionally projecting the
/// final size from the logical source size of the current/last backup.
pub fn estimate_storage(
    config: &AppConfig,
    store: &dyn BlobStore,
    source_bytes: Option<u64>,
) -> Result<StorageEstimate> {
    let mut estimate = estimate_storage_current(config, store)?;
    if let Some(source_bytes) = source_bytes {
        estimate.project_from_source(source_bytes);
    }
    Ok(estimate)
}

fn estimate_storage_current(config: &AppConfig, store: &dyn BlobStore) -> Result<StorageEstimate> {
    let root = config.key_root();
    let index = BlobIndex::load(store, &root, config)?;
    if index.packs.is_empty() && index.blobs.is_empty() {
        let has_records =
            !record::list_record_keys(store, &root, &config.backup_set_uuid)?.is_empty();
        if has_records {
            return Err(BackupError::Store(
                "the blob index is missing for this backup set; run a backup to rebuild it".into(),
            ));
        }
        return Ok(estimate_for_provider(
            config.storage_provider,
            0,
            0,
            0,
            root,
            config.region.clone(),
        ));
    }
    let (bytes, objects, monitored_objects) = usage_from_index(&index);
    Ok(estimate_for_provider(
        config.storage_provider,
        bytes,
        objects,
        monitored_objects,
        root,
        config.region.clone(),
    ))
}

fn estimate_for_provider(
    storage_provider: StorageProvider,
    bytes: u64,
    objects: u64,
    monitored_objects: u64,
    prefix: String,
    region: String,
) -> StorageEstimate {
    match storage_provider {
        StorageProvider::AwsS3 => {
            estimate_from_usage(bytes, objects, monitored_objects, prefix, region)
        }
        StorageProvider::BackblazeB2 => estimate_b2_from_usage(bytes, objects, prefix, region),
    }
}

/// Count stored bytes and objects from the index. Pack objects are counted by
/// their uploaded size; unpacked tree/data objects are counted by their unique
/// object path.
fn usage_from_index(index: &BlobIndex) -> (u64, u64, u64) {
    let mut bytes = 0u64;
    let mut objects = 0u64;
    let mut monitored_objects = 0u64;

    for pack in index.packs.values() {
        bytes += pack.size;
        objects += 1;
        if pack.size >= MIN_TIERED_OBJECT_SIZE {
            monitored_objects += 1;
        }
    }

    let mut standalone: BTreeMap<&str, u64> = BTreeMap::new();
    for loc in index.blobs.values().filter(|loc| !loc.is_packed) {
        standalone
            .entry(loc.relative_path.as_str())
            .and_modify(|length| *length = (*length).max(loc.length))
            .or_insert(loc.length);
    }
    for length in standalone.into_values() {
        bytes += length;
        objects += 1;
        if length >= MIN_TIERED_OBJECT_SIZE {
            monitored_objects += 1;
        }
    }

    (bytes, objects, monitored_objects)
}

/// Compute an estimate from already-known usage. Exposed for tests and for
/// callers that obtain usage from another source.
pub fn estimate_from_usage(
    bytes: u64,
    objects: u64,
    monitored_objects: u64,
    prefix: impl Into<String>,
    region: impl Into<String>,
) -> StorageEstimate {
    let region = region.into();
    let gb = bytes as f64 / GIB;
    let monthly_frequent = gb * IT_FREQUENT_PER_GB_MONTH;
    let monthly_infrequent = gb * IT_INFREQUENT_PER_GB_MONTH;
    let monthly_archive_instant = gb * IT_ARCHIVE_INSTANT_PER_GB_MONTH;
    let monthly_monitoring = monitored_objects as f64 * IT_MONITORING_PER_OBJECT_MONTH;
    let blended = gb
        * (ASSUMED_FREQUENT * IT_FREQUENT_PER_GB_MONTH
            + ASSUMED_INFREQUENT * IT_INFREQUENT_PER_GB_MONTH
            + ASSUMED_ARCHIVE_INSTANT * IT_ARCHIVE_INSTANT_PER_GB_MONTH);

    StorageEstimate {
        storage_provider: StorageProvider::AwsS3,
        bytes,
        objects,
        monitored_objects,
        prefix: prefix.into(),
        region: region.clone(),
        monthly_estimate: blended + monthly_monitoring,
        monthly_low: monthly_archive_instant + monthly_monitoring,
        monthly_high: monthly_frequent + monthly_monitoring,
        monthly_frequent,
        monthly_infrequent,
        monthly_archive_instant,
        monthly_monitoring,
        assumed_tier_mix:
            "10% Frequent Access / 20% Infrequent Access / 70% Archive Instant Access".to_string(),
        pricing_basis: "S3 Intelligent-Tiering list prices for EU (Ireland), USD".to_string(),
        source_bytes: None,
        projected_bytes_low: None,
        projected_bytes_high: None,
        projected_monthly_low: None,
        projected_monthly_high: None,
        projection_basis: String::new(),
        note: if region.eq_ignore_ascii_case("eu-west-1") {
            "Based on the blob index; excludes small metadata objects, requests, data transfer, noncurrent versions and taxes."
                .to_string()
        } else {
            format!(
                "Configured region is {region}; this estimate uses EU (Ireland) list prices, so your AWS bill may differ. Based on the blob index; excludes small metadata objects, requests, data transfer, noncurrent versions and taxes."
            )
        },
    }
}

/// Estimate the gross cost of keeping the indexed current objects for 30 days.
/// B2 bills actual byte-hours. A single backup-set index cannot allocate the
/// account-wide free allowance or reveal hidden versions, so neither adjusts
/// this estimate. AWS tier-specific fields are zero because they do not apply.
pub fn estimate_b2_from_usage(
    bytes: u64,
    objects: u64,
    prefix: impl Into<String>,
    region: impl Into<String>,
) -> StorageEstimate {
    let monthly_estimate = bytes as f64 / DECIMAL_TB * B2_PER_TB_MONTH;
    StorageEstimate {
        storage_provider: StorageProvider::BackblazeB2,
        bytes,
        objects,
        monitored_objects: 0,
        prefix: prefix.into(),
        region: region.into(),
        monthly_estimate,
        monthly_low: monthly_estimate,
        monthly_high: monthly_estimate,
        monthly_frequent: 0.0,
        monthly_infrequent: 0.0,
        monthly_archive_instant: 0.0,
        monthly_monitoring: 0.0,
        assumed_tier_mix: "Always-hot B2 storage; no access tiers or per-object monitoring".into(),
        pricing_basis: "Backblaze B2 pay-as-you-go: $6.95 per decimal TB per 30 days, USD".into(),
        note: "Gross storage estimate from the current blob index, before the account-wide first 10 GB free allowance. Assumes these bytes remain stored for 30 days; actual B2 billing uses byte-hours. Excludes small metadata objects, hidden/noncurrent versions, data transfer, account discounts and taxes.".into(),
        source_bytes: None,
        projected_bytes_low: None,
        projected_bytes_high: None,
        projected_monthly_low: None,
        projected_monthly_high: None,
        projection_basis: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::store::LocalStore;

    fn close(a: f64, b: f64) {
        assert!((a - b).abs() < 0.000_001, "{a} != {b}");
    }

    #[test]
    fn one_tebibyte_rates_and_blend() {
        let bytes = 1024 * 1024 * 1024 * 1024;
        let estimate = estimate_from_usage(bytes, 1_000, 1_000, "prefix/set", "eu-west-1");
        close(estimate.monthly_frequent, 1024.0 * 0.023);
        close(estimate.monthly_infrequent, 1024.0 * 0.0125);
        close(estimate.monthly_archive_instant, 1024.0 * 0.004);
        close(estimate.monthly_monitoring, 1_000.0 * 0.000_002_5);
        close(
            estimate.monthly_estimate,
            1024.0 * (0.10 * 0.023 + 0.20 * 0.0125 + 0.70 * 0.004) + 1_000.0 * 0.000_002_5,
        );
        assert!(estimate.monthly_low < estimate.monthly_estimate);
        assert!(estimate.monthly_estimate < estimate.monthly_high);
    }

    #[test]
    fn small_objects_are_not_monitored() {
        let estimate = estimate_from_usage(1_000, 10, 0, "p", "eu-west-1");
        close(estimate.monthly_monitoring, 0.0);
    }

    #[test]
    fn b2_flat_rate_uses_decimal_bytes_before_account_allowances() {
        let estimate =
            estimate_b2_from_usage(1_000_000_000_000, 1_000_000, "prefix/set", "us-west-004");
        assert_eq!(estimate.storage_provider, StorageProvider::BackblazeB2);
        close(estimate.monthly_estimate, 6.95);
        close(estimate.monthly_low, 6.95);
        close(estimate.monthly_high, 6.95);
        assert_eq!(estimate.monitored_objects, 0);
        close(estimate.monthly_monitoring, 0.0);
        close(estimate.monthly_frequent, 0.0);
        close(estimate.monthly_infrequent, 0.0);
        close(estimate.monthly_archive_instant, 0.0);

        let small = estimate_b2_from_usage(1_000_000_000, 1, "prefix/set", "us-west-004");
        // The account may consume its free allowance in another bucket or set.
        close(small.monthly_estimate, 0.00695);
        let empty = estimate_b2_from_usage(0, 0, "prefix/set", "us-west-004");
        close(empty.monthly_estimate, 0.0);
    }

    #[test]
    fn b2_projection_uses_its_flat_rate_and_never_shrinks_current_usage() {
        let mut estimate =
            estimate_b2_from_usage(200_000_000_000, 200, "prefix/set", "us-west-004");
        estimate.project_from_source(1_000_000_000_000);
        assert_eq!(estimate.projected_bytes_low, Some(500_000_000_000));
        assert_eq!(estimate.projected_bytes_high, Some(1_000_000_000_000));
        close(estimate.projected_monthly_low.unwrap(), 3.475);
        close(estimate.projected_monthly_high.unwrap(), 6.95);

        estimate.project_from_source(10_000_000_000);
        assert_eq!(estimate.projected_bytes_low, Some(estimate.bytes));
        assert_eq!(estimate.projected_bytes_high, Some(estimate.bytes));
        close(estimate.projected_monthly_low.unwrap(), 1.39);
        close(estimate.projected_monthly_high.unwrap(), 1.39);
    }

    #[test]
    fn index_usage_counts_packs_once_and_standalone_objects_by_path() {
        let mut index = BlobIndex::default();
        index.add_pack("blobpacks/a".into(), 200_000);
        index.add_pack("blobpacks/b".into(), 300_000);
        index.add_blob(standalone_loc("tree", "treepacks/tree", 100));
        // Two locs in the same standalone object must not be double-counted.
        index.add_blob(standalone_loc("tree-2", "treepacks/tree", 400_000));
        let (bytes, objects, monitored) = usage_from_index(&index);
        assert_eq!(bytes, 200_000 + 300_000 + 400_000);
        assert_eq!(objects, 3);
        assert_eq!(monitored, 3);
    }

    fn standalone_loc(id: &str, path: &str, length: u64) -> crate::core::model::BlobLoc {
        crate::core::model::BlobLoc {
            blob_identifier: id.into(),
            is_packed: false,
            relative_path: path.into(),
            offset: 0,
            length,
            stretch_encryption_key: false,
            compression_type: crate::core::model::COMPRESSION_LZ4,
        }
    }

    #[test]
    fn estimate_storage_reads_the_remote_index() {
        let dir = std::env::temp_dir().join(format!("cost-test-{}", uuid::Uuid::new_v4()));
        let store = LocalStore::new(&dir).unwrap();
        let config = AppConfig {
            bucket: "bucket".into(),
            prefix: "qia-backups".into(),
            backup_set_uuid: "set".into(),
            ..AppConfig::default()
        };
        let root = config.key_root();
        let mut index = BlobIndex::default();
        index.add_pack("blobpacks/a".into(), 4096);
        let json = serde_json::to_vec(&index).unwrap();
        let compressed = crate::core::compression::lz4_compress_with_prefix(&json);
        store
            .put(&BlobIndex::remote_key(&root), &compressed)
            .unwrap();
        let estimate = estimate_storage(&config, &store, None).unwrap();
        assert_eq!(estimate.bytes, 4096);
        assert_eq!(estimate.objects, 1);
        assert_eq!(estimate.monitored_objects, 0);
        assert!(estimate.projected_bytes_low.is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn b2_storage_estimate_reads_remote_index_and_projects_at_b2_rates() {
        let uuid = uuid::Uuid::new_v4();
        let dir = std::env::temp_dir().join(format!("cost-b2-test-{uuid}"));
        let store = LocalStore::new(&dir).unwrap();
        let config = AppConfig {
            storage_provider: StorageProvider::BackblazeB2,
            endpoint: "https://s3.us-west-004.backblazeb2.com".into(),
            bucket: "cost-test-bucket".into(),
            region: "us-west-004".into(),
            backup_set_uuid: uuid.to_string(),
            ..AppConfig::default()
        };
        let root = config.key_root();
        let empty = estimate_storage(&config, &store, None).unwrap();
        assert_eq!(empty.storage_provider, StorageProvider::BackblazeB2);
        close(empty.monthly_estimate, 0.0);

        let mut index = BlobIndex::default();
        index.add_pack("blobpacks/a".into(), 400_000_000_000);
        index.add_blob(standalone_loc("tree", "treepacks/tree", 100_000_000_000));
        let json = serde_json::to_vec(&index).unwrap();
        let compressed = crate::core::compression::lz4_compress_with_prefix(&json);
        store
            .put(&BlobIndex::remote_key(&root), &compressed)
            .unwrap();
        let estimate = estimate_storage(&config, &store, Some(2_000_000_000_000)).unwrap();
        assert_eq!(estimate.storage_provider, StorageProvider::BackblazeB2);
        assert_eq!(estimate.bytes, 500_000_000_000);
        assert_eq!(estimate.objects, 2);
        assert_eq!(estimate.monitored_objects, 0);
        close(estimate.monthly_estimate, 3.475);
        close(estimate.projected_monthly_low.unwrap(), 6.95);
        close(estimate.projected_monthly_high.unwrap(), 13.9);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn projection_tracks_source_size_and_stays_ordered() {
        let mut estimate =
            estimate_from_usage(1024 * 1024 * 1024, 1_000, 100, "prefix/set", "eu-west-1");
        estimate.project_from_source(4 * 1024 * 1024 * 1024);
        assert_eq!(estimate.source_bytes, Some(4 * 1024 * 1024 * 1024));
        let low = estimate.projected_bytes_low.unwrap();
        let high = estimate.projected_bytes_high.unwrap();
        assert_eq!(low, 2 * 1024 * 1024 * 1024);
        assert_eq!(high, 4 * 1024 * 1024 * 1024);
        assert!(
            estimate.projected_monthly_low.unwrap() <= estimate.projected_monthly_high.unwrap()
        );
        assert!(!estimate.projection_basis.is_empty());
    }
}
