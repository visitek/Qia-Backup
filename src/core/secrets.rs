//! Storage for long-lived secrets.
//!
//! On macOS the storage secret access key and the per-backup-set encryption
//! password live in the login Keychain; `config.json` only contains
//! non-sensitive settings. Other platforms keep the historical config-file
//! behaviour so the CLI still builds and runs there.
//!
//! The Keychain service matches the Tauri bundle identifier in
//! `src-tauri/tauri.conf.json`. Entries from the previous bundle identifier are
//! read and migrated once so an existing AWS key and encryption password are
//! not lost by the rename.

use crate::core::error::Result;
use crate::core::model::{AppConfig, StorageProvider};

pub const KEYCHAIN_SERVICE: &str = "com.quantaia.backup";
#[cfg(target_os = "macos")]
const LEGACY_KEYCHAIN_SERVICES: &[&str] = &["com.photominds.backup"];

/// Password ownership includes the destination and set, independently of the
/// access key used to reach it. Rotating a key must not lose the set password.
fn encryption_account(config: &AppConfig) -> String {
    let provider = match config.storage_provider {
        StorageProvider::AwsS3 => "aws_s3",
        StorageProvider::BackblazeB2 => "backblaze_b2",
    };
    let identity = format!(
        "{}|{}|{}|{}|{}",
        provider,
        config.endpoint.trim().trim_end_matches('/'),
        config.bucket.trim(),
        config.region.trim(),
        config.key_root()
    );
    format!(
        "encryption-password:{provider}:{}",
        crate::core::crypto::sha256_hex(identity.as_bytes())
    )
}

#[cfg(any(target_os = "macos", test))]
fn legacy_encryption_account(backup_set_uuid: &str) -> String {
    format!("encryption-password:{backup_set_uuid}")
}

#[derive(Default)]
pub struct StoredSecrets {
    pub secret_access_key: Option<String>,
    pub encryption_password: Option<String>,
}

fn non_empty(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn changed(previous: &str, next: &str) -> Option<String> {
    if !next.is_empty() && previous != next {
        Some(next.to_string())
    } else {
        None
    }
}

#[cfg(target_os = "macos")]
mod imp {
    use super::*;
    use crate::core::error::BackupError;
    use security_framework::passwords::{
        PasswordOptions, delete_generic_password, generic_password, set_generic_password,
    };

    // errSecItemNotFound from Security/SecBase.h.
    const ERR_SEC_ITEM_NOT_FOUND: i32 = -25300;

    fn read_service(service: &str, account: &str) -> Result<Option<String>> {
        match generic_password(PasswordOptions::new_generic_password(service, account)) {
            Ok(bytes) => String::from_utf8(bytes)
                .map(Some)
                .map_err(|_| BackupError::Config("Keychain entry is not valid UTF-8".into())),
            Err(e) if e.code() == ERR_SEC_ITEM_NOT_FOUND => Ok(None),
            Err(e) => Err(BackupError::Config(format!(
                "could not read macOS Keychain entry {account:?}: {e}"
            ))),
        }
    }

    fn get(account: &str) -> Result<Option<String>> {
        if let Some(value) = read_service(KEYCHAIN_SERVICE, account)? {
            return Ok(Some(value));
        }
        for service in LEGACY_KEYCHAIN_SERVICES {
            if let Some(value) = read_service(service, account)? {
                // Do not remove an on-disk plaintext copy unless migration
                // into the current service actually succeeds.
                set(account, &value)?;
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    fn set(account: &str, value: &str) -> Result<()> {
        set_generic_password(KEYCHAIN_SERVICE, account, value.as_bytes()).map_err(|e| {
            BackupError::Config(format!(
                "could not store macOS Keychain entry {account:?}: {e}"
            ))
        })
    }

    fn delete_service(service: &str, account: &str) -> Result<()> {
        match delete_generic_password(service, account) {
            Ok(()) => Ok(()),
            Err(e) if e.code() == ERR_SEC_ITEM_NOT_FOUND => Ok(()),
            Err(e) => Err(BackupError::Config(format!(
                "could not delete macOS Keychain entry {account:?}: {e}"
            ))),
        }
    }

    fn delete(account: &str) -> Result<()> {
        delete_service(KEYCHAIN_SERVICE, account)?;
        for service in LEGACY_KEYCHAIN_SERVICES {
            delete_service(service, account)?;
        }
        Ok(())
    }

    pub fn load(secret_account: &str, password_account: &str) -> Result<StoredSecrets> {
        Ok(StoredSecrets {
            secret_access_key: get(secret_account)?,
            encryption_password: get(password_account)?,
        })
    }

    pub fn legacy_password(backup_set_uuid: &str) -> Result<Option<String>> {
        get(&legacy_encryption_account(backup_set_uuid))
    }

    pub fn store(
        secret_account: &str,
        password_account: &str,
        secrets: &StoredSecrets,
    ) -> Result<()> {
        if let Some(value) = &secrets.secret_access_key {
            set(secret_account, value)?;
        }
        if let Some(value) = &secrets.encryption_password {
            set(password_account, value)?;
        }
        Ok(())
    }

    pub fn delete_encryption_password(password_account: &str) -> Result<()> {
        delete(password_account)
    }
}

#[cfg(not(target_os = "macos"))]
mod imp {
    use super::*;

    pub fn load(_secret_account: &str, _password_account: &str) -> Result<StoredSecrets> {
        Ok(StoredSecrets::default())
    }

    pub fn legacy_password(_backup_set_uuid: &str) -> Result<Option<String>> {
        Ok(None)
    }

    pub fn store(
        _secret_account: &str,
        _password_account: &str,
        _secrets: &StoredSecrets,
    ) -> Result<()> {
        Ok(())
    }

    pub fn delete_encryption_password(_password_account: &str) -> Result<()> {
        Ok(())
    }
}

/// B2 keys have their own account, scoped to the validated endpoint and key ID.
/// The existing AWS account remains unchanged for older installations.
fn secret_account(config: &AppConfig) -> String {
    match config.storage_provider {
        crate::core::model::StorageProvider::AwsS3 => "aws-secret-access-key".into(),
        crate::core::model::StorageProvider::BackblazeB2 => {
            let identity = format!(
                "{}|{}",
                config.endpoint.trim().trim_end_matches('/'),
                config.access_key_id.trim()
            );
            format!(
                "b2-application-key:{}",
                crate::core::crypto::sha256_hex(identity.as_bytes())
            )
        }
    }
}

/// Load only this destination's owned accounts. Legacy UUID-only passwords
/// are deliberately unavailable here, even when two providers share a UUID.
pub fn load_for(config: &AppConfig) -> Result<StoredSecrets> {
    imp::load(&secret_account(config), &encryption_account(config))
}

/// Called only while migrating a single-profile config.json. That document
/// identifies the sole owner to which a UUID-only legacy password may migrate;
/// ordinary multi-profile loads must never use this compatibility path.
pub(crate) fn load_legacy_for(config: &AppConfig) -> Result<StoredSecrets> {
    let mut stored = load_for(config)?;
    if stored.encryption_password.is_none() {
        stored.encryption_password = imp::legacy_password(&config.backup_set_uuid)?;
    }
    Ok(stored)
}

/// Store every non-empty secret from `config` in the Keychain.
pub fn store(config: &AppConfig) -> Result<()> {
    imp::store(
        &secret_account(config),
        &encryption_account(config),
        &StoredSecrets {
            secret_access_key: non_empty(&config.secret_access_key),
            encryption_password: non_empty(&config.encryption_password),
        },
    )
}

fn changed_secrets(previous: &AppConfig, next: &AppConfig) -> StoredSecrets {
    StoredSecrets {
        secret_access_key: if secret_account(previous) == secret_account(next) {
            changed(&previous.secret_access_key, &next.secret_access_key)
        } else {
            non_empty(&next.secret_access_key)
        },
        encryption_password: if encryption_account(previous) == encryption_account(next) {
            changed(&previous.encryption_password, &next.encryption_password)
        } else {
            non_empty(&next.encryption_password)
        },
    }
}

/// Store changed secrets, including equal values moved to a new owning
/// destination. Empty fields never erase existing Keychain values.
pub fn store_changed(previous: &AppConfig, next: &AppConfig) -> Result<()> {
    imp::store(
        &secret_account(next),
        &encryption_account(next),
        &changed_secrets(previous, next),
    )
}

/// Remove only the encryption password owned by this exact destination/set.
pub fn delete_encryption_password_for(config: &AppConfig) -> Result<()> {
    imp::delete_encryption_password(&encryption_account(config))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::model::StorageProvider;

    #[test]
    fn b2_keychain_identity_is_separate_from_aws_and_other_b2_keys() {
        let aws = AppConfig::default();
        assert_eq!(secret_account(&aws), "aws-secret-access-key");
        let b2 = AppConfig {
            storage_provider: StorageProvider::BackblazeB2,
            endpoint: "https://s3.us-west-004.backblazeb2.com".into(),
            access_key_id: "key-one".into(),
            ..aws.clone()
        };
        let another = AppConfig {
            access_key_id: "key-two".into(),
            ..b2.clone()
        };
        assert_ne!(secret_account(&aws), secret_account(&b2));
        assert_ne!(secret_account(&b2), secret_account(&another));
        let canonical = AppConfig {
            endpoint: format!(" {}/ ", b2.endpoint),
            ..b2.clone()
        };
        assert_eq!(secret_account(&b2), secret_account(&canonical));
    }

    #[test]
    fn same_uuid_passwords_are_owned_by_provider_and_complete_destination() {
        let aws = AppConfig {
            bucket: "aws-bucket".into(),
            prefix: "aws-prefix".into(),
            encryption_password: "synthetic-password".into(),
            ..AppConfig::default()
        };
        let b2 = AppConfig {
            storage_provider: StorageProvider::BackblazeB2,
            endpoint: "https://s3.us-west-004.backblazeb2.com".into(),
            region: "us-west-004".into(),
            bucket: "b2-bucket".into(),
            access_key_id: "b2-key".into(),
            ..aws.clone()
        };
        let aws_account = encryption_account(&aws);
        let b2_account = encryption_account(&b2);
        assert_ne!(aws_account, b2_account);
        assert_ne!(aws_account, legacy_encryption_account(&aws.backup_set_uuid));
        assert_ne!(b2_account, legacy_encryption_account(&b2.backup_set_uuid));
        for changed_destination in [
            AppConfig {
                bucket: "another-bucket".into(),
                ..b2.clone()
            },
            AppConfig {
                prefix: "another-prefix".into(),
                ..b2.clone()
            },
            AppConfig {
                backup_set_uuid: uuid::Uuid::new_v4().to_string(),
                ..b2.clone()
            },
            AppConfig {
                region: "us-east-005".into(),
                endpoint: "https://s3.us-east-005.backblazeb2.com".into(),
                ..b2.clone()
            },
        ] {
            assert_ne!(b2_account, encryption_account(&changed_destination));
        }
        let rotated_key = AppConfig {
            access_key_id: "rotated-key".into(),
            ..b2.clone()
        };
        assert_eq!(b2_account, encryption_account(&rotated_key));
        let canonical_endpoint = AppConfig {
            endpoint: format!(" {}/ ", b2.endpoint),
            ..b2.clone()
        };
        assert_eq!(b2_account, encryption_account(&canonical_endpoint));
    }

    #[test]
    fn moving_equal_password_values_to_another_destination_still_stores_them() {
        let aws = AppConfig {
            bucket: "aws-bucket".into(),
            encryption_password: "same-synthetic-password".into(),
            ..AppConfig::default()
        };
        let b2 = AppConfig {
            storage_provider: StorageProvider::BackblazeB2,
            endpoint: "https://s3.us-west-004.backblazeb2.com".into(),
            bucket: "b2-bucket".into(),
            ..aws.clone()
        };
        assert!(changed_secrets(&aws, &aws).encryption_password.is_none());
        assert_eq!(
            changed_secrets(&aws, &b2).encryption_password.as_deref(),
            Some("same-synthetic-password")
        );
        let blank = AppConfig {
            encryption_password: String::new(),
            ..b2.clone()
        };
        assert!(changed_secrets(&aws, &blank).encryption_password.is_none());
    }
}
