//! One versioned settings document keeps independent AWS and B2 backup plans.
//! Each slot owns its complete configuration, including its backup-set UUID,
//! source selection, retention, schedule and last run. A missing slot is an
//! unconfigured destination; selecting it must never copy another slot's plan.

use std::path::Path;

use serde::{Deserialize, Deserializer, Serialize};

use crate::core::error::{BackupError, IoContext, Result};
use crate::core::model::{AppConfig, StorageProvider};

pub const SETTINGS_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AppSettings {
    pub settings_version: u32,
    pub selected_provider: StorageProvider,
    // Require both slot fields in the document. Explicit null means the
    // destination has not been configured; a misspelled/missing slot is invalid.
    #[serde(deserialize_with = "deserialize_profile")]
    pub aws_s3: Option<AppConfig>,
    #[serde(deserialize_with = "deserialize_profile")]
    pub backblaze_b2: Option<AppConfig>,
}

fn deserialize_profile<'de, D>(deserializer: D) -> std::result::Result<Option<AppConfig>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<AppConfig>::deserialize(deserializer)
}

impl Default for AppSettings {
    fn default() -> Self {
        Self {
            settings_version: SETTINGS_VERSION,
            selected_provider: StorageProvider::AwsS3,
            aws_s3: None,
            backblaze_b2: None,
        }
    }
}

impl AppSettings {
    pub fn profile(&self, provider: StorageProvider) -> Option<&AppConfig> {
        match provider {
            StorageProvider::AwsS3 => self.aws_s3.as_ref(),
            StorageProvider::BackblazeB2 => self.backblaze_b2.as_ref(),
        }
    }

    pub fn selected_profile(&self) -> Option<&AppConfig> {
        self.profile(self.selected_provider)
    }

    /// Replace only the supplied provider's plan and select that destination.
    pub fn with_profile(&self, config: &AppConfig) -> Self {
        match config.storage_provider {
            StorageProvider::AwsS3 => Self {
                selected_provider: config.storage_provider,
                aws_s3: Some(config.clone()),
                ..self.clone()
            },
            StorageProvider::BackblazeB2 => Self {
                selected_provider: config.storage_provider,
                backblaze_b2: Some(config.clone()),
                ..self.clone()
            },
        }
    }

    /// Validate the persisted shape without requiring secrets, a mounted
    /// source, or connectivity. Those are checked before an operation starts.
    pub fn validate(&self) -> Result<()> {
        if self.settings_version != SETTINGS_VERSION {
            return Err(BackupError::Config(format!(
                "unsupported settingsVersion {}; this app supports {SETTINGS_VERSION}",
                self.settings_version
            )));
        }
        for (provider, profile) in [
            (StorageProvider::AwsS3, self.aws_s3.as_ref()),
            (StorageProvider::BackblazeB2, self.backblaze_b2.as_ref()),
        ] {
            if let Some(profile) = profile {
                if profile.storage_provider != provider {
                    return Err(BackupError::Config(format!(
                        "saved {provider:?} profile contains storageProvider {:?}; move it to its matching settings slot",
                        profile.storage_provider
                    )));
                }
                if profile.version != 1 {
                    return Err(BackupError::Config(format!(
                        "saved {provider:?} profile uses unsupported version {}; this app supports 1",
                        profile.version
                    )));
                }
                if profile.backup_set_uuid.trim().is_empty() {
                    return Err(BackupError::Config(format!(
                        "saved {provider:?} profile is missing its backup-set UUID"
                    )));
                }
            }
        }
        if self.selected_profile().is_none()
            && (self.aws_s3.is_some() || self.backblaze_b2.is_some())
        {
            return Err(BackupError::Config(format!(
                "selectedProvider {:?} has no saved profile; select a configured destination",
                self.selected_provider
            )));
        }
        Ok(())
    }
}

/// Create an independent draft. A B2 region must be entered explicitly, and
/// new drafts never schedule a backup before their settings have been saved.
pub fn new_profile(provider: StorageProvider) -> AppConfig {
    AppConfig {
        storage_provider: provider,
        region: match provider {
            StorageProvider::AwsS3 => crate::core::model::DEFAULT_REGION.to_string(),
            StorageProvider::BackblazeB2 => String::new(),
        },
        schedule_minutes: 0,
        ..AppConfig::recommended()
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
pub(crate) enum SettingsDocument {
    Current(AppSettings),
    Legacy(AppConfig),
}

pub(crate) fn read_document(path: &Path) -> Result<Option<SettingsDocument>> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(BackupError::io(path, error)),
    };
    let document: SettingsDocument = serde_json::from_slice(&bytes).map_err(|error| {
        BackupError::Config(format!(
            "could not read settings at {}: {error}; expected settingsVersion, selectedProvider, awsS3 and backblazeB2, or a legacy single profile",
            path.display()
        ))
    })?;
    match &document {
        SettingsDocument::Current(settings) => settings.validate()?,
        SettingsDocument::Legacy(config) => {
            AppSettings::default().with_profile(config).validate()?
        }
    }
    Ok(Some(document))
}

/// Atomically publish one settings document. The uniquely named temporary file
/// is exclusive and private; an unsuccessful write never truncates the old
/// settings or follows a pre-existing temporary-file symlink.
pub(crate) fn write_document(path: &Path, settings: &AppSettings) -> Result<()> {
    settings.validate()?;
    let bytes = serde_json::to_vec_pretty(settings)?;
    let parent = path
        .parent()
        .ok_or_else(|| BackupError::Config("settings path must have a parent directory".into()))?;
    std::fs::create_dir_all(parent).at(parent)?;
    let temporary = parent.join(format!(".config-{}.tmp", uuid::Uuid::new_v4()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    // Cleanup is permitted only after exclusive creation succeeded.
    let mut file = options.open(&temporary).at(&temporary)?;
    let outcome = (|| -> Result<()> {
        use std::io::Write;
        file.write_all(&bytes).at(&temporary)?;
        file.sync_all().at(&temporary)?;
        Ok(())
    })();
    drop(file);
    let outcome = outcome.and_then(|()| std::fs::rename(&temporary, path).at(path));
    if outcome.is_err() {
        match std::fs::remove_file(&temporary) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => crate::core::logging::warn(format!(
                "could not remove temporary settings file {}: {error}",
                temporary.display()
            )),
        }
    }
    outcome
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn temporary_directory() -> PathBuf {
        let directory = std::env::temp_dir().join(format!("qia-settings-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        directory
    }

    fn profile(provider: StorageProvider) -> AppConfig {
        AppConfig {
            storage_provider: provider,
            backup_set_name: format!("{provider:?} independent plan"),
            bucket: format!("{provider:?}-test-bucket"),
            region: match provider {
                StorageProvider::AwsS3 => "eu-west-1".into(),
                StorageProvider::BackblazeB2 => "us-west-004".into(),
            },
            endpoint: match provider {
                StorageProvider::AwsS3 => String::new(),
                StorageProvider::BackblazeB2 => "https://s3.us-west-004.backblazeb2.com".into(),
            },
            prefix: format!("{provider:?}/private-backups"),
            access_key_id: format!("{provider:?}-key-id"),
            source_kind: "path".into(),
            source_path: format!("/synthetic/{provider:?}"),
            included_folders: vec![format!("{provider:?}/Documents")],
            exclude_patterns: vec!["**/dev".into(), "**/custom.ignore".into()],
            schedule_minutes: match provider {
                StorageProvider::AwsS3 => 45,
                StorageProvider::BackblazeB2 => 90,
            },
            retain_days: match provider {
                StorageProvider::AwsS3 => 17,
                StorageProvider::BackblazeB2 => 61,
            },
            encryption_enabled: true,
            secret_access_key: "synthetic-key-never-written-on-macos".into(),
            encryption_password: "synthetic-password-never-written-on-macos".into(),
            last_backup_at: Some("2026-11-03T10:11:12Z".parse().unwrap()),
            ..AppConfig::default()
        }
    }

    fn read_current(path: &Path) -> AppSettings {
        match read_document(path).unwrap().unwrap() {
            SettingsDocument::Current(settings) => settings,
            SettingsDocument::Legacy(_) => panic!("expected the current settings envelope"),
        }
    }

    #[test]
    fn legacy_migration_preserves_each_provider_only_in_its_own_slot() {
        let directory = temporary_directory();
        let path = directory.join("config.json");
        for provider in [StorageProvider::AwsS3, StorageProvider::BackblazeB2] {
            let original = profile(provider);
            let legacy_bytes = serde_json::to_vec(&original).unwrap();
            std::fs::write(&path, &legacy_bytes).unwrap();
            let legacy = match read_document(&path).unwrap().unwrap() {
                SettingsDocument::Legacy(config) => config,
                SettingsDocument::Current(_) => {
                    panic!("a legacy document lost its migration marker")
                }
            };
            let migrated = AppSettings::default().with_profile(&legacy);
            write_document(&path, &migrated).unwrap();
            let reopened = read_current(&path);
            assert_eq!(reopened.selected_provider, provider);
            assert_eq!(
                serde_json::to_value(reopened.selected_profile()).unwrap(),
                serde_json::to_value(&original).unwrap()
            );
            let other = match provider {
                StorageProvider::AwsS3 => StorageProvider::BackblazeB2,
                StorageProvider::BackblazeB2 => StorageProvider::AwsS3,
            };
            assert!(reopened.profile(other).is_none());
            assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 1);
        }
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn saving_and_reopening_each_plan_keeps_the_other_complete_profile() {
        let directory = temporary_directory();
        let path = directory.join("config.json");
        let mut aws = profile(StorageProvider::AwsS3);
        let b2 = AppConfig {
            // UUID reuse is allowed, but must not merge the two plans.
            backup_set_uuid: aws.backup_set_uuid.clone(),
            ..profile(StorageProvider::BackblazeB2)
        };
        write_document(&path, &AppSettings::default().with_profile(&aws)).unwrap();
        write_document(&path, &read_current(&path).with_profile(&b2)).unwrap();
        aws.included_folders = vec!["new-aws-source-only".into()];
        aws.last_backup_at = Some("2026-11-03T12:13:14Z".parse().unwrap());
        write_document(&path, &read_current(&path).with_profile(&aws)).unwrap();
        let reopened = read_current(&path);
        assert_eq!(reopened.selected_provider, StorageProvider::AwsS3);
        assert_eq!(
            serde_json::to_value(reopened.profile(StorageProvider::AwsS3)).unwrap(),
            serde_json::to_value(&aws).unwrap()
        );
        assert_eq!(
            serde_json::to_value(reopened.profile(StorageProvider::BackblazeB2)).unwrap(),
            serde_json::to_value(&b2).unwrap()
        );
        #[cfg(target_os = "macos")]
        {
            let bytes = std::fs::read_to_string(&path).unwrap();
            assert!(!bytes.contains("secretAccessKey"));
            assert!(!bytes.contains("encryptionPassword"));
            assert!(!bytes.contains("synthetic-key-never-written"));
            assert!(!bytes.contains("synthetic-password-never-written"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 1);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn closed_settings_reject_missing_unknown_and_mismatched_profiles() {
        let settings = AppSettings::default().with_profile(&profile(StorageProvider::AwsS3));
        let mut unknown = serde_json::to_value(&settings).unwrap();
        unknown["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<SettingsDocument>(unknown).is_err());
        let mut missing = serde_json::to_value(&settings).unwrap();
        missing.as_object_mut().unwrap().remove("backblazeB2");
        assert!(serde_json::from_value::<SettingsDocument>(missing).is_err());
        let mut unknown_profile = serde_json::to_value(&settings).unwrap();
        unknown_profile["awsS3"]["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<SettingsDocument>(unknown_profile).is_err());
        let mismatched = AppSettings {
            aws_s3: Some(profile(StorageProvider::BackblazeB2)),
            ..settings.clone()
        };
        assert!(mismatched.validate().is_err());
        let missing_selected = AppSettings {
            selected_provider: StorageProvider::BackblazeB2,
            ..settings.clone()
        };
        assert!(missing_selected.validate().is_err());
        let unsupported_profile = AppSettings {
            aws_s3: Some(AppConfig {
                version: 2,
                ..profile(StorageProvider::AwsS3)
            }),
            ..settings.clone()
        };
        assert!(unsupported_profile.validate().is_err());
        let missing_uuid = AppSettings {
            aws_s3: Some(AppConfig {
                backup_set_uuid: String::new(),
                ..profile(StorageProvider::AwsS3)
            }),
            ..settings.clone()
        };
        assert!(missing_uuid.validate().is_err());
        let newer = AppSettings {
            settings_version: SETTINGS_VERSION + 1,
            ..settings
        };
        assert!(newer.validate().is_err());
    }

    #[test]
    fn invalid_save_and_failed_atomic_replacement_preserve_existing_data() {
        let directory = temporary_directory();
        let path = directory.join("config.json");
        let settings = AppSettings::default().with_profile(&profile(StorageProvider::AwsS3));
        write_document(&path, &settings).unwrap();
        let original = std::fs::read(&path).unwrap();
        let invalid = AppSettings {
            settings_version: SETTINGS_VERSION + 1,
            ..settings.clone()
        };
        assert!(write_document(&path, &invalid).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), original);
        let blocked = directory.join("existing-directory");
        std::fs::create_dir(&blocked).unwrap();
        std::fs::write(blocked.join("keep"), b"keep original").unwrap();
        assert!(write_document(&blocked, &settings).is_err());
        assert_eq!(
            std::fs::read(blocked.join("keep")).unwrap(),
            b"keep original"
        );
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 2);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn independent_drafts_have_new_identity_and_no_other_provider_credentials() {
        let aws = new_profile(StorageProvider::AwsS3);
        let b2 = new_profile(StorageProvider::BackblazeB2);
        assert_ne!(aws.backup_set_uuid, b2.backup_set_uuid);
        assert_eq!(aws.storage_provider, StorageProvider::AwsS3);
        assert_eq!(b2.storage_provider, StorageProvider::BackblazeB2);
        assert!(b2.region.is_empty());
        for draft in [aws, b2] {
            assert!(draft.access_key_id.is_empty());
            assert!(draft.secret_access_key.is_empty());
            assert!(draft.encryption_password.is_empty());
            assert!(draft.last_backup_at.is_none());
            assert_eq!(draft.schedule_minutes, 0);
        }
    }
}
