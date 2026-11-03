# Qia Backup

![Qia Backup dashboard](intro.png)

Qia Backup is a macOS backup app that captures **APFS local snapshots** (the same
mechanism Time Machine uses locally) and stores them in **AWS S3 or Backblaze B2**.
**No external Time Machine drive is required.** The engine is Rust; the desktop UI is Tauri
(macOS-only).

---

## What it does

- Creates an APFS local snapshot with `tmutil`, mounts it read-only with
  `mount_apfs`, and backs it up without touching live files. No external drive and
  no Time Machine destination are needed. If snapshot creation or mounting fails,
  the backup fails instead of reading live files. The desktop app is
  APFS-snapshot-only; the CLI can also point at a mounted Time Machine volume
  or any folder.
- Backs up only the folders you list under **Included folders**. An APFS
  snapshot backup requires at least one included folder, so the app never
  silently walks the whole SSD. Custom glob exclusions are still supported;
  macOS filesystem metadata such as `.DS_Store`, `.Spotlight-V100`,
  `.fseventsd` and `.Trashes` is always ignored. New configurations start with
  recommended cache/log exclusions; you can edit them or use **Reset defaults**.
- Splits file contents with a buzhash content-defined chunker, SHA-256 addresses
  every blob, and deduplicates across runs. Changed files are updated
  incrementally; unchanged files reuse the previous record's nodes.
- Checkpoints confirmed uploads into small append-only index journals (every
  ~2 GiB of new data or five minutes, including mid-file) and compacts them
  periodically, so an interrupted run resumes from what is actually in S3.
- Packs large data blobs into 64 MiB blobpacks instead of writing one S3 object
  per chunk. A 1.5 TB first backup is tens of thousands of objects rather than
  millions, without changing the 1 MiB chunking that existing blob IDs use.
- Uploads blobpacks and tree objects on a dynamically scaled worker pool while
  the file walk continues, rather than serializing every upload behind the
  scan. The pool starts conservatively and grows up to 16 concurrent PUTs as
  measured throughput increases (four workers around 5 MB/s, matching four
  64 MiB packs in roughly a minute). In-flight upload memory is bounded.
- Stores data in content-addressed containers: `blobpacks/`, `treepacks/`,
  `standardobjects/`, JSON configuration files, and LZ4-compressed
  `.backuprecord` files under `backupfolders/<uuid>/backuprecords/`.
- Optionally encrypts blobs, trees, packs and records with client-side
  AES-256-CBC + HMAC-SHA256 and a PBKDF2-protected keyset.
- Uses AWS Signature V4 for S3.
- Restores any record, including sparse-file layouts.
- Compares any two records and lists added, modified and deleted files. The full
  trees are already stored in S3, so this works with existing backups without
  re-uploading anything or changing the format.
- Estimates the monthly S3 storage cost from the actual object sizes under the
  backup-set prefix on the Dashboard. The estimate uses S3 Intelligent-Tiering
  list prices, a 10/20/70 Frequent/Infrequent/Archive-Instant mix, and the
  per-object monitoring fee; it also shows the all-Frequent upper bound.
- Applies hourly/daily/weekly/monthly retention and garbage-collects unreferenced
  packs and objects.
- Runs backups manually, on an interval (default hourly, like Time Machine's local
  snapshots), or through the CLI daemon/launchd.
- Pauses backups on battery power and while the Mac sleeps. Automatic and
  manual runs resume only after five minutes of stable AC power; a power-paused
  run remembers its progress and can be cancelled from the Dashboard.
- Shows live upload throughput, active/max worker counts, uploaded bytes, and a
  rolling speed sparkline while a backup runs.

Destinations use the S3 protocol: AWS S3 and Backblaze B2 are supported. The
backup format, encryption, incremental backups, retention and restore are shared.
The Dashboard has separate **Back up to AWS S3** and **Back up to Backblaze B2**
buttons. Both destinations retain independent settings and backup histories. A
provider's **View settings and backups** button selects the context for Settings,
Files, history, restore, comparisons and estimates without starting a backup.
An unconfigured provider opens its Settings before its first backup.

---

## Repository layout

```
src/                 core engine (cross-platform, unit tested)
  core/              chunker, codec, crypto, S3 store, backup, restore, retention
  cli.rs             qia-backupctl / CLI
  lib.rs
src-tauri/           Tauri desktop shell (macOS)
  src/app.rs         commands, scheduler, status/events
  tauri.conf.json
  capabilities/
  icons/
ui/                  static HTML/CSS/JS frontend (no npm build required)
docs/iam-policy.json least-privilege S3 IAM policy
```

The root crate builds and tests on Linux and macOS. The Tauri crate is compiled
only for macOS; `cargo check` at the root does not pull in WebKit.

---

## Build

### Prerequisites (macOS)

- macOS 12 or newer
- Xcode Command Line Tools (`xcode-select --install`)
- Rust stable
- Grant **Full Disk Access** to `Qia Backup.app` in System Settings → Privacy &
  Security → Full Disk Access. Without it, protected files are reported as
  per-file errors and omitted, and mounting the APFS snapshot can fail with
  `Operation not permitted (77)`. When running the CLI from Terminal, grant
  Full Disk Access to Terminal (or iTerm) instead.
- Tauri CLI v2 (for the `.app` / `.dmg`):

```sh
cargo install tauri-cli --version "^2"
```

### Desktop app

```sh
cd src-tauri
cargo tauri build
```

Outputs:

```
src-tauri/target/release/bundle/macos/Qia Backup.app
src-tauri/target/release/bundle/dmg/Qia Backup_0.1.0_*.dmg
```

The app is signed only if you configure a signing identity in Tauri; unsigned
builds are fine for local use. Mounting APFS snapshots runs `mount_apfs` directly
from the app so it inherits the app's Full Disk Access grant. If mounting is
refused with `Operation not permitted (77)`, grant Full Disk Access, restart the
app, and retry. The backup fails rather than reading the live volume, or you can
configure a path source explicitly.

### Updating without losing Full Disk Access

macOS ties Full Disk Access to the app's code signature. Ad-hoc or unsigned
builds get a new identity on every rebuild, so TCC may stop recognizing the app
after an update. To keep the grant across updates:

- Sign every build with the same stable identity, preferably a Developer ID
  Application certificate:
  ```sh
  export APPLE_SIGNING_IDENTITY="Developer ID Application: Your Name (TEAMID)"
  cd src-tauri && cargo tauri build
  ```
  You can also set `bundle.macOS.signingIdentity` in `tauri.conf.json`. Never
  switch identities or rotate the certificate without expecting to re-grant
  Full Disk Access.
- Keep the bundle identifier (`com.quantaia.backup`) and install path
  unchanged. Install to `/Applications/Qia Backup.app`; do not run the app from a
  DMG, Downloads, or `target/` because macOS may apply a different path or
  translocation.
- Quit the app, replace the existing `Qia Backup.app` in place, and relaunch. Keep
  a single copy so the TCC entry and the running binary match.
- Inspect the signature with
  `codesign -dv --verbose=4 /Applications/Qia Backup.app`. A stable
  `Authority`/`TeamIdentifier` preserves the grant; `Signature=adhoc` changes
  with each build.
- If the grant was already invalidated, remove `Qia Backup.app` from Full Disk
  Access, re-add the current `/Applications` copy, and relaunch. A stale entry
  can optionally be cleared with
  `tccutil reset SystemPolicyAllFiles com.quantaia.backup`.

### CLI / engine only

```sh
cargo build --release
./target/release/qia-backupctl --help
# or install it on PATH:
cargo install --path . --bin qia-backupctl
```

`cargo test` runs library tests plus 7 versioned format tests, including full
backup/restore, encryption, incremental dedupe, symlinks, compression, partial
browsing and retention.

---

## Quick start (CLI)

Credentials can come from flags, the macOS Keychain, or the standard AWS
environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_REGION`). On macOS, `init` stores the secret access key and the encryption
password in the login Keychain; `config.json` only keeps non-secret settings.
The default AWS region is `eu-west-1`. For B2 credentials and endpoint settings,
see [Backblaze B2](#backblaze-b2).

```sh
# Configure a backup set. Omit --source to use APFS local snapshots.
qia-backupctl init \
  --bucket my-time-machine-backups \
  --region us-west-2 \
  --prefix qia-backups \
  --name "MacBook Pro" \
  --source "/Volumes/Time Machine Backups" \
  --schedule 1440

# Verify credentials/bucket
qia-backupctl test

# Run now
qia-backupctl backup --verbose

# History
qia-backupctl list

# Changed files in the latest record vs. the previous one
qia-backupctl diff --record latest

# Changed files for a specific record as JSON
qia-backupctl diff --record 1700000000 --json

# Restore the latest record
qia-backupctl restore --dest "$HOME/Downloads/Qia Backup Restore"

# Restore a specific record
qia-backupctl restore --record 'qia-backups/<uuid>/backupfolders/.../backuprecords/123/1700000000.backuprecord' \
  --dest /tmp/restore

# Preview retention changes, then apply
qia-backupctl prune --dry-run
qia-backupctl prune

# Foreground scheduler (interval comes from config)
qia-backupctl daemon
```

For snapshot mode (the default when `--source` is omitted):

```sh
qia-backupctl init \
  --bucket my-time-machine-backups \
  --region us-west-2 \
  --snapshot-volume /System/Volumes/Data \
  --include Users/you/Documents \
  --include Users/you/Pictures
```

`--include` may be repeated. Paths are relative to the source root. For a
folder/Time Machine volume source, omit `--include` to back up the entire
selected path, or repeat it to back up only selected subfolders.

Each backup creates a new local snapshot, mounts it under
`~/Library/Caches/com.quantaia.backup/snapshots/<uuid>`, walks the included folders, unmounts it,
and deletes it with `tmutil deletelocalsnapshots`.

### Background scheduling

The desktop app runs a 30-second scheduler loop while it is open. Automatic
backups always start **paused** when the app launches: click `Auto: Paused` on
the Dashboard to enable them for the current session, or use `Back Up Now` to
run one manually. For always-on scheduling, install the CLI daemon as a
LaunchAgent; see `docs/com.quantaia.backup.daemon.plist` (edit the binary
path, copy to `~/Library/LaunchAgents/`, then `launchctl load`). The daemon
reloads the config on every tick.

### Existing destinations and encryption

Before a backup starts, the app reads `backupconfig.json`,
`encryptedkeyset.dat` and any `indexjournal/*` segments. A configured password
must decrypt the remote keyset, and the recovered master key is then tested
against the newest encrypted backup record in the bucket. This catches both a
wrong password and a `encryptedkeyset.dat` that was replaced with a valid but
different key set. If the destination is encrypted but no password (or the
wrong password/key) is available, or if the local encryption setting disagrees
with the set, the backup is blocked with an actionable message and automatic
backups stay paused. Resolve it by entering the correct password in Settings,
restoring the original keyset, disabling encryption, or using
`Start a new backup set` to create a fresh UUID. An interrupted run is detected
through its index journals and resumes from its last checkpoint instead of
starting over. A scheduled run that fails or is cancelled pauses automatic
backups again, so a misconfiguration does not retry every 30 seconds.

---

## Restore on another machine

The S3 bucket contains the backup data and backup-set metadata, but **not** your
local `config.json`, AWS credentials, or encryption password. To restore onto a
different Mac you need:

- the bucket name, region and key prefix;
- the backup-set UUID (the final component of the S3 path shown on the
  Dashboard, or the `backupfolders/<uuid>` directory in S3);
- an access key with read access to that bucket/prefix;
- the backup-set encryption password, if encryption was enabled.

Using the CLI, attach the new machine to the existing set before listing or
restoring:

```sh
qia-backupctl init \
  --bucket my-time-machine-backups \
  --region eu-west-1 \
  --prefix qia-backups \
  --set-uuid <existing-backup-set-uuid> \
  --password '<backup password>' \
  --no-test

qia-backupctl list
qia-backupctl restore --record latest --dest "$HOME/Downloads/Qia Backup Restore"
```

The app's **Settings → Attach to an existing backup set** field does the same
thing. Do not use `--new-set` when recovering an existing backup set.

If the encryption password is lost, the encrypted backups cannot be recovered:
there is no key escrow or recovery key. Keep the password somewhere
independent of the Mac, and keep a copy of the config if possible.

## Desktop app

1. Open `Qia Backup.app`.
2. **Settings** → enter bucket, region, prefix, and access key/secret. The
   desktop app is APFS-only: keep the volume on `/System/Volumes/Data`; no
   external drive is needed. New configurations start with the recommended
   folders (`Applications`, `Users`, `Library`, `private/etc`, `opt`,
   `usr/local`) and cache/log exclusions. Use **Add folder…** (or type a
   relative path) to extend the include list, **Add file…** / **Add folder…**
   for exclusions, and **Reset defaults** to restore the recommended lists.
   Use **Test connection** to verify the bucket and (if enabled) the
   encryption password.
3. **Save settings**.
4. **Dashboard** → **Back up to AWS S3** or **Back up to Backblaze B2**. Progress, files, bytes read/uploaded,
   upload speed, active upload workers and activity are live. The Dashboard
   also shows an estimated monthly S3 cost based on the bytes currently stored
   under this backup set. Backups pause on battery power or while the Mac
   sleeps and resume after five minutes of stable power; **Don't resume**
   cancels a power-paused run.
5. **Backups** lists completed records and offers **Changes** (added, modified
   and deleted files vs. the previous record), restore, prune and delete-set.
   Restore, Ignore, Remove, Restore entire backup, Cancel backup, Delete backup
   set and Clear log all ask for confirmation. Removing a file or deleting the
   backup set additionally requires Touch ID/Face ID or the macOS account
   password **before** the confirmation dialog; the matching backend command
   also rejects the request unless that one-shot authorization grant is
   present.
   In **Changes**, a modified file can be opened in a side-by-side diff that
   aligns removed and added lines. Binary files and files above the diff size
   limit are intentionally not diffed. A running backup is shown on the
   Dashboard and as an in-progress notice.
6. **Files** is the single restore surface: browse the latest completed backup,
   a specific revision, or the live **Partial current backup** while a run is
   active. The **Versions** column counts distinct content versions across
   completed backups. Use **Show content** to preview text files and **Diff**
   to compare a completed file with its previous revision (or a partial file
   with the latest completed backup). Binary and large files are intentionally
   excluded from previews and diffs. Restore one file or folder by choosing the
   exact version to bring back, or restore the whole backup to a chosen folder.
   Partial restore is available after stopping/cancelling the active run.
7. **Activity** shows the log and can open the log/config folders.

Closing the window hides Qia Backup to the macOS menu bar instead of quitting,
so scheduled backups continue. The tray menu offers **Open Qia Backup**,
**Cancel backup** (or **Cancel restore**) while an operation is running,
**Pause automatic backups** / **Resume automatic backups**, and **Quit Qia
Backup**. The tray title shows the current backup percentage while a run is
active.

Settings are stored at:

```
~/Library/Application Support/Qia Backup/config.json
```

The file is created with mode `0600` and contains non-secret settings only. On
macOS the AWS secret access key and the encryption password are stored in the
login Keychain under the service `com.quantaia.backup`. Existing entries from
the previous `com.photominds.backup` service are read and migrated on first
access. Older config files that still contain plaintext secrets are migrated to
the Keychain and rewritten without them on first load. Use a dedicated IAM user
with `docs/iam-policy.json`.

> Renaming from the old app changes the bundle identifier to
> `com.quantaia.backup`, so macOS may require granting **Full Disk Access** to
> `Qia Backup.app` again. The Application Support data is read from
> `~/Library/Application Support/Qia Backup`; the old
> `~/Library/Caches/com.example.backup` tree cache is not reused.

---

## Backblaze B2

B2 uses its [S3-compatible API](https://www.backblaze.com/docs/cloud-storage-call-the-s3-compatible-api).
The app uses the bucket's regional HTTPS endpoint, path-style object URLs and
SigV4 signing. For example, `https://s3.us-west-004.backblazeb2.com` must use
region `us-west-004`. Copy your bucket's actual S3 endpoint from Backblaze; do
not use a download URL, include the bucket name in the endpoint, or substitute
an AWS region. TLS verification stays enabled and signed requests do not follow
redirects. Buckets with periods in their names work with B2's path-style URLs.

Create a private bucket and a dedicated **Read and Write application key**
restricted to that bucket. Use its **keyID** as the access key ID and its
**applicationKey** as the secret. The master application key is unsupported by
the S3 API. The store uses `listFiles`, `readFiles` and `writeFiles`; include
`deleteFiles` for the standard read/write/delete capability set. It performs no
account-wide bucket listing or HeadBucket request. An optional key name-prefix
restriction must encompass every object under the configured key prefix (for
example `qia-backups/`). The connection test lists only the selected backup-set
prefix and is read-only; it does not prove that uploads/deletes are permitted.
See Backblaze's [application-key capability mapping](https://www.backblaze.com/docs/cloud-storage-s3-compatible-app-keys).

In **Settings**, select the saved **Backblaze B2** destination, enter the S3 endpoint, matching region,
bucket, prefix, key ID and application key, select included folders, then save
and test the connection. Enable client-side encryption before the first backup
if required, and keep the encryption password separately for recovery. On
macOS, B2 application keys are stored separately from the existing AWS key in
the Keychain, scoped to endpoint and key ID. Changing storage accounts requires
entering the corresponding key. **Copy application-key requirements** provides
the bucket, prefix and capability requirements for B2 setup. Selecting a new destination does not copy
objects from another bucket; start a new backup there or attach to a backup
set already present in that destination.

Each provider owns its backup-set UUID, included folders, exclusions, encryption,
retention, schedule and last-run state. Both profiles are saved atomically in one
versioned `config.json`; an existing single destination is migrated into only its
own provider slot with its UUID and settings preserved. Secrets remain in
separate macOS Keychain accounts, including destination-scoped encryption
passwords. Both automatic schedules start paused when the app launches. You can
enable each configured provider's schedule separately; one backup, restore or
metadata mutation runs at a time. Destination switching and settings changes
are blocked during those operations so completion and pruning keep their
originating bucket. Unsaved form edits remain in this app session when switching
views and are used for a backup only after you save them.

For the CLI, populate `B2_APPLICATION_KEY_ID` and `B2_APPLICATION_KEY` securely
in your local environment, then configure the destination, for example:

```sh
qia-backupctl init \
  --provider backblaze_b2 \
  --endpoint https://s3.us-west-004.backblazeb2.com \
  --region us-west-004 \
  --bucket my-private-backup-bucket \
  --prefix qia-backups \
  --source "/path/to/selected/folder" \
  --schedule 0

qia-backupctl test
qia-backupctl backup
qia-backupctl restore --dest "/path/to/restore-check"
```

The source above is a folder; omit `--source` and provide `--include` folders
for APFS snapshot mode. The existing CLI `--password` option configures
client-side encryption. `B2_ENDPOINT` and `B2_REGION` can supply B2 endpoint and
region; explicit flags take precedence. B2 never reads AWS credential variables.
The remaining commands, including list, diff, prune and restore, use the saved
provider automatically. `init --provider` and `backup --provider` select that
provider's saved profile without replacing the other one. For recovery on another machine, supply the same B2
endpoint/region/bucket/prefix, backup-set UUID and encryption password.

B2 keeps old object versions. Qia's pruning and Delete backup set hide current
objects; historical bytes remain billed until a separately configured
[lifecycle rule](https://www.backblaze.com/docs/cloud-storage-lifecycle-rules)
expires hidden/noncurrent versions. Do **not** expire current backup objects
merely by their upload age: recent backups can still reference older deduplicated
blobs. The app does not manage lifecycle rules or Object Lock; retained/locked
objects can prevent cleanup. The Dashboard estimates indexed current B2 storage
at $6.95 per decimal TB per 30 days, before account-wide free storage allowances,
and projects the final storage range from source size. Actual billing is by
byte-hour and also reflects metadata, hidden versions, transfers, discounts and
taxes excluded from this estimate. Check [Backblaze pricing](https://www.backblaze.com/cloud-storage/pricing)
and the console for actual usage and retained versions.

Use one writer per backup set. Backblaze documents a one-second ordering limit
for successive uploads/hides of the same key. The adapter spaces PUT/DELETE
attempts to the same object by at least 1.1 seconds, including retries and new
store instances within the same process. Unrelated keys can upload concurrently;
separate processes or other clients are not coordinated. See Backblaze's
[successive-call behavior](https://www.backblaze.com/docs/cloud-storage-call-the-s3-compatible-api).

## AWS S3 permissions

`docs/iam-policy.json` grants only:

- `s3:ListBucket` / `s3:GetBucketLocation` on the bucket
- `s3:GetObject` / `s3:PutObject` / `s3:DeleteObject` on
  `YOUR_BUCKET/YOUR_PREFIX/*`

For AWS, the app uses virtual-hosted-style requests to the regional S3 endpoint.
It does not create or update the bucket and does not require bucket-level
create/delete/list-all permissions.

Once the bucket is filled in Settings, **Copy IAM policy** copies this policy
with your bucket and key prefix substituted into the clipboard.

---

### Bucket versioning

The app does not require bucket versioning, but it can stay enabled. It does not
break backup or restore: reads use the current object version, and backup
records have unique keys.

Keep these points in mind when versioning is enabled:

- Pruning and **Delete backup set** only delete the current object version.
  S3 creates a delete marker, while noncurrent versions remain and continue to
  be billed.
- The Dashboard cost estimate counts current objects from the blob index. It
  does not include noncurrent versions, so the AWS bill can be higher.
- The app cannot browse or restore old object versions by version ID. To
  recover data that was overwritten or delete-marked, use the S3 console or AWS
  CLI to restore the relevant version. Recovering a whole old backup record
  also requires restoring every blob it references.
- Add an S3 lifecycle rule to expire noncurrent versions after a retention
  window you are comfortable with, otherwise storage can grow indefinitely.
  This includes noncurrent versions of `blobindex.json.lz4` and metadata files.

**MFA delete** is independent of the app. If you enable it, keep the MFA device
safe; the app never changes versioning settings or permanently deletes object
versions.

## How backups work

1. **Prepare source** — create/mount an APFS snapshot, or validate the folder /
   Time Machine volume.
2. **Load keyset** — create `encryptedkeyset.dat` on first encrypted backup, or
   decrypt it with the configured password.
3. **Ensure metadata** — create `backupconfig.json`, `backupplan.json`,
   `backupfolders.json` and `backupfolder.json` if missing.
4. **Load index** — download `blobindex.json.lz4`, replay any
   `indexjournal/*` segments from an interrupted run, and merge with the local
   cache at `~/Library/Application Support/Qia Backup/index/<uuid>.json`.
5. **Load previous record** — read the latest record and its trees, reuse
   unchanged file nodes, and re-index referenced blobs.
6. **Walk** — recursively compare `(inode, size, mtime, mode)`, chunk changed
   files, write new data blobs into `blobpacks/`, and write content-addressed
   trees to `treepacks/`. Only blobs larger than a pack use
   `standardobjects/`.
7. **Checkpoint / flush** — every ~2 GiB or five minutes upload a small journal
   containing only confirmed entries. At the end, flush the final pack, write
   the `.backuprecord`, write the compacted base index and clear the journals.
8. **Retain** — optionally prune old records and unreferenced packs/objects.

Blob IDs are SHA-256 of plaintext, so compression/encryption do not break
dedupe. Trees and blobs are all content-addressed; `blobpacks` are deleted only
when every blob they contain is unreferenced, and GC aborts if any kept record
cannot be read.

An interrupted run resumes from `blobindex.json.lz4` plus any
`indexjournal/*` segments, so already-uploaded blobs are reused instead of
re-sent. Keep `chunkSize` unchanged between runs: chunk boundaries, and
therefore blob IDs, are derived from it. A hard crash loses at most the last
~2 GiB / five minutes of deduplication progress; those objects are uploaded
again. Cancel a run from the app and let it reach idle before stopping the
process so it can write a final checkpoint.

### Retention policy

The default retention values are a Time Machine-style thinning policy:

- **Keep every backup for the last 24 hours** — all recent runs are kept.
- **Keep one backup per day for 30 days** — after the recent window, the newest
  backup from each day is kept.
- **Keep one backup per week for 8 weeks** — then the newest backup from each
  week.
- **Keep one backup per month for 12 months** — then the newest backup from each
  month.

The newest record is always kept, and pruning runs after a successful backup.
A value of `0` disables that window. Enable **Keep every backup forever** to
disable pruning entirely. The Settings screen shows a plain-English summary of
the current policy; the four numeric windows are under **Advanced retention
windows**.

### Estimated S3 cost

The Dashboard estimate reads `blobindex.json.lz4` instead of listing the whole
bucket. The index already records every data and tree object with its uploaded
size, so the estimate reflects the compressed/encrypted and deduplicated bytes
without paging through millions of S3 objects. Small metadata objects (records,
configuration, the index itself) are excluded. S3 Intelligent-Tiering bills per
object and per access tier; the index does not reveal each object's current
tier. The displayed number therefore assumes a typical cold-backup mix of 10%
Frequent Access, 20% Infrequent Access and 70% Archive Instant Access, and
includes the $0.0025 per 1,000 monitored objects (objects under 128 KB are not
monitored). The range shows all-Archive-Instant to all-Frequent. It uses EU
(Ireland) list prices and excludes request charges, data transfer, noncurrent
versions, early-delete effects and taxes. If the index is missing for an
existing backup set, the estimate asks you to run a backup to rebuild it rather
than starting a long bucket listing.

---

## Format

Backup sets use content-addressed SHA-256 blobs, LZ4-compressed binary
nodes/trees, packed 64 MiB blobpacks, per-backup records and optional
client-side encryption with a password-protected keyset. The engine can restore
the backup sets it creates. It does not claim compatibility with backup sets
written by other tools.

---

## Limitations

- macOS 12+ only for the GUI and snapshot mounting; the CLI engine also builds on
  Linux for tests and for backing up a mounted folder.
- APFS snapshot mounting requires Full Disk Access (`Operation not permitted
  (77)` means Full Disk Access is missing or the app was not restarted).
- No ACL capture and no owner restoration; uid/gid and modes are recorded.
- POSIX permissions and modification times are restored. Creation times, BSD
  file flags (locked/hidden/immutable), ACLs and symlink timestamps are not.
- Extended attributes, including Finder metadata and resource forks, are
  restored on a best-effort basis. Failures are logged but not counted as hard
  restore errors.
- Hardlinks are deduplicated on write but restored as independent files.
- Sparse-file detection is not performed on write, so new sparse files are
  stored as full content (data is preserved, but the sparse layout is not).
  Sparse records from other sources are restored correctly.
- Special filesystem nodes such as device nodes, FIFOs and sockets are recorded
  as metadata-only entries but are not faithfully recreated on restore; they
  become empty regular files. They are uncommon in user data.
- The macOS system volume is not backed up, only the selected data volume
  (`/System/Volumes/Data` by default). A fresh macOS installation is still
  required before restoring user data to another machine.
- Uploads use a small worker pool (four concurrent PUTs by default) while the
  walk continues, but each pack is still a single PUT: there is no multipart
  upload. Packs default to 64 MiB, so a slow uplink can still cap throughput.
  In-flight upload memory is bounded by the configured pack size and worker
  count.
- No WORM/Object Lock and no S3 lifecycle management.
- Credentials and the encryption password are stored in the macOS Keychain;
  `config.json` contains only non-secret settings. Keep a separate copy of the
  encryption password outside the Mac.
- The temporary APFS snapshot is always unmounted and deleted after each run.
  Retention manages S3 records and objects only, not APFS snapshots.
- If APFS snapshot creation/mounting is unavailable, the backup fails. There is
  no live-file fallback; use a path source if you want to back up a folder
  directly.
- Unknown/corrupt records are skipped while listing and only surface on restore.
- A backup record is written only after the walk and upload finish. While a run
  is active, the Files view exposes a live **Partial current backup** from the
  in-memory tree; after a cancel/error that tree remains available for browsing
  and best-effort restore of already-uploaded content. The partial tree is
  in-memory for the current app session; a restart rebuilds it while rescanning
  and reuses the existing uploaded blobs. Journal segments make confirmed blobs
  reusable by the next run. A hard crash can lose at most the last ~2 GiB / five
  minutes of deduplication progress; those objects are uploaded again.
- Tree metadata is cached under
  `~/Library/Caches/com.quantaia.backup/treepacks` so incremental walks, diffs
  and pruning do not re-download every directory. The cache is
  content-addressed and safe to delete.
- Changed-file browsing reads both record trees from S3. The first comparison of
  two very large backups can be slow and shows at most 5,000 entries by default;
  use `qia-backupctl diff --limit 0` for an unlimited CLI listing.

---

## Security notes

- Enable Settings → client-side encryption for end-to-end confidentiality. The
  password is required for restore and is never sent to S3.
- The app does not manage bucket settings; configure default encryption or
  lifecycle rules on the bucket in AWS if you need them.
- Use a dedicated IAM user and the minimal policy in `docs/iam-policy.json`.
- The AWS secret key and encryption password are read from the macOS Keychain
  and are never sent to the webview. If the Keychain is unavailable (for
  example over SSH), pass credentials via environment variables or `--password`
  for that run.
- The GUI is not sandboxed and runs with your user's permissions; snapshot
  mounting relies on the app's Full Disk Access grant.
- Review the config file permissions if you copy it between machines.

---

## Tests

```sh
cargo test
```

The tests use a local filesystem `BlobStore`, and SigV4 signing is covered by a
reference-vector unit test. The default suite needs no cloud credentials.

`tests/backblaze_b2.rs` contains an ignored test against a **dedicated B2 test
bucket**, using only synthetic files under a generated UUID prefix. It covers
object/range/list/delete operations, encrypted full and incremental backups, and
byte-for-byte restore of both revisions. Its module documents the required
`QIA_B2_TEST_*` variables. After configuring scoped test credentials and explicitly
authorizing those test uploads/deletions with `QIA_B2_TEST_ALLOW_WRITE=1`, run:

```sh
cargo test --locked --test backblaze_b2 -- --ignored --nocapture
```

The test removes current objects only within its generated prefix. Historical
B2 versions require the test bucket's lifecycle policy. A successful default
local test run does not establish live B2 compatibility.

---

## License

MIT.
