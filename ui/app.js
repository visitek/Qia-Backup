/* global window, document, setInterval, clearTimeout */

const tauri = window.__TAURI__;
const invoke = tauri?.core?.invoke;
const listen = tauri?.event?.listen;
const demo = !invoke;

const DEFAULT_INCLUDED_FOLDERS = [
  "Applications",
  "Users",
  "Library",
  "private/etc",
  "opt",
  "usr/local",
];
const DEFAULT_UPLOAD_LIMIT_BPS = 3 * 1024 * 1024;
const MIN_UPLOAD_LIMIT_BPS = 256 * 1024;
const DEFAULT_EXCLUDE_PATTERNS = [
  "**/Library/Caches",
  "**/Library/Logs",
  "**/*.log",
  "**/*.log.*",
  "**/*.log[0-9]*",
  "**/.ollama/models",
  "**/.lmstudio/models",
  "**/.keras/models",
  "**/.docker/models",
  "**/Library/Application Support/LM Studio/models",
  "**/Library/Application Support/Jan/data/models",
  "**/*.gguf",
  "**/*.ggml",
  "**/*.safetensors",
  "**/*.onnx",
  "**/*.ckpt",
  "**/*.keras",
  "**/*.mlmodel",
  "**/*.mlmodelc",
  "**/*.mlpackage",
  "**/*.tflite",
  "**/*.nemo",
  "**/*.caffemodel",
  "**/*.pdparams",
  "**/*.weights",
  "**/Library/Saved Application State",
  "**/Library/Application Support/CrashReporter",
  "**/.cache",
  "**/.npm/_cacache",
  "**/.yarn/cache",
  "**/.cargo/registry/cache",
  "**/Library/Developer/Xcode/DerivedData",
  "**/Library/Developer/Xcode/iOS DeviceSupport",
  "**/Library/Developer/Xcode/watchOS DeviceSupport",
  "**/Library/Developer/Xcode/tvOS DeviceSupport",
  "**/Library/Developer/CoreSimulator",
  "**/private/var/folders",
  "**/private/var/tmp",
  "**/private/tmp",
  "**/Library/CloudStorage",
  "**/Library/Updates",
  "**/go/pkg/mod",
  "**/.gradle/caches",
  "**/.m2/repository",
  "**/.rustup/toolchains",
  "**/__pycache__",
  "**/.pytest_cache",
  "**/.mypy_cache",
  "**/.ruff_cache",
];

const state = {
  config: null,
  status: {},
  logs: [],
  backups: [],
  snapshots: [],
  restoreDestination: "",
  configPath: "",
  hasSecretAccessKey: false,
  hasEncryptionPassword: false,
  destinationCheck: null,
  version: "0.2.0",
  lastRunning: false,
  diff: null,
  diffRecord: null,
  diffFilter: "",
  filesRecordKey: "",
  filesPath: "",
  filesEntries: [],
  filesResolvedRecord: "",
  filesFilter: "",
  filesIgnoredOnly: false,
  filesListingIgnored: false,
  filesLoading: false,
  filesRequest: 0,
  filesActionBusy: false,
  filesError: "",
  filesNotice: "",
  versionCounts: {},
  versionCountsLoading: false,
  versionCountsPath: "",
  versionCountsRequest: 0,
  versionCountsCache: new Map(),
  restorePath: "",
  restorePartial: false,
  restoreVersions: [],
  restoreSelectedKey: "",
  restoreError: "",
  restoreLoading: false,
  restoreRequest: 0,
  fileDiff: null,
  fileDiffRequest: 0,
  contentRequest: 0,
  confirmResolve: null,
  speedHistory: [],
  lastSpeedSampleAt: 0,
  storageEstimate: null,
  estimateAt: 0,
  estimateError: "",
  progressStart: null,
  progressRun: null,
};

const $ = (id) => document.getElementById(id);
const views = {
  dashboard: ["Dashboard", "Your Time Machine snapshots, safely in AWS S3."],
  backups: ["Backups", "Completed records — compare changes or restore any point in time."],
  files: ["Files", "Browse backed-up files and restore a file, folder, or whole backup."],
  settings: ["Settings", "Destination, source, schedule, upload limit and encryption."],
  activity: ["Activity", "Agent log and diagnostics."],
};

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

function fmtBytes(n) {
  n = Number(n || 0);
  if (!Number.isFinite(n) || n <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return `${n >= 100 || i === 0 ? Math.round(n) : n.toFixed(1)} ${units[i]}`;
}

function fmtSpeed(n) {
  return `${fmtBytes(n)}/s`;
}

function limitMbValue(bps) {
  const mb = Math.max(MIN_UPLOAD_LIMIT_BPS, Number(bps) || DEFAULT_UPLOAD_LIMIT_BPS) / (1024 * 1024);
  return Number.isInteger(mb) ? String(mb) : mb.toFixed(2);
}

function limitBpsFromInput() {
  const mb = Math.max(MIN_UPLOAD_LIMIT_BPS / (1024 * 1024), num("upload-limit") || 3);
  return Math.round(mb * 1024 * 1024);
}

function backupProgress(s) {
  const done = Number(s?.files || 0);
  const total = Number(s?.totalFiles || 0);
  if (s?.restoring || total <= 0) return null;
  return Math.max(0, Math.min(1, done / total));
}

function fmtPercent(ratio) {
  if (ratio == null) return "";
  const pct = ratio * 100;
  if (pct > 0 && pct < 0.1) return "<0.1%";
  if (pct < 10) return `${pct.toFixed(1)}%`;
  return `${Math.round(pct)}%`;
}

function fmtFileProgress(s) {
  const done = Number(s?.files || 0).toLocaleString();
  const total = Number(s?.totalFiles || 0);
  return total > 0 ? `${done} / ~${total.toLocaleString()} files` : `${done} files`;
}

// Use the average file rate since scanning started rather than a short
// rolling window. Short windows make the ETA jump wildly when a large file or
// a slow directory is encountered.
function sampleProgress(s) {
  const total = Number(s?.totalFiles || 0);
  const done = Number(s?.files || 0);
  const active = !!s?.running && !s?.restoring && !s?.cancelRequested
    && total > 0 && done > 0 && done < total;
  if (!active) {
    if (!s?.running) {
      state.progressStart = null;
      state.progressRun = null;
    }
    return null;
  }
  const runKey = s.startedAt || "";
  if (state.progressRun !== runKey) {
    state.progressRun = runKey;
    state.progressStart = null;
  }
  const now = Date.now();
  if (!state.progressStart || done < state.progressStart.files) {
    state.progressStart = { t: now, files: done };
    return null;
  }
  const elapsed = (now - state.progressStart.t) / 1000;
  const processed = done - state.progressStart.files;
  // Wait for a meaningful sample before showing an estimate.
  if (elapsed < 10 || processed < 50) return null;
  const rate = processed / elapsed;
  if (!Number.isFinite(rate) || rate <= 0) return null;
  return { seconds: (total - done) / rate, rate };
}

function fmtEta(seconds) {
  if (!Number.isFinite(seconds) || seconds < 0) return "";
  const minutes = Math.max(1, Math.round(seconds / 60));
  if (minutes < 60) return `~${minutes}m left`;
  const hours = Math.floor(minutes / 60);
  const remMinutes = minutes % 60;
  if (hours < 24) return remMinutes ? `~${hours}h ${remMinutes}m left` : `~${hours}h left`;
  const days = Math.floor(hours / 24);
  const remHours = hours % 24;
  return remHours ? `~${days}d ${remHours}h left` : `~${days}d left`;
}

function fmtDate(value) {
  if (!value) return "Never";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value);
  return d.toLocaleString(undefined, {
    year: "numeric", month: "short", day: "numeric",
    hour: "2-digit", minute: "2-digit",
  });
}

function fmtSchedule(minutes) {
  minutes = Number(minutes || 0);
  if (!minutes) return "Manual";
  if (minutes % 1440 === 0) return `Every ${minutes / 1440} day${minutes / 1440 === 1 ? "" : "s"}`;
  if (minutes % 60 === 0) return `Every ${minutes / 60} hour${minutes / 60 === 1 ? "" : "s"}`;
  return `Every ${minutes} min`;
}

function toast(message, isError = false) {
  const el = $("toast");
  el.textContent = message;
  el.className = `toast show${isError ? " error" : ""}`;
  clearTimeout(toast._timer);
  toast._timer = setTimeout(() => { el.className = "toast"; }, 4200);
}

function banner(message, isError = false) {
  const el = $("banner");
  if (!message) { el.className = "banner hidden"; return; }
  el.textContent = message;
  el.className = `banner${isError ? " error" : ""}`;
}

function renderBanner() {
  if (demo) return;
  const s = state.status || {};
  if (!state.config?.bucket) {
    banner("Configure your S3 bucket, credentials and source in Settings to get started.");
    return;
  }
  if (s.running) { banner(""); return; }
  if (s.lastError) { banner(s.lastError, true); return; }
  if (s.powerPaused) { banner(""); return; }
  if (s.powerMessage) { banner(s.powerMessage); return; }
  if (state.destinationCheck && !state.destinationCheck.canBackup) {
    const paused = s.schedulerPaused ? " Automatic backups are paused." : "";
    banner(`${state.destinationCheck.message}${paused}`, true);
    return;
  }
  banner("");
}

function confirmAction({ title, message, detail = "", confirmLabel = "Confirm", danger = false }) {
  if (state.confirmResolve) state.confirmResolve(false);
  return new Promise((resolve) => {
    state.confirmResolve = resolve;
    $("confirm-title").textContent = title;
    $("confirm-message").textContent = message;
    $("confirm-detail").textContent = detail;
    $("confirm-detail").classList.toggle("hidden", !detail);
    const accept = $("confirm-accept");
    accept.textContent = confirmLabel;
    accept.classList.toggle("danger", !!danger);
    accept.classList.toggle("primary", !danger);
    $("confirm-modal").classList.remove("hidden");
    window.setTimeout(() => accept.focus(), 0);
  });
}

function closeConfirm(result) {
  if (!state.confirmResolve) return;
  const resolve = state.confirmResolve;
  state.confirmResolve = null;
  $("confirm-modal").classList.add("hidden");
  resolve(result);
}

async function authorizeSensitive(action) {
  if (demo || !invoke) return true;
  try {
    await invoke("authorize_sensitive_action", { action });
    return true;
  } catch (e) {
    toast(String(e), true);
    return false;
  }
}

async function clearSensitiveAuthorization() {
  if (demo || !invoke) return;
  try { await invoke("clear_sensitive_authorization"); } catch (_) { /* ignore */ }
}

function showView(name) {
  document.querySelectorAll(".view").forEach((v) => v.classList.remove("active"));
  document.querySelectorAll(".nav-item").forEach((b) => b.classList.toggle("active", b.dataset.view === name));
  const target = $(`view-${name}`);
  if (target) target.classList.add("active");
  const [title, subtitle] = views[name] || views.dashboard;
  $("view-title").textContent = title;
  $("view-subtitle").textContent = subtitle;
  if (name === "backups") refreshBackups();
  if (name === "files") refreshFiles();
  if (name === "dashboard") refreshEstimate();
  if (name === "activity") renderLogs();
}

function val(id) { return $(id)?.value?.trim() ?? ""; }
function num(id) { const v = Number($(id)?.value); return Number.isFinite(v) ? v : 0; }
function checked(id) { return !!$(id)?.checked; }

function updateIamPolicyButton() {
  const btn = $("copy-iam-policy");
  if (!btn) return;
  const bucket = val("bucket");
  btn.disabled = !bucket;
  btn.title = bucket
    ? "Copy an IAM policy scoped to this bucket and prefix"
    : "Fill in the bucket to enable this";
}

function iamPolicyJson(bucket, prefix) {
  const cleanPrefix = String(prefix || "").replace(/^\/+|\/+$/g, "");
  const objectResource = cleanPrefix
    ? `arn:aws:s3:::${bucket}/${cleanPrefix}/*`
    : `arn:aws:s3:::${bucket}/*`;
  return JSON.stringify({
    Version: "2012-10-17",
    Statement: [
      {
        Sid: "ListBucketForBackupSet",
        Effect: "Allow",
        Action: ["s3:ListBucket", "s3:GetBucketLocation"],
        Resource: `arn:aws:s3:::${bucket}`,
      },
      {
        Sid: "ReadWriteBackupSetObjects",
        Effect: "Allow",
        Action: ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
        Resource: objectResource,
      },
    ],
  }, null, 2);
}

async function copyText(text) {
  if (invoke) {
    try {
      await invoke("copy_to_clipboard", { text });
      return;
    } catch (_) {
      /* fall back to the web clipboard API */
    }
  }
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }
  throw new Error("Clipboard is unavailable");
}

function applyConfig(cfg) {
  state.config = cfg;
  if (!cfg) return;
  $("bucket").value = cfg.bucket || "";
  $("region").value = cfg.region || "";
  $("prefix").value = cfg.prefix || "";
  $("backup-set-uuid").value = cfg.backupSetUuid || "";
  $("access-key").value = cfg.accessKeyId || "";
  $("secret-key").value = "";
  $("secret-key").placeholder = state.hasSecretAccessKey
    ? "Saved in macOS Keychain — leave blank to keep"
    : "Secret access key";
  $("snapshot-volume").value = cfg.snapshotVolume || "/System/Volumes/Data";
  $("included-folders").value = (cfg.includedFolders || []).join("\n");
  $("excludes").value = (cfg.excludePatterns || []).join("\n");
  $("schedule").value = cfg.scheduleMinutes ?? 60;
  $("upload-limit").value = limitMbValue(cfg.uploadLimitBytesPerSec);
  $("retain-all").checked = !!cfg.retainAll;
  $("retain-hours").value = cfg.retainHours ?? 24;
  $("retain-days").value = cfg.retainDays ?? 30;
  $("retain-weeks").value = cfg.retainWeeks ?? 8;
  $("retain-months").value = cfg.retainMonths ?? 12;
  updateRetentionUI();
  $("encryption-enabled").checked = !!cfg.encryptionEnabled;
  $("encryption-password").value = "";
  $("encryption-password").placeholder = state.hasEncryptionPassword
    ? "Saved in macOS Keychain — leave blank to keep"
    : "Encryption password";
  $("config-path").textContent = state.configPath || "";
  updateIamPolicyButton();
}

function resetSourceDefaults() {
  $("included-folders").value = DEFAULT_INCLUDED_FOLDERS.join("\n");
  $("excludes").value = DEFAULT_EXCLUDE_PATTERNS.join("\n");
  toast("Recommended folders and cache exclusions restored — click Save settings");
}

function textareaLines(id) {
  return ($(id)?.value || "").split("\n").map((s) => s.trim()).filter(Boolean);
}

function gatherConfig() {
  const base = state.config || {};
  return {
    ...base,
    bucket: val("bucket"),
    region: val("region"),
    prefix: val("prefix"),
    backupSetUuid: val("backup-set-uuid") || base.backupSetUuid,
    accessKeyId: val("access-key"),
    secretAccessKey: val("secret-key"),
    sourceKind: "snapshot",
    sourcePath: "",
    snapshotVolume: val("snapshot-volume") || "/System/Volumes/Data",
    includedFolders: textareaLines("included-folders"),
    excludePatterns: textareaLines("excludes"),
    scheduleMinutes: Math.max(0, Math.round(num("schedule"))),
    uploadLimitBytesPerSec: limitBpsFromInput(),
    retainAll: checked("retain-all"),
    retainHours: Math.max(0, Math.round(num("retain-hours"))),
    retainDays: Math.max(0, Math.round(num("retain-days"))),
    retainWeeks: Math.max(0, Math.round(num("retain-weeks"))),
    retainMonths: Math.max(0, Math.round(num("retain-months"))),
    encryptionEnabled: checked("encryption-enabled"),
    encryptionPassword: val("encryption-password"),
  };
}

function updateRetentionUI() {
  const forever = checked("retain-all");
  ["retain-hours", "retain-days", "retain-weeks", "retain-months"].forEach((id) => {
    $(id).disabled = forever;
  });
  const summary = $("retention-summary");
  if (forever) {
    summary.textContent = "Every successful backup is kept forever. Automatic pruning is disabled.";
    return;
  }
  const hours = num("retain-hours");
  const days = num("retain-days");
  const weeks = num("retain-weeks");
  const months = num("retain-months");
  const parts = [];
  if (hours > 0) parts.push(`every backup from the last ${hours} hour${hours === 1 ? "" : "s"}`);
  if (days > 0) parts.push(`one backup per day for ${days} day${days === 1 ? "" : "s"}`);
  if (weeks > 0) parts.push(`one backup per week for ${weeks} week${weeks === 1 ? "" : "s"}`);
  if (months > 0) parts.push(`one backup per month for ${months} month${months === 1 ? "" : "s"}`);
  summary.textContent = parts.length
    ? `Keeps ${parts.join(", ")}. The newest record is always kept; older records are deleted after each successful backup.`
    : "Only the newest record is kept; older records are deleted after each successful backup.";
}

function updateDashboard() {
  const cfg = state.config || {};
  $("dash-destination").textContent = cfg.bucket
    ? `s3://${cfg.bucket}/${cfg.prefix || ""}/${cfg.backupSetUuid || ""}`
    : "Not configured — open Settings";
  $("dash-source").textContent = `APFS snapshot of ${cfg.snapshotVolume || "/System/Volumes/Data"}`;
  $("dash-schedule").textContent = fmtSchedule(cfg.scheduleMinutes);
  const limitEnabled = state.status?.uploadLimitEnabled ?? cfg.uploadLimitEnabled ?? true;
  const limitBps = state.status?.uploadLimitBytesPerSec
    || cfg.uploadLimitBytesPerSec
    || DEFAULT_UPLOAD_LIMIT_BPS;
  const limitToggle = $("upload-limit-toggle");
  limitToggle.setAttribute("aria-pressed", String(!!limitEnabled));
  $("dash-upload-limit").textContent = limitEnabled
    ? `Limit ${fmtSpeed(limitBps)}`
    : "Upload limit off";
  limitToggle.title = limitEnabled
    ? `Uploads are capped at ${fmtSpeed(limitBps)}. Click to remove the cap.`
    : `Upload limit is off (${fmtSpeed(limitBps)} configured). Click to enable it.`;
  const last = state.status?.lastSummary || null;
  const backingUp = !!state.status?.running && !state.status?.restoring;
  const live = backingUp ? state.status : null;
  $("dash-last").textContent = backingUp
    ? `Running since ${fmtDate(state.status.startedAt)}`
    : (last ? fmtDate(last.finishedAt || cfg.lastBackupAt) : fmtDate(cfg.lastBackupAt));
  $("dash-incremental").textContent = last
    ? `${Number(last.blobsReused || 0).toLocaleString()} reused / ${Number(last.blobsNew || 0).toLocaleString()} new chunks`
    : "—";
  $("stat-files-label").textContent = backingUp ? "Files scanned so far" : "Files in last backup";
  $("stat-uploaded-label").textContent = backingUp ? "Uploaded so far" : "Uploaded";
  $("stat-read-label").textContent = backingUp ? "Read so far" : "Read";
  $("stat-files").textContent = live
    ? (Number(live.totalFiles || 0) > 0
        ? `${Number(live.files || 0).toLocaleString()} / ${Number(live.totalFiles || 0).toLocaleString()}`
        : Number(live.files || 0).toLocaleString())
    : (last ? Number(last.files || 0).toLocaleString() : "—");
  $("stat-uploaded").textContent = live
    ? fmtBytes(live.bytesUploaded)
    : (last ? fmtBytes(last.bytesUploaded) : "—");
  $("stat-read").textContent = live
    ? fmtBytes(live.bytesScanned)
    : (last ? fmtBytes(last.bytesScanned) : "—");
  $("stat-records").textContent = state.backups.length ? String(state.backups.length) : "—";
  $("version").textContent = `v${state.version}`;
  renderBanner();
}

function fmtMoney(value) {
  return `$${Number(value || 0).toFixed(2)}`;
}

function renderEstimate() {
  const estimate = state.storageEstimate;
  if (!estimate) {
    $("cost-value").textContent = "—";
    $("cost-sub").textContent = state.estimateError || "Connect to S3 to calculate.";
    $("cost-range").textContent = "";
    $("cost-projection").textContent = "";
    $("cost-assumption").textContent = "";
    $("cost-basis").textContent = "";
    return;
  }
  $("cost-value").textContent = fmtMoney(estimate.monthlyEstimate);
  $("cost-sub").textContent = `${fmtBytes(estimate.bytes)} stored · ${Number(estimate.objects || 0).toLocaleString()} objects · ${Number(estimate.monitoredObjects || 0).toLocaleString()} monitored`;
  $("cost-range").textContent = `Range ${fmtMoney(estimate.monthlyLow)}–${fmtMoney(estimate.monthlyHigh)}/month`;
  const hasProjection = estimate.projectedBytesLow != null && estimate.projectedBytesHigh != null;
  $("cost-projection").textContent = hasProjection
    ? `After this backup: ~${fmtBytes(estimate.projectedBytesLow)}–${fmtBytes(estimate.projectedBytesHigh)} · ${fmtMoney(estimate.projectedMonthlyLow)}–${fmtMoney(estimate.projectedMonthlyHigh)}/month (rough)`
    : "";
  $("cost-projection").title = hasProjection ? (estimate.projectionBasis || "") : "";
  $("cost-assumption").textContent = estimate.assumedTierMix;
  $("cost-basis").textContent = estimate.pricingBasis;
  $("cost-card").title = estimate.note || "";
}

async function refreshEstimate(force = false) {
  if (demo) {
    state.storageEstimate = null;
    state.estimateError = "Preview mode: connect the macOS app to estimate S3 cost.";
    renderEstimate();
    return;
  }
  if (!state.config?.bucket) {
    state.storageEstimate = null;
    state.estimateError = "Configure the bucket to estimate S3 cost.";
    renderEstimate();
    return;
  }
  if (refreshEstimate._busy) return;
  if (!force && state.storageEstimate && Date.now() - state.estimateAt < 5 * 60 * 1000) {
    renderEstimate();
    return;
  }
  refreshEstimate._busy = true;
  state.estimateError = "";
  $("cost-value").textContent = "…";
  $("cost-sub").textContent = "Reading the backup index…";
  try {
    const sourceBytes = Number(
      state.status?.totalBytes || state.status?.lastSummary?.bytesScanned || 0
    );
    state.storageEstimate = await invoke("storage_estimate", {
      sourceBytes: sourceBytes || null,
    });
    state.estimateAt = Date.now();
  } catch (e) {
    state.storageEstimate = null;
    state.estimateError = `Estimate unavailable: ${e}`;
  } finally {
    refreshEstimate._busy = false;
    renderEstimate();
  }
}

function renderTransferSpark() {
  const el = $("transfer-spark");
  if (!el) return;
  const history = state.speedHistory;
  if (!history.length) { el.innerHTML = ""; return; }
  const max = Math.max(1, ...history.map((sample) => sample.bps));
  el.innerHTML = history.map((sample) => {
    const height = Math.max(6, Math.round((sample.bps / max) * 100));
    return `<span style="height:${height}%" title="${esc(fmtSpeed(sample.bps))}"></span>`;
  }).join("");
}

function renderTransfer(s) {
  const active = !!s.running && !s.restoring;
  const speed = Number(s.uploadSpeedBps || 0);
  const target = Number(s.uploadTargetThreads || 0);
  const max = Number(s.uploadMaxThreads || 0);
  const inFlight = Number(s.uploadInFlight || 0);
  $("progress-speed").textContent = active
    ? (speed > 0 ? fmtSpeed(speed) : (inFlight > 0 ? "measuring…" : "0 B/s"))
    : "—";
  $("progress-transfer").textContent = active
    ? `${fmtBytes(s.bytesUploaded)} uploaded`
    : "Waiting for uploads…";
  $("progress-workers").textContent = active && target > 0
    ? `${inFlight} active · ${target}/${max} workers`
    : (active ? "Starting upload workers…" : "");
  if (!active) return;
  const now = Date.now();
  if (now - state.lastSpeedSampleAt >= 3000) {
    state.speedHistory.push({ at: now, bps: speed });
    if (state.speedHistory.length > 30) state.speedHistory.shift();
    state.lastSpeedSampleAt = now;
    renderTransferSpark();
  }
}

function updatePowerPause() {
  const el = $("power-pause");
  if (!el) return;
  const paused = !!state.status?.powerPaused;
  el.classList.toggle("hidden", !paused);
  if (paused) {
    $("power-pause-message").textContent = state.status?.powerMessage
      || "Power conditions paused the backup. It will resume automatically after five minutes of stable power.";
  }
}

function updateStatus() {
  const s = state.status || {};
  const running = !!s.running;
  const busy = running || !!s.editing;
  const pill = $("status-pill");
  const text = $("status-text");
  pill.className = "status-pill";
  if (s.editing) {
    pill.classList.add("running");
    text.textContent = "Updating backup…";
  } else if (running) {
    pill.classList.add("running");
    text.textContent = s.cancelRequested
      ? "Cancelling…"
      : (s.restoring ? "Restoring…" : "Backing up…");
  } else if (s.lastError) {
    pill.classList.add("error");
    text.textContent = "Needs attention";
  } else if (s.powerPaused || s.powerMessage) {
    pill.classList.add("paused");
    text.textContent = "Paused";
  } else if (s.lastSummary) {
    pill.classList.add("success");
    text.textContent = "Ready";
  } else {
    text.textContent = "Ready";
  }
  const missingIncludes =
    !(state.config?.includedFolders || []).some((path) => String(path).trim());
  const powerBlocked = !busy && !!s.powerMessage;
  $("backup-now").disabled = busy || missingIncludes || powerBlocked;
  $("backup-now").title = missingIncludes
    ? "Select at least one Included folder in Settings"
    : (powerBlocked ? s.powerMessage : "");
  $("cancel-run").classList.toggle("hidden", !running);
  $("cancel-run").disabled = !!s.cancelRequested;
  const auto = $("toggle-auto");
  const interval = state.config?.scheduleMinutes || 0;
  if (interval === 0 || !state.config?.bucket) {
    auto.textContent = interval === 0 ? "Auto: Manual" : "Auto: Paused";
    auto.disabled = true;
    auto.classList.remove("primary");
  } else {
    auto.textContent = s.schedulerPaused ? "Auto: Paused" : "Auto: On";
    auto.disabled = busy || missingIncludes;
    auto.classList.toggle("primary", !s.schedulerPaused);
  }
  const card = $("progress-card");
  card.classList.toggle("hidden", !running);
  if (running) {
    $("progress-phase").textContent = s.cancelRequested
      ? "Cancelling…"
      : (s.restoring ? "Restoring" : (s.phase || "Working"));
    $("progress-detail").textContent = s.detail || "";
    const progress = backupProgress(s);
    const pct = progress == null ? null : progress * 100;
    const eta = sampleProgress(s);
    $("progress-eta").textContent = eta ? fmtEta(eta.seconds) : "";
    $("progress-eta").title = eta
      ? `${Math.round(eta.rate).toLocaleString()} files/s average since scanning started`
      : "";
    const counting = pct == null && String(s.phase || "").toLowerCase() === "counting";
    if (counting) {
      // The denominator is still being discovered; avoid showing a
      // misleading "0 files / 0 B" while the metadata walk runs.
      $("progress-files").textContent = "Counting files…";
      $("progress-files").title = "Walking the snapshot's metadata to find the total";
      $("progress-read").textContent = "";
      $("progress-upload").textContent = "";
      $("progress-percent").textContent = "counting…";
    } else {
      $("progress-files").textContent = fmtFileProgress(s);
      $("progress-files").title = Number(s.totalFiles || 0) > 0
        ? "Total is estimated from the previous backup"
        : "Total files are unknown until the first backup completes";
      const totalBytes = Number(s.totalBytes || 0);
      $("progress-read").textContent = totalBytes > 0
        ? `${fmtBytes(s.bytesScanned)} / ~${fmtBytes(totalBytes)} read`
        : `${fmtBytes(s.bytesScanned)} read`;
      $("progress-read").title = totalBytes > 0
        ? `${((Math.min(Number(s.bytesScanned || 0), totalBytes) / totalBytes) * 100).toFixed(1)}% of the estimated bytes`
        : "";
      $("progress-upload").textContent = `${fmtBytes(s.bytesUploaded)} uploaded`;
      $("progress-percent").textContent = fmtPercent(progress);
    }
    renderTransfer(s);
    const bar = $("progress-bar");
    if (pct == null) {
      bar.classList.add("indeterminate");
      bar.style.width = "";
      bar.title = "Total file count unknown";
    } else {
      bar.classList.remove("indeterminate");
      bar.style.width = `${pct.toFixed(2)}%`;
      bar.title = `${fmtFileProgress(s)} (estimated from the previous backup)`;
    }
  }
  updatePowerPause();
  updateFilesNote();
  updateDashboard();
  updateBackupsRunning();
  if (state.lastRunning && !running) {
    // A run just finished; reload history and the saved config.
    state.estimateAt = 0;
    refreshBackups();
    loadState({ quiet: true });
  } else if (!state.lastRunning && running) {
    // Make the empty history state explain the run that is in progress.
    state.speedHistory = [];
    state.lastSpeedSampleAt = 0;
    renderTransferSpark();
    renderBackups();
  }
  state.lastRunning = running;
}

function updateBackupsRunning() {
  const el = $("backups-running");
  if (!el) return;
  const s = state.status || {};
  if (!s.running || s.restoring) {
    el.classList.add("hidden");
    el.textContent = "";
    return;
  }
  const speed = Number(s.uploadSpeedBps || 0);
  const workers = Number(s.uploadTargetThreads || 0);
  const transfer = speed > 0 ? ` at ${fmtSpeed(speed)}` : "";
  const concurrency = workers > 0
    ? ` · ${Number(s.uploadInFlight || 0)}/${workers} uploads active`
    : "";
  el.textContent = `Backup in progress — ${Number(s.files || 0).toLocaleString()} files scanned, ${fmtBytes(s.bytesUploaded)} uploaded${transfer}${concurrency}. Browse live progress in Files under Partial current backup; the completed record appears here when the run finishes.`;
  el.classList.remove("hidden");
  el.classList.remove("error");
}

function renderLogs() {
  const lines = state.logs || [];
  const html = lines.length ? lines.map(logLine).join("") : '<span class="muted">No activity yet.</span>';
  $("log-preview").innerHTML = html;
  $("log-full").innerHTML = html;
  $("log-preview").scrollTop = $("log-preview").scrollHeight;
  $("log-full").scrollTop = $("log-full").scrollHeight;
}

function logLine(line) {
  const text = String(line);
  const m = text.match(/^\[([^\]]+)\]\s+(\w+)\s+(.*)$/);
  if (!m) return `<span class="log-line">${esc(text)}</span>`;
  const level = m[2].toLowerCase();
  const cls = level === "warn" ? " warn" : level === "error" ? " error" : "";
  return `<span class="log-line${cls}"><span class="ts">[${esc(m[1])}]</span> <span class="lvl">${esc(m[2])}</span> ${esc(m[3])}</span>`;
}

function statusPill(entry) {
  if (entry.isComplete === true) return '<span class="pill ok">complete</span>';
  if (entry.isComplete === false) return '<span class="pill warn">errors</span>';
  return '<span class="pill">unknown</span>';
}

function backupRow(entry, withRecord = false) {
  const record = esc(entry.recordName || entry.key.split("/").pop());
  return `<tr>
    <td>${esc(fmtDate(entry.timestamp))}</td>
    <td>${Number(entry.filesCount || 0).toLocaleString()}</td>
    <td>${fmtBytes(entry.bytesUploaded)}</td>
    <td>${statusPill(entry)}</td>
    ${withRecord ? `<td class="mono">${record}</td>` : ""}
    <td>
      <span class="row-actions">
        <button class="link-button" data-diff="${esc(entry.key)}">Changes</button>
        <button class="link-button" data-files-browse="${esc(entry.key)}">Browse</button>
        <button class="link-button" data-restore="${esc(entry.key)}" title="Open the Files view to choose a destination and restore">Restore…</button>
      </span>
    </td>
  </tr>`;
}

function renderBackups() {
  const recentBody = $("recent-table").querySelector("tbody");
  const body = $("backups-table").querySelector("tbody");
  if (!state.backups.length) {
    const running = !!state.status?.running && !state.status?.restoring;
    const empty = running
      ? "No completed backups yet. Browse the partial backup in Files while it runs."
      : "No backups yet.";
    recentBody.innerHTML = `<tr><td colspan="5" class="empty">${empty}</td></tr>`;
    body.innerHTML = `<tr><td colspan="6" class="empty">${empty}</td></tr>`;
  } else {
    recentBody.innerHTML = state.backups.slice(0, 5).map((b) => backupRow(b)).join("");
    body.innerHTML = state.backups.map((b) => backupRow(b, true)).join("");
  }
  document.querySelectorAll("[data-restore]").forEach((btn) => {
    btn.addEventListener("click", () => {
      state.filesRecordKey = btn.getAttribute("data-restore");
      state.filesPath = "";
      state.filesFilter = "";
      state.filesIgnoredOnly = false;
      $("files-filter").value = "";
      if ($("files-ignored-only")) $("files-ignored-only").checked = false;
      $("files-dest").value = $("files-dest").value || state.restoreDestination;
      showView("files");
      window.setTimeout(() => {
        const dest = $("files-dest");
        if (!dest) return;
        dest.scrollIntoView({ behavior: "smooth", block: "center" });
        dest.focus();
      }, 60);
    });
  });
  document.querySelectorAll("[data-diff]").forEach((btn) => {
    btn.addEventListener("click", () => openDiff(btn.getAttribute("data-diff")));
  });
  document.querySelectorAll("[data-files-browse]").forEach((btn) => {
    btn.addEventListener("click", () => {
      state.filesRecordKey = btn.getAttribute("data-files-browse");
      state.filesPath = "";
      state.filesFilter = "";
      state.filesIgnoredOnly = false;
      $("files-filter").value = "";
      if ($("files-ignored-only")) $("files-ignored-only").checked = false;
      showView("files");
    });
  });
  renderFilesRecords();
  updateDashboard();
}

async function refreshBackups() {
  if (demo) { renderBackups(); return; }
  if (!state.config?.bucket) {
    state.backups = [];
    renderBackups();
    renderFilesRecords();
    return;
  }
  // Drop the previous list before the request so a slow or failed refresh can
  // never leave deleted records visible in the Backups view.
  state.backups = [];
  renderBackups();
  try {
    state.backups = await invoke("list_backups");
  } catch (e) {
    state.backups = [];
    if (String(e).toLowerCase().includes("credential") || String(e).toLowerCase().includes("bucket")) {
      banner(String(e), true);
    }
  }
  renderBackups();
  renderFilesRecords();
}

function filesParentPath(path) {
  const i = (path || "").lastIndexOf("/");
  return i < 0 ? "" : path.slice(0, i);
}

function renderFilesRecords() {
  const select = $("files-record");
  if (!select) return;
  const partialAvailable = !!state.status?.partialAvailable;
  const current = state.filesRecordKey || "";
  if (current === "partial" && !partialAvailable) state.filesRecordKey = "";
  const latest = state.backups[0];
  const currentLabel = latest ? `Current (latest completed) — ${fmtDate(latest.timestamp)}` : "Current — no completed backups yet";
  const partialLabel = state.status?.running
    ? "Partial current backup (in progress)"
    : "Partial backup (last stopped run)";
  let html = "";
  if (partialAvailable) {
    html += `<option value="partial">${esc(partialLabel)}</option>`;
  }
  html += `<option value="">${esc(currentLabel)}</option>`;
  html += state.backups.map((b) => `<option value="${esc(b.key)}">${esc(fmtDate(b.timestamp))} — ${esc(b.recordName || "")}</option>`).join("");
  select.innerHTML = html;
  if (state.filesRecordKey === "partial" && partialAvailable) {
    select.value = "partial";
  } else if (state.filesRecordKey && state.backups.some((b) => b.key === state.filesRecordKey)) {
    select.value = state.filesRecordKey;
  } else {
    state.filesRecordKey = "";
    select.value = "";
  }
}

function updateFilesNote() {
  const note = $("files-note");
  if (!note) return;
  if (state.filesRecordKey === "partial") {
    note.textContent = state.status?.running
      ? "Partial current backup — showing files and folders processed so far. Click Refresh to see newer files. Ignore affects future backups; Remove needs a completed record and is unavailable while running."
      : "Partial backup from the last stopped run. Its record was not completed, but already-uploaded content can still be restored. Ignore affects future backups; Remove needs a completed record.";
  } else if (state.status?.running) {
    note.textContent = state.status?.partialAvailable
      ? "A backup is running. Select Partial current backup and click Refresh to browse progress; the completed backup appears when it finishes. Ignore affects future backups; Remove is unavailable while running."
      : "A backup is running. Partial files will appear here as the first directories complete. Ignore affects future backups; Remove is unavailable while running.";
  } else if (state.status?.partialAvailable) {
    note.textContent = "A partial backup from the last stopped run is available. It has no completed record yet; restore works for content already uploaded. Ignore affects future backups; Remove needs a completed record.";
  } else {
    note.textContent = "Current backup means the latest completed backup record. Ignore excludes a path from future backups; Remove edits only the selected completed revision. Partial uploads from an interrupted run are kept for resume until a completed record exists.";
  }
}

function setVersionStatus(text) {
  const el = $("files-version-status");
  if (!el) return;
  el.textContent = text || "";
  el.classList.toggle("hidden", !text);
}

function backupsSignature() {
  return (state.backups || [])
    .map((b) => `${b.key}:${b.size || 0}:${b.isComplete === false ? 0 : 1}`)
    .join("|");
}

function resetVersionCounts() {
  state.versionCounts = {};
  state.versionCountsLoading = false;
  state.versionCountsPath = state.filesPath || "";
  state.versionCountsRequest += 1;
  setVersionStatus("");
}

async function refreshVersionCounts() {
  const path = state.filesPath || "";
  if (demo || !invoke || !state.config?.bucket || state.filesRecordKey === "partial") {
    resetVersionCounts();
    renderFiles();
    return;
  }
  const cacheKey = `${backupsSignature()}::${path}`;
  if (state.versionCountsCache.has(cacheKey)) {
    state.versionCounts = state.versionCountsCache.get(cacheKey);
    state.versionCountsPath = path;
    state.versionCountsLoading = false;
    state.versionCountsRequest += 1;
    setVersionStatus("");
    renderFiles();
    return;
  }
  const requestId = ++state.versionCountsRequest;
  state.versionCounts = {};
  state.versionCountsPath = path;
  state.versionCountsLoading = true;
  const backups = state.backups.length;
  setVersionStatus(`Counting versions across ${backups} completed backup${backups === 1 ? "" : "s"}…`);
  renderFiles();
  try {
    const result = await invoke("backup_version_counts", { path });
    if (requestId !== state.versionCountsRequest || state.filesPath !== path) return;
    const counts = {};
    for (const entry of result.counts || []) {
      counts[entry.path] = Number(entry.versions || 0);
    }
    state.versionCounts = counts;
    state.versionCountsCache.set(cacheKey, counts);
    if (state.versionCountsCache.size > 80) {
      const oldest = state.versionCountsCache.keys().next().value;
      state.versionCountsCache.delete(oldest);
    }
    setVersionStatus("");
  } catch (_) {
    if (requestId === state.versionCountsRequest) {
      state.versionCounts = {};
      setVersionStatus("Version counts are unavailable.");
    }
  } finally {
    if (requestId === state.versionCountsRequest) {
      state.versionCountsLoading = false;
      renderFiles();
    }
  }
}


function renderFilesBreadcrumb() {
  const el = $("files-breadcrumb");
  if (!el) return;
  const parts = state.filesPath ? state.filesPath.split("/") : [];
  let html = '<button class="link-button" type="button" data-files-path="">Backup root</button>';
  let accumulated = "";
  for (const part of parts) {
    accumulated = accumulated ? `${accumulated}/${part}` : part;
    html += `<span class="crumb-sep">/</span><button class="link-button" type="button" data-files-path="${esc(accumulated)}">${esc(part)}</button>`;
  }
  el.innerHTML = html;
}

function renderFiles() {
  const body = $("files-table")?.querySelector("tbody");
  if (!body) return;
  updateFilesNote();
  const partial = state.filesRecordKey === "partial";
  const actionBusy = !!state.filesActionBusy;
  const restoreBlocked = !!state.status?.running || !!state.status?.restoring || !!state.status?.editing || actionBusy;
  $("files-restore-all").disabled = restoreBlocked;
  $("files-restore-all").title = actionBusy || state.status?.editing
    ? "Wait for the current file action to finish"
    : restoreBlocked ? "Stop the active backup or restore before restoring" : "";
  const selectedRecord = partial
    ? null
    : (state.backups.find((entry) => entry.key === state.filesRecordKey) || state.backups[0]);
  if (!partial && selectedRecord) {
    $("files-restore-all").title = `Restore every file from the ${fmtDate(selectedRecord.timestamp)} backup`;
  }
  if ($("files-revision-date")) {
    $("files-revision-date").textContent = partial
      ? (state.status?.running ? "In progress" : "Stopped run")
      : (selectedRecord ? fmtDate(selectedRecord.timestamp) : "—");
    $("files-revision-files").textContent = partial
      ? (state.status?.running ? `${Number(state.status?.files || 0).toLocaleString()} so far` : "—")
      : (selectedRecord ? Number(selectedRecord.filesCount || 0).toLocaleString() : "—");
    $("files-revision-uploaded").textContent = partial
      ? (state.status?.running ? fmtBytes(state.status?.bytesUploaded) : "—")
      : (selectedRecord ? fmtBytes(selectedRecord.bytesUploaded) : "—");
  }
  if ($("files-refresh")) $("files-refresh").disabled = actionBusy;
  if ($("files-record")) $("files-record").disabled = actionBusy;
  $("files-up").disabled = !state.filesPath || actionBusy;
  const removeIgnoredBlocked = partial || restoreBlocked;
  if ($("files-remove-ignored")) {
    $("files-remove-ignored").disabled = removeIgnoredBlocked;
    $("files-remove-ignored").title = partial
      ? "Partial backups have no completed record to edit"
      : restoreBlocked
        ? "Stop the active backup or restore before removing files"
        : "Remove every ignored path from this revision (authentication required)";
  }
  if ($("files-ignored-dir")) {
    $("files-ignored-dir").classList.toggle("hidden", !state.filesListingIgnored);
  }
  if ($("files-ignored-only") && $("files-ignored-only").checked !== state.filesIgnoredOnly) {
    $("files-ignored-only").checked = state.filesIgnoredOnly;
  }
  renderFilesBreadcrumb();
  if (state.filesLoading) {
    body.innerHTML = '<tr><td colspan="6" class="empty">Loading backup files…</td></tr>';
    return;
  }
  if (state.filesError) {
    body.innerHTML = `<tr><td colspan="6" class="empty">${esc(state.filesError)}</td></tr>`;
    return;
  }
  if (state.filesNotice && !state.filesEntries.length) {
    body.innerHTML = `<tr><td colspan="6" class="empty">${esc(state.filesNotice)}</td></tr>`;
    return;
  }
  const filter = (state.filesFilter || "").trim().toLowerCase();
  let entries = (state.filesEntries || []).filter((e) => !filter || e.name.toLowerCase().includes(filter));
  if (state.filesIgnoredOnly) entries = entries.filter((e) => e.ignored);
  if (!entries.length) {
    let message;
    if (state.filesIgnoredOnly) {
      message = (state.filesEntries || []).some((e) => e.ignored)
        ? "No matching ignored entries."
        : "No ignored entries in this folder.";
    } else {
      message = state.filesEntries.length ? "No matching entries." : "This folder is empty.";
    }
    body.innerHTML = `<tr><td colspan="6" class="empty">${message}</td></tr>`;
    return;
  }
  body.innerHTML = entries.map((entry) => {
    const kind = entry.isDir ? "Folder" : entry.isSymlink ? "Symlink" : "File";
    const icon = entry.isDir ? "▸" : entry.isSymlink ? "↗" : "·";
    const ignoredBadge = entry.ignored
      ? '<span class="ignored-badge" title="This path matches a current ignore rule">Ignored</span>'
      : "";
    const name = entry.isDir
      ? `<button class="link-button" type="button" data-files-open="${esc(entry.path)}">${esc(entry.name)}</button>`
      : `<span>${esc(entry.name)}</span>`;
    const size = entry.isDir ? `${Number(entry.filesCount || 0).toLocaleString()} files` : fmtBytes(entry.size);
    const modified = entry.mtimeSec ? esc(fmtDate(Number(entry.mtimeSec) * 1000)) : "—";
    const versionCount = state.versionCounts[entry.path];
    let versionsCell;
    if (entry.isDir || partial) {
      versionsCell = `<td class="versions-cell muted" title="${partial ? "Version history is available for completed backups" : "Folders do not have individual versions"}">—</td>`;
    } else if (versionCount != null) {
      versionsCell = `<td class="versions-cell"><button class="version-badge" type="button" data-files-versions="${esc(entry.path)}" title="${versionCount} version${versionCount === 1 ? "" : "s"} across completed backups — view and restore">${versionCount}</button></td>`;
    } else if (state.versionCountsLoading && state.versionCountsPath === state.filesPath) {
      versionsCell = '<td class="versions-cell"><span class="version-loading" title="Counting versions across completed backups…">…</span></td>';
    } else {
      versionsCell = '<td class="versions-cell muted">—</td>';
    }
    const inspectBlocked = actionBusy || !!state.status?.editing;
    const inspectTitle = inspectBlocked ? "Wait for the current file action to finish" : "";
    const canInspect = !entry.isDir && !entry.isSymlink;
    const showContent = canInspect
      ? (inspectBlocked
          ? `<button class="link-button" type="button" disabled title="${inspectTitle}">Show content</button>`
          : `<button class="link-button" type="button" data-files-content="${esc(entry.path)}" title="Show the file content">Show content</button>`)
      : "";
    const showDiff = canInspect
      ? (inspectBlocked
          ? `<button class="link-button" type="button" disabled title="${inspectTitle}">Diff</button>`
          : `<button class="link-button" type="button" data-files-diff="${esc(entry.path)}" title="${partial ? "Compare this partial file with the latest completed backup" : "Compare this file with the previous revision"}">Diff</button>`)
      : "";
    const restore = restoreBlocked
      ? '<button class="link-button" type="button" disabled title="Stop the active backup or restore before restoring">Restore</button>'
      : `<button class="link-button" type="button" data-files-restore="${esc(entry.path)}" title="Choose which version to restore">Restore</button>`;
    const ignore = actionBusy
      ? '<button class="link-button" type="button" disabled title="Wait for the current file action to finish">Ignore</button>'
      : `<button class="link-button" type="button" data-files-ignore="${esc(entry.path)}">Ignore</button>`;
    const removeBlocked = partial || restoreBlocked;
    const removeTitle = partial
      ? "Partial backups have no completed record to edit"
      : actionBusy
        ? "Wait for the current file action to finish"
        : "Stop the active backup or restore before removing files";
    const remove = removeBlocked
      ? `<button class="link-button" type="button" disabled title="${removeTitle}">Remove</button>`
      : `<button class="link-button" type="button" data-files-remove="${esc(entry.path)}">Remove</button>`;
    return `<tr${entry.ignored ? ' class="files-ignored-row"' : ""}>
      <td><div class="files-name"><span class="files-icon${entry.isDir ? "" : " file"}">${icon}</span>${name}${ignoredBadge}</div></td>
      <td>${kind}</td>
      <td>${size}</td>
      <td>${modified}</td>
      ${versionsCell}
      <td><div class="files-actions">${showContent}${showDiff}${restore}${ignore}${remove}</div></td>
    </tr>`;
  }).join("");
}

async function refreshFiles() {
  if (demo || !invoke || !state.config?.bucket) {
    state.filesEntries = [];
    state.filesListingIgnored = false;
    state.filesLoading = false;
    state.filesError = demo
      ? "Preview mode — the file explorer needs the macOS build."
      : "Configure the S3 destination in Settings first.";
    state.filesNotice = "";
    resetVersionCounts();
    renderFilesRecords();
    renderFiles();
    return;
  }
  const requestId = ++state.filesRequest;
  state.filesLoading = true;
  state.filesEntries = [];
  state.filesListingIgnored = false;
  state.filesError = "";
  state.filesNotice = "";
  resetVersionCounts();
  renderFiles();
  // Always re-read the record list. A stale in-memory backup list must never
  // make deleted records or a cleared bucket look like they still exist.
  await refreshBackups();
  if (requestId !== state.filesRequest) return;

  const partialAvailable = !!state.status?.partialAvailable;
  if (!state.filesRecordKey && state.status?.running && partialAvailable) {
    state.filesRecordKey = "partial";
  }
  renderFilesRecords();
  if (state.filesRecordKey === "partial") {
    if (!partialAvailable) {
      state.filesRecordKey = "";
      renderFilesRecords();
    } else {
      renderFiles();
      try {
        const result = await invoke("list_partial_entries", { path: state.filesPath });
        if (requestId !== state.filesRequest) return;
        state.filesEntries = result.entries || [];
        state.filesListingIgnored = !!result.ignored;
        state.filesResolvedRecord = "partial";
        state.filesPath = result.path || state.filesPath;
        if (!state.filesEntries.length) {
          state.filesNotice = "No files completed in this folder yet. Click Refresh to see newer files.";
        }
      } catch (e) {
        if (requestId !== state.filesRequest) return;
        const message = String(e);
        state.filesEntries = [];
        state.filesListingIgnored = false;
        state.filesError = message;
        if (message.toLowerCase().includes("partial backup")) {
          state.status = { ...state.status, partialAvailable: false };
          state.filesRecordKey = "";
          state.filesPath = "";
          renderFilesRecords();
        }
      } finally {
        if (requestId === state.filesRequest) {
          state.filesLoading = false;
          renderFiles();
        }
      }
      return;
    }
  }
  if (!state.backups.length) {
    state.filesLoading = false;
    state.filesError = "";
    state.filesNotice = state.status?.running
      ? "A backup is running. Partial files will appear as the first directories complete."
      : "No backup data in the destination. Run a backup first.";
    resetVersionCounts();
    renderFiles();
    return;
  }
  renderFiles();
  try {
    const result = await invoke("list_backup_entries", {
      recordKey: state.filesRecordKey || null,
      path: state.filesPath,
    });
    if (requestId !== state.filesRequest) return;
    state.filesEntries = result.entries || [];
    state.filesListingIgnored = !!result.ignored;
    state.filesResolvedRecord = result.recordKey || "";
    state.filesPath = result.path || state.filesPath;
  } catch (e) {
    if (requestId !== state.filesRequest) return;
    state.filesEntries = [];
    state.filesListingIgnored = false;
    state.filesError = String(e);
  } finally {
    if (requestId === state.filesRequest) {
      state.filesLoading = false;
      renderFiles();
      if (!state.filesError) refreshVersionCounts();
    }
  }
}

function openFilesDir(path) {
  state.filesPath = path || "";
  state.filesFilter = "";
  if ($("files-filter")) $("files-filter").value = "";
  refreshFiles();
}

async function ignoreFilesEntry(path) {
  if (demo || !invoke) return toast("Preview mode — file actions are not available.", true);
  if (!path) return;
  const ok = await confirmAction({
    title: "Ignore this path?",
    message: `Ignore "${path}" in future backups?`,
    detail: "Existing backups are not changed. You can remove the exclude pattern later in Settings.",
    confirmLabel: "Ignore path",
  });
  if (!ok) return;
  try {
    const added = await invoke("ignore_backup_entry", { path });
    if (added && added.length) {
      if (state.config) {
        state.config.excludePatterns = [
          ...(state.config.excludePatterns || []),
          ...added.filter((pattern) => !(state.config.excludePatterns || []).includes(pattern)),
        ];
        if ($("excludes")) $("excludes").value = state.config.excludePatterns.join("\n");
      }
      for (const entry of state.filesEntries || []) {
        if (entry.path === path) entry.ignored = true;
      }
      renderFiles();
      toast(`Ignored ${path} in future backups.`);
    } else {
      toast(`${path} is already ignored.`);
    }
  } catch (e) {
    toast(String(e), true);
  }
}

async function removeFilesEntry(path) {
  if (demo || !invoke) return toast("Preview mode — file actions are not available.", true);
  if (!path) return;
  if (state.filesRecordKey === "partial") {
    return toast("Partial backups have no completed record to edit.", true);
  }
  if (state.status?.running || state.status?.restoring || state.status?.editing || state.filesActionBusy) {
    return toast("Stop the active backup or restore before removing files.", true);
  }
  const authed = await authorizeSensitive("remove");
  if (!authed) return;
  const ok = await confirmAction({
    title: "Remove from this revision?",
    message: `Remove "${path}" from this backup revision?`,
    detail: "This cannot be undone. Other backup revisions keep their copy. Use Ignore to skip it in future backups.",
    confirmLabel: "Remove",
    danger: true,
  });
  if (!ok) {
    await clearSensitiveAuthorization();
    return;
  }
  state.filesActionBusy = true;
  renderFiles();
  try {
    await invoke("remove_backup_entry", {
      recordKey: state.filesRecordKey || null,
      path,
    });
    const parent = filesParentPath(path);
    if (state.filesPath === path || state.filesPath.startsWith(`${path}/`)) {
      state.filesPath = parent;
      state.filesFilter = "";
      if ($("files-filter")) $("files-filter").value = "";
    }
    toast(`Removed ${path} from this backup revision.`);
    await refreshFiles();
  } catch (e) {
    toast(String(e), true);
  } finally {
    state.filesActionBusy = false;
    renderFiles();
  }
}

async function removeIgnoredFiles() {
  if (demo || !invoke) return toast("Preview mode — file actions are not available.", true);
  if (state.filesRecordKey === "partial") {
    return toast("Partial backups have no completed record to edit.", true);
  }
  if (state.status?.running || state.status?.restoring || state.status?.editing || state.filesActionBusy) {
    return toast("Stop the active backup or restore before removing files.", true);
  }
  const authed = await authorizeSensitive("remove");
  if (!authed) return;
  const ok = await confirmAction({
    title: "Remove all ignored paths?",
    message: "Remove every path that matches the current ignore rules from this backup revision?",
    detail: "This cannot be undone. Other backup revisions keep their copies. The ignore rules themselves are not changed.",
    confirmLabel: "Remove all ignored",
    danger: true,
  });
  if (!ok) {
    await clearSensitiveAuthorization();
    return;
  }
  state.filesActionBusy = true;
  renderFiles();
  try {
    const summary = await invoke("remove_ignored_entries", {
      recordKey: state.filesRecordKey || null,
    });
    const removed = Number(summary?.removedEntries || 0);
    const files = Number(summary?.removedFiles || 0);
    if (removed > 0) {
      if (state.filesPath && state.filesEntries.every((entry) => entry.ignored)) {
        state.filesPath = "";
        state.filesFilter = "";
        if ($("files-filter")) $("files-filter").value = "";
      }
      toast(`Removed ${removed} ignored ${removed === 1 ? "entry" : "entries"} (${files} ${files === 1 ? "file" : "files"}) from this backup revision.`);
      await refreshFiles();
    } else {
      toast("No ignored entries remain in this revision.");
    }
  } catch (e) {
    toast(String(e), true);
  } finally {
    state.filesActionBusy = false;
    renderFiles();
  }
}

function restoreDestinationValue() {
  return ($("files-dest").value || "").trim() || state.restoreDestination;
}

function renderRestoreVersions() {
  const list = $("restore-versions");
  const confirm = $("restore-confirm");
  if (!list || !confirm) return;
  if (state.restoreLoading) {
    list.innerHTML = '<div class="version-empty">Loading versions…</div>';
    confirm.disabled = true;
    return;
  }
  if (state.restoreError) {
    list.innerHTML = `<div class="version-empty">${esc(state.restoreError)}</div>`;
    confirm.disabled = true;
    return;
  }
  const versions = state.restoreVersions || [];
  if (!versions.length) {
    list.innerHTML = '<div class="version-empty">No versions of this path were found in completed backups.</div>';
    confirm.disabled = true;
    return;
  }
  if (!versions.some((version) => version.partial)) {
    $("restore-meta").textContent = `${versions.length} version${versions.length === 1 ? "" : "s"} available. Pick the point in time you want this path restored from.`;
  }
  list.innerHTML = versions.map((version, index) => {
    const selected = version.recordKey === state.restoreSelectedKey;
    if (version.partial) {
      return `<label class="version-row selected">
        <input type="radio" name="restore-version" value="partial" checked />
        <span class="version-main">
          <span class="version-date">Partial backup (current app session)</span>
          <span class="version-detail">Restores content already uploaded; files still queued may be skipped.</span>
        </span>
      </label>`;
    }
    const size = version.isDir ? "Folder" : fmtBytes(version.size);
    const occurrences = Number(version.occurrences || 1);
    const label = index === 0 ? "Latest" : `Version ${versions.length - index}`;
    const viewing = selected && index !== 0 ? '<span class="pill ok">Viewing</span>' : "";
    return `<label class="version-row${selected ? " selected" : ""}">
      <input type="radio" name="restore-version" value="${esc(version.recordKey)}" ${selected ? "checked" : ""} />
      <span class="version-main">
        <span class="version-date">${esc(fmtDate(version.timestamp))}</span>
        <span class="version-detail">${size} · seen in ${occurrences} backup${occurrences === 1 ? "" : "s"}</span>
      </span>
      <span class="version-badges"><span class="pill">${label}</span>${viewing}</span>
    </label>`;
  }).join("");
  confirm.disabled = !state.restoreSelectedKey;
}

async function pickRestoreDestination(inputId) {
  if (demo) return toast("Preview mode — use the macOS app to choose a folder.");
  try {
    const current = ($(inputId)?.value || "").trim();
    const picked = await invoke("pick_restore_destination", { current: current || null });
    if (picked) $(inputId).value = picked;
  } catch (e) {
    toast(String(e), true);
  }
}

function closeRestoreDialog() {
  state.restoreRequest += 1;
  $("restore-modal").classList.add("hidden");
}

async function openRestoreDialog(path) {
  if (demo || !invoke) return toast("Preview mode — restore is not available.", true);
  const partial = state.filesRecordKey === "partial";
  if (partial && state.status?.running) {
    return toast("Stop the backup before restoring from partial data.", true);
  }
  const requestId = ++state.restoreRequest;
  state.restorePath = path || "";
  state.restorePartial = partial;
  state.restoreVersions = [];
  state.restoreSelectedKey = "";
  state.restoreError = "";
  state.restoreLoading = true;
  $("restore-modal").classList.remove("hidden");
  $("restore-title").textContent = partial ? "Restore from partial backup" : "Choose a version to restore";
  $("restore-meta").textContent = partial
    ? "Only content that has already been uploaded can be restored."
    : "Pick the point in time you want this path restored from.";
  $("restore-target").textContent = path || "Entire backup";
  $("restore-dest").value = restoreDestinationValue();
  renderRestoreVersions();

  if (partial) {
    state.restoreVersions = [{
      recordKey: "partial",
      timestamp: null,
      size: 0,
      occurrences: 1,
      selected: true,
      partial: true,
      isDir: false,
    }];
    state.restoreSelectedKey = "partial";
    state.restoreLoading = false;
    renderRestoreVersions();
    window.setTimeout(() => {
      $("restore-versions")?.querySelector('input:checked')?.focus();
    }, 0);
    return;
  }

  try {
    const result = await invoke("backup_file_versions", {
      path: state.restorePath,
      atRecord: state.filesRecordKey || null,
    });
    if (requestId !== state.restoreRequest) return;
    state.restoreVersions = result.versions || [];
    const selected = state.restoreVersions.find((version) => version.selected) || state.restoreVersions[0];
    state.restoreSelectedKey = selected ? selected.recordKey : "";
  } catch (e) {
    if (requestId !== state.restoreRequest) return;
    state.restoreError = String(e);
  } finally {
    if (requestId === state.restoreRequest) {
      state.restoreLoading = false;
      renderRestoreVersions();
      window.setTimeout(() => {
        $("restore-versions")?.querySelector('input:checked')?.focus();
      }, 0);
    }
  }
}

async function restoreSelectedVersion() {
  if (demo || !invoke) return toast("Preview mode — restore is not available.", true);
  if (!state.restoreSelectedKey) return toast("Choose a version first.", true);
  const destination = ($("restore-dest").value || "").trim() || state.restoreDestination;
  if (!destination) return toast("Choose a destination folder first.", true);
  const path = state.restorePath || "";
  const partial = state.restorePartial;
  const version = (state.restoreVersions || []).find(
    (item) => item.recordKey === state.restoreSelectedKey
  );
  const when = partial
    ? "the partial backup"
    : (version?.timestamp ? fmtDate(version.timestamp) : "the selected version");
  const ok = await confirmAction({
    title: "Restore this version?",
    message: `Restore ${path || "the entire backup"} from ${when}?`,
    detail: `Destination: ${destination}\nExisting files may be overwritten.`,
    confirmLabel: "Restore",
  });
  if (!ok) return;
  $("files-dest").value = destination;
  closeRestoreDialog();
  $("files-progress").classList.remove("hidden");
  $("files-phase").textContent = path ? `Restoring ${path}…` : "Restoring backup…";
  $("files-stats").textContent = "";
  $("files-cancel").classList.remove("hidden");
  try {
    if (partial) {
      await invoke("restore_partial_entry", { path, destination });
    } else {
      await invoke("restore_backup_entry", {
        recordKey: state.restoreSelectedKey,
        path,
        destination,
      });
    }
  } catch (e) {
    toast(String(e), true);
    $("files-progress").classList.add("hidden");
    $("files-cancel").classList.add("hidden");
  }
}

async function restoreEntireFiles() {
  if (demo || !invoke) return toast("Preview mode — restore is not available.", true);
  const partial = state.filesRecordKey === "partial";
  if (partial && state.status?.running) {
    return toast("Stop the backup before restoring from partial data.", true);
  }
  if (!partial && !state.backups.length) return toast("No completed backup is available yet.", true);
  const destination = restoreDestinationValue();
  if (!destination) return toast("Choose a destination folder first.", true);
  const record = partial
    ? null
    : (state.backups.find((entry) => entry.key === state.filesRecordKey) || state.backups[0]);
  const when = partial ? "the partial backup" : (record ? fmtDate(record.timestamp) : "latest backup");
  const ok = await confirmAction({
    title: "Restore entire backup?",
    message: `Restore every file from ${when}?`,
    detail: `Destination: ${destination}\nExisting files may be overwritten.`,
    confirmLabel: "Restore everything",
  });
  if (!ok) return;
  $("files-progress").classList.remove("hidden");
  $("files-phase").textContent = partial ? "Restoring partial backup…" : "Starting whole backup restore…";
  $("files-stats").textContent = "";
  $("files-cancel").classList.remove("hidden");
  try {
    if (partial) {
      await invoke("restore_partial_entry", { path: "", destination });
    } else {
      await invoke("restore_backup_entry", {
        recordKey: state.filesRecordKey || null,
        path: "",
        destination,
      });
    }
  } catch (e) {
    toast(String(e), true);
    $("files-progress").classList.add("hidden");
    $("files-cancel").classList.add("hidden");
  }
}

function diffKindPill(change) {
  const label = change === "added" ? "added" : change === "modified" ? "modified" : "deleted";
  return `<span class="pill ${esc(change)}">${label}</span>`;
}

function renderDiff() {
  const d = state.diff;
  if (!d) return;
  const filter = (state.diffFilter || "").trim().toLowerCase();
  const kind = $("diff-kind").value;
  const entries = (d.entries || []).filter((entry) => {
    if (kind && entry.change !== kind) return false;
    return !filter || String(entry.path || "").toLowerCase().includes(filter);
  });
  const body = $("diff-table").querySelector("tbody");
  if (!entries.length) {
    body.innerHTML = `<tr><td colspan="5" class="empty">${d.entries?.length ? "No changes match this filter." : "No changes between these records."}</td></tr>`;
  } else {
    body.innerHTML = entries.map((entry) => {
      const previous = entry.change === "modified" && entry.previousSize != null
        ? ` <span class="muted">(was ${fmtBytes(entry.previousSize)})</span>`
        : "";
      const diffAction = entry.kind === "file"
        ? `<button class="link-button" type="button" data-diff-file="${esc(entry.path)}">Side-by-side</button>`
        : "";
      return `<tr>
        <td>${diffKindPill(entry.change)}</td>
        <td>${esc(entry.kind)}</td>
        <td>${fmtBytes(entry.size)}${previous}</td>
        <td class="mono path">${esc(entry.path)}</td>
        <td>${diffAction}</td>
      </tr>`;
    }).join("");
  }
  $("diff-counts").textContent = `${Number(d.added || 0).toLocaleString()} added · ${Number(d.modified || 0).toLocaleString()} modified · ${Number(d.deleted || 0).toLocaleString()} deleted${d.truncated ? " · truncated" : ""}`;
}

async function openDiff(key) {
  if (demo) {
    toast("Preview mode — run the macOS app to inspect backup records.");
    return;
  }
  state.diff = null;
  state.diffRecord = key;
  state.diffFilter = "";
  $("diff-filter").value = "";
  $("diff-kind").value = "";
  $("diff-modal").classList.remove("hidden");
  $("diff-meta").textContent = "Loading record trees from S3…";
  $("diff-counts").textContent = "";
  $("diff-table").querySelector("tbody").innerHTML =
    '<tr><td colspan="5" class="empty">Loading changes…</td></tr>';
  try {
    const result = await invoke("diff_backup", { record: key, previous: null, limit: 5000 });
    state.diff = result;
    $("diff-meta").textContent = result.previousRecordKey
      ? `Comparing ${fmtDate(result.recordTimestamp)} with ${fmtDate(result.previousTimestamp)}.`
      : "No older record found; every entry is shown as added.";
    renderDiff();
  } catch (e) {
    $("diff-meta").textContent = "Could not load changes.";
    $("diff-table").querySelector("tbody").innerHTML =
      `<tr><td colspan="5" class="empty">${esc(e)}</td></tr>`;
  }
}

function closeDiff() {
  $("diff-modal").classList.add("hidden");
  state.diff = null;
  state.diffRecord = null;
}

function closeFileDiff() {
  state.fileDiffRequest += 1;
  $("file-diff-modal").classList.add("hidden");
  state.fileDiff = null;
}

function closeFileContent() {
  state.contentRequest += 1;
  $("content-modal").classList.add("hidden");
}

function renderFileContent(content, context = {}) {
  const partial = context.partial ?? (state.filesRecordKey === "partial");
  const recordKey = context.recordKey ?? state.filesRecordKey;
  const record = state.backups.find((entry) => entry.key === recordKey) || state.backups[0];
  const where = partial
    ? (state.status?.running ? "in-progress backup" : "partial backup")
    : (record ? `revision ${fmtDate(record.timestamp)}` : "latest revision");
  $("content-title").textContent = content.path;
  $("content-meta").textContent = `${fmtBytes(content.size)} · ${where}`;
  const note = $("content-note");
  if (content.message) {
    note.textContent = content.message;
    note.classList.remove("hidden");
  } else {
    note.textContent = "";
    note.classList.add("hidden");
  }
  if (content.text == null) {
    const excluded = content.binary || content.tooLarge;
    $("content-body").innerHTML = `<div class="diff-unavailable">
      <div class="diff-unavailable-icon" aria-hidden="true">…</div>
      <p>${esc(content.message || "This file has no text content to show.")}</p>
      ${excluded ? '<span class="muted small">Binary and large files are intentionally excluded from previews.</span>' : ""}
    </div>`;
    return;
  }
  const lines = content.text.split("\n");
  if (lines.length && lines[lines.length - 1] === "") lines.pop();
  if (lines.length === 1 && lines[0] === "") lines.length = 0;
  if (!lines.length) {
    $("content-body").innerHTML = '<div class="version-empty">This file is empty.</div>';
    return;
  }
  const rows = lines.map((line, index) =>
    `<tr><td class="ln" aria-hidden="true">${index + 1}</td><td class="code">${line === "" ? "&nbsp;" : esc(line)}</td></tr>`
  ).join("");
  $("content-meta").textContent += ` · ${lines.length.toLocaleString()} line${lines.length === 1 ? "" : "s"}`;
  $("content-body").innerHTML = `<table class="content-lines"><tbody>${rows}</tbody></table>`;
}

async function openFileContent(path) {
  if (demo || !invoke) return toast("Preview mode — file content needs the macOS build.", true);
  const partial = state.filesRecordKey === "partial";
  const recordKey = state.filesRecordKey || null;
  const requestId = ++state.contentRequest;
  $("content-modal").classList.remove("hidden");
  $("content-title").textContent = path;
  $("content-meta").textContent = "Loading file content…";
  $("content-note").textContent = "";
  $("content-note").classList.add("hidden");
  $("content-body").innerHTML = '<div class="version-empty">Loading content…</div>';
  try {
    const result = partial
      ? await invoke("read_partial_file_content", { path })
      : await invoke("read_backup_file_content", {
          recordKey,
          path,
        });
    if (requestId !== state.contentRequest) return;
    renderFileContent(result, { partial, recordKey });
  } catch (e) {
    if (requestId !== state.contentRequest) return;
    $("content-meta").textContent = "Could not load file content.";
    $("content-body").innerHTML = `<div class="version-empty">${esc(e)}</div>`;
  }
}

function renderFileDiff(diff) {
  const partial = diff.recordKey === "partial";
  const running = !!state.status?.running;
  const partialLabel = running ? "in-progress" : "partial";
  const leftLabel = diff.previousTimestamp
    ? `${partial ? "Completed" : "Older"} — ${fmtDate(diff.previousTimestamp)}`
    : (partial ? "No completed baseline" : "Older version");
  const rightLabel = partial ? `${running ? "In-progress" : "Partial"} — current` : `Newer — ${fmtDate(diff.recordTimestamp)}`;
  $("file-diff-title").textContent = diff.path;
  $("file-diff-meta").textContent = partial
    ? (diff.previousTimestamp
        ? `Comparing the latest completed backup (${fmtDate(diff.previousTimestamp)}) with the ${partialLabel} backup.`
        : `No completed backup exists; the ${partialLabel} file is shown as added.`)
    : (diff.previousTimestamp
        ? `Comparing ${fmtDate(diff.previousTimestamp)} with ${fmtDate(diff.recordTimestamp)}.`
        : "No older version exists; the file is shown as added.");
  const note = $("file-diff-note");
  const rows = diff.rows || [];
  if (!rows.length) {
    note.textContent = "";
    note.classList.add("hidden");
    $("file-diff-legend").classList.add("hidden");
    const excluded = diff.binary || /too large/i.test(diff.message || "");
    $("file-diff-body").innerHTML = `<div class="diff-unavailable">
      <div class="diff-unavailable-icon" aria-hidden="true">≠</div>
      <p>${esc(diff.message || "There is no text content to show for this file.")}</p>
      ${excluded ? '<span class="muted small">Binary and large files are intentionally excluded from side-by-side diffs.</span>' : ""}
    </div>`;
    return;
  }
  $("file-diff-legend").classList.remove("hidden");
  if (diff.message) {
    note.textContent = diff.message;
    note.classList.remove("hidden");
  } else if (partial) {
    note.textContent = running
      ? "The in-progress side reflects files completed so far and may still change."
      : "The partial side reflects files captured by the interrupted run.";
    note.classList.remove("hidden");
  } else {
    note.textContent = "";
    note.classList.add("hidden");
  }
  const added = rows.filter((row) => row.kind !== "same" && row.right != null).length;
  const removed = rows.filter((row) => row.kind !== "same" && row.left != null).length;
  if (added || removed) {
    $("file-diff-meta").textContent += ` · +${added} −${removed}`;
  }
  const cell = (text) => (text == null || text === "" ? "&nbsp;" : esc(text));
  const rowHtml = diff.rows.map((row) => {
    const kind = row.kind || "same";
    return `<tr class="${esc(kind)}">
      <td class="ln ln-left${row.left == null ? " empty" : ""}">${row.leftNumber ?? ""}</td>
      <td class="code left-code${row.left == null ? " empty" : ""}">${cell(row.left)}</td>
      <td class="ln ln-right${row.right == null ? " empty" : ""}">${row.rightNumber ?? ""}</td>
      <td class="code right-code${row.right == null ? " empty" : ""}">${cell(row.right)}</td>
    </tr>`;
  }).join("");
  $("file-diff-body").innerHTML =
    `<table class="side-diff"><colgroup><col class="diff-ln-col" /><col /><col class="diff-ln-col" /><col /></colgroup><thead><tr><th colspan="2">${esc(leftLabel)}</th><th colspan="2">${esc(rightLabel)}</th></tr></thead><tbody>${rowHtml}</tbody></table>`;
}

async function openFileDiff(path, options = {}) {
  if (demo || !invoke) return toast("Preview mode — the file diff needs the macOS build.", true);
  const partial = !!options.partial;
  const record = options.record ?? state.diff?.recordKey;
  const previous = Object.prototype.hasOwnProperty.call(options, "previous")
    ? options.previous
    : (state.diff?.previousRecordKey ?? null);
  if (!partial && !record) return;
  const requestId = ++state.fileDiffRequest;
  state.fileDiff = null;
  $("file-diff-modal").classList.remove("hidden");
  $("file-diff-title").textContent = path;
  $("file-diff-meta").textContent = "Loading both versions…";
  $("file-diff-note").textContent = "";
  $("file-diff-note").classList.add("hidden");
  $("file-diff-legend").classList.remove("hidden");
  $("file-diff-body").innerHTML = '<div class="version-empty">Loading file contents…</div>';
  try {
    const result = partial
      ? await invoke("diff_partial_file", { path })
      : await invoke("diff_backup_file", { record, previous, path });
    if (requestId !== state.fileDiffRequest) return;
    state.fileDiff = result;
    renderFileDiff(result);
  } catch (e) {
    if (requestId !== state.fileDiffRequest) return;
    $("file-diff-meta").textContent = "Could not load the file diff.";
    $("file-diff-body").innerHTML = `<div class="version-empty">${esc(e)}</div>`;
  }
}

async function refreshLogs() {
  if (demo) return;
  try {
    state.logs = await invoke("get_logs", { lines: 400 });
  } catch (_) { /* keep existing */ }
  renderLogs();
}

async function refreshSnapshots() {
  if (demo) return;
  try {
    const res = await invoke("list_snapshots");
    state.snapshots = res.snapshots || [];
    $("snapshot-summary").textContent = `${state.snapshots.length} local snapshot(s)`;
  } catch (e) {
    $("snapshot-summary").textContent = String(e);
  }
}

async function refreshDestinationCheck() {
  if (demo || !invoke || !state.config?.bucket) {
    state.destinationCheck = null;
    renderBanner();
    return;
  }
  try {
    state.destinationCheck = await invoke("check_destination");
  } catch (_) {
    // A transient network/credential error is reported by Test connection.
    state.destinationCheck = null;
  }
  renderBanner();
}

async function loadState({ quiet = false } = {}) {
  if (demo) {
    state.config = demoConfig();
    state.status = demoStatus();
    state.logs = demoLogs();
    state.backups = demoBackups();
    state.configPath = "~/Library/Application Support/Qia Backup/config.json";
    banner("Preview mode: run the macOS Tauri build to connect to AWS S3. Settings are editable but not saved.");
    applyConfig(state.config);
    renderLogs();
    renderBackups();
    renderEstimate();
    updateStatus();
    return;
  }
  try {
    const boot = await invoke("get_state");
    state.config = boot.config;
    state.hasSecretAccessKey = !!boot.hasSecretAccessKey;
    state.hasEncryptionPassword = !!boot.hasEncryptionPassword;
    state.status = boot.status;
    state.logs = boot.logs || [];
    state.configPath = boot.configPath;
    state.restoreDestination = boot.restoreDestination || "";
    state.version = boot.version || state.version;
    state.snapshots = boot.snapshots || [];
    if (!$("files-dest").value) $("files-dest").value = state.restoreDestination;
    applyConfig(boot.config);
    renderLogs();
    updateStatus();
    updateDashboard();
    refreshDestinationCheck();
    await refreshBackups();
    refreshEstimate();
    if (!quiet) await refreshLogs();
  } catch (e) {
    banner(String(e), true);
  }
}

async function poll() {
  if (demo || !invoke) return;
  if (poll._busy) return;
  poll._busy = true;
  try {
    const res = await invoke("get_status");
    state.status = res.status || {};
    state.logs = res.logs || state.logs;
    updateStatus();
    renderLogs();
    if (state.status.restoring) {
      $("files-progress").classList.remove("hidden");
      $("files-phase").textContent = state.status.detail || "Restoring…";
      $("files-stats").textContent = `${Number(state.status.files || 0).toLocaleString()} files`;
      $("files-cancel").classList.remove("hidden");
      $("files-restore-all").disabled = true;
    } else if (state.status.editing) {
      $("files-progress").classList.remove("hidden");
      $("files-phase").textContent = state.status.detail || "Updating the backup…";
      $("files-stats").textContent = "Removing the entry and cleaning up unreferenced data…";
      $("files-cancel").classList.add("hidden");
      $("files-restore-all").disabled = true;
    } else {
      $("files-progress").classList.add("hidden");
      $("files-cancel").classList.add("hidden");
      $("files-restore-all").disabled = !!state.status.running || !!state.status.restoring || !!state.status.editing;
    }
  } catch (_) {
    /* transient errors are expected while the app starts */
  } finally {
    poll._busy = false;
  }
}

async function setupEvents() {
  if (!listen) return;
  const handlers = {
    "backup-progress": (e) => {
      const p = e.payload || {};
      const next = { ...state.status, running: true };
      if (p.kind === "phase") {
        if (!next.cancelRequested) {
          next.phase = p.name || "";
          next.detail = p.detail || "";
        }
      } else if (p.kind === "totals") {
        next.totalFiles = Number(p.files || 0);
        next.totalBytes = Number(p.bytes || 0);
      } else if (p.kind === "file") {
        next.files = Number(p.files || 0);
        next.bytesScanned = Number(p.bytesScanned ?? p.bytes_scanned ?? 0);
      } else if (p.kind === "upload") {
        next.bytesUploaded = Number(next.bytesUploaded || 0) + Number(p.bytes || 0);
      } else if (p.kind === "transfer") {
        next.uploadSpeedBps = Number(p.speedBps || next.uploadSpeedBps || 0);
        next.uploadTargetThreads = Number(p.targetThreads || 0);
        next.uploadMaxThreads = Number(p.maxThreads || 0);
        next.uploadInFlight = Number(p.inFlight || 0);
      }
      state.status = next;
      updateStatus();
    },
    "backup-finished": async () => {
      const wasPartial = state.filesRecordKey === "partial";
      await loadState({ quiet: true });
      if (wasPartial && $("view-files")?.classList.contains("active")) {
        state.filesEntries = [];
        state.filesError = "";
        state.filesNotice = "Backup finished. Click Refresh to load the completed backup.";
        renderFiles();
      }
    },
    "backup-failed": (e) => {
      toast(String(e.payload || "Backup failed"), true);
    },
    "backup-cancelled": () => {
      toast("Backup cancelled");
    },
    "backup-paused": async (e) => {
      toast(String(e.payload || "Backup paused for power conditions"));
      if (!demo && invoke) {
        try {
          const res = await invoke("get_status");
          state.status = res.status || state.status;
          updateStatus();
        } catch (_) { /* poll will retry */ }
      }
    },
    "restore-progress": (e) => {
      const p = e.payload || {};
      const detail = p.path ? `Restoring ${p.path}` : (p.detail || p.name || "Restoring…");
      $("files-progress").classList.remove("hidden");
      $("files-phase").textContent = detail;
      if (p.size) $("files-stats").textContent = fmtBytes(p.size);
    },
    "restore-finished": (e) => {
      const s = e.payload || {};
      toast(`Restored ${s.files || 0} files to ${s.destination || "destination"}`);
      $("files-progress").classList.add("hidden");
      $("files-cancel").classList.add("hidden");
      $("files-restore-all").disabled = !!state.status.running || !!state.status.restoring || !!state.status.editing;
    },
    "restore-failed": (e) => {
      toast(String(e.payload || "Restore failed"), true);
      $("files-progress").classList.add("hidden");
      $("files-cancel").classList.add("hidden");
      $("files-restore-all").disabled = !!state.status.running || !!state.status.restoring || !!state.status.editing;
    },
  };
  for (const [name, fn] of Object.entries(handlers)) {
    try { await listen(name, fn); } catch (_) { /* older runtime */ }
  }
}

async function pickBackupPath(kind, purpose) {
  if (demo) return toast("Preview mode — use the macOS app to pick files.");
  try {
    const picked = await invoke("pick_backup_path", { kind, purpose });
    if (!picked) return;
    if (purpose === "include") {
      const current = textareaLines("included-folders");
      if (current.includes(picked.relative)) {
        return toast("That folder is already included");
      }
      current.push(picked.relative);
      $("included-folders").value = current.join("\n");
      return toast(`Added ${picked.relative}`);
    }
    const pattern = picked.pattern || picked.relative;
    const current = textareaLines("excludes");
    if (current.includes(pattern)) {
      return toast("That path is already excluded");
    }
    current.push(pattern);
    $("excludes").value = current.join("\n");
    toast(`Added exclude ${picked.relative}`);
  } catch (e) {
    toast(String(e), true);
  }
}

function bind() {
  document.querySelectorAll(".nav-item").forEach((btn) => {
    btn.addEventListener("click", () => showView(btn.dataset.view));
  });
  document.querySelectorAll("[data-goto]").forEach((btn) => {
    btn.addEventListener("click", () => showView(btn.dataset.goto));
  });
  $("bucket").addEventListener("input", updateIamPolicyButton);
  $("add-included-folder").addEventListener("click", () => pickBackupPath("folder", "include"));
  $("add-excluded-file").addEventListener("click", () => pickBackupPath("file", "exclude"));
  $("add-excluded-folder").addEventListener("click", () => pickBackupPath("folder", "exclude"));
  $("reset-source-defaults").addEventListener("click", resetSourceDefaults);
  $("retain-all").addEventListener("change", updateRetentionUI);
  ["retain-hours", "retain-days", "retain-weeks", "retain-months"].forEach((id) => {
    $(id).addEventListener("input", updateRetentionUI);
  });
  $("reset-retention").addEventListener("click", () => {
    $("retain-all").checked = false;
    $("retain-hours").value = 24;
    $("retain-days").value = 30;
    $("retain-weeks").value = 8;
    $("retain-months").value = 12;
    updateRetentionUI();
    toast("Retention reset to the default policy");
  });
  $("backup-now").addEventListener("click", async () => {
    if (demo) return toast("Preview mode — run the macOS app to back up.");
    try { await invoke("start_backup"); state.lastRunning = true; toast("Backup started"); }
    catch (e) { toast(String(e), true); }
  });
  $("toggle-auto").addEventListener("click", async () => {
    if (demo) return toast("Preview mode — automatic backups are not scheduled.");
    if ((state.config?.scheduleMinutes || 0) === 0) {
      return toast("Set a backup interval in Settings first", true);
    }
    const nextPaused = !state.status?.schedulerPaused;
    try {
      const paused = await invoke("set_scheduler_paused", { paused: nextPaused });
      state.status = { ...state.status, schedulerPaused: paused };
      updateStatus();
      toast(paused ? "Automatic backups paused" : "Automatic backups enabled");
    } catch (e) { toast(String(e), true); }
  });
  $("upload-limit-toggle").addEventListener("click", async () => {
    const next = !(state.status?.uploadLimitEnabled ?? state.config?.uploadLimitEnabled ?? true);
    if (demo) {
      state.status = { ...state.status, uploadLimitEnabled: next };
      if (state.config) state.config.uploadLimitEnabled = next;
      updateDashboard();
      toast(next ? "Upload limit enabled" : "Upload limit disabled");
      return;
    }
    try {
      const enabled = await invoke("set_upload_limit_enabled", { enabled: next });
      state.status = { ...state.status, uploadLimitEnabled: enabled };
      if (state.config) state.config.uploadLimitEnabled = enabled;
      updateDashboard();
      toast(enabled ? "Upload limit enabled" : "Upload limit disabled");
    } catch (e) { toast(String(e), true); }
  });
  $("upload-limit").addEventListener("change", async () => {
    const bps = limitBpsFromInput();
    $("upload-limit").value = limitMbValue(bps);
    if (demo) {
      state.status = { ...state.status, uploadLimitBytesPerSec: bps };
      if (state.config) state.config.uploadLimitBytesPerSec = bps;
      updateDashboard();
      toast(`Upload limit set to ${fmtSpeed(bps)}`);
      return;
    }
    try {
      await invoke("apply_upload_limit", { bytesPerSec: bps });
      state.status = { ...state.status, uploadLimitBytesPerSec: bps };
      if (state.config) state.config.uploadLimitBytesPerSec = bps;
      updateDashboard();
      toast(`Upload limit set to ${fmtSpeed(bps)} — save Settings to keep it`);
    } catch (e) { toast(String(e), true); }
  });
  $("new-backup-set").addEventListener("click", () => {
    const uuid = globalThis.crypto?.randomUUID?.() ||
      `new-${Date.now().toString(16)}-${Math.random().toString(16).slice(2)}`;
    $("backup-set-uuid").value = uuid;
    $("encryption-password").value = "";
    $("encryption-password").placeholder = "Enter a password for the new backup set";
    toast("New backup set UUID generated — review encryption settings and save.");
  });
  $("power-pause-cancel").addEventListener("click", async () => {
    if (demo) return toast("Preview mode");
    const ok = await confirmAction({
      title: "Cancel the paused backup?",
      message: "Do not resume this backup automatically?",
      detail: "Already uploaded data stays available. Automatic backups will remain paused until you re-enable them.",
      confirmLabel: "Don't resume",
      danger: true,
    });
    if (!ok) return;
    try {
      await invoke("cancel_power_pause");
      await poll();
      toast("Paused backup cancelled");
    } catch (e) { toast(String(e), true); }
  });
  $("cancel-run").addEventListener("click", async () => {
    const ok = await confirmAction({
      title: "Cancel backup?",
      message: "Cancel the running backup?",
      detail: "Already uploaded data is kept. The next run resumes from the saved progress.",
      confirmLabel: "Cancel backup",
      danger: true,
    });
    if (!ok) return;
    try {
      await invoke("cancel_backup");
      state.status = {
        ...state.status,
        cancelRequested: true,
        phase: "cancelling",
        detail: "cancellation requested — finishing the current operation",
      };
      updateStatus();
      toast("Cancelling…");
    } catch (e) { toast(String(e), true); }
  });

  $("settings-form").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    if (demo) return toast("Preview mode — settings are not saved.");
    try {
      const saved = await invoke("save_config", { config: gatherConfig() });
      state.config = saved;
      state.estimateAt = 0;
      toast("Settings saved");
      await loadState({ quiet: true });
    } catch (e) { toast(String(e), true); }
  });
  $("reload-settings").addEventListener("click", () => loadState());
  $("copy-iam-policy").addEventListener("click", async () => {
    const bucket = val("bucket");
    if (!bucket) return toast("Enter a bucket first", true);
    try {
      await copyText(iamPolicyJson(bucket, val("prefix")));
      toast("IAM policy copied to clipboard");
    } catch (e) {
      toast(String(e), true);
    }
  });
  $("test-connection").addEventListener("click", async () => {
    if (demo) return toast("Preview mode — no S3 connection.");
    const out = $("test-result");
    out.textContent = "Testing…";
    out.className = "test-result";
    try {
      const saved = await invoke("save_config", { config: gatherConfig() });
      state.config = saved;
      state.estimateAt = 0;
      const msg = await invoke("test_connection");
      out.textContent = msg;
      out.className = "test-result ok";
      refreshDestinationCheck();
    } catch (e) {
      out.textContent = String(e);
      out.className = "test-result bad";
    }
  });
  $("refresh-snapshots").addEventListener("click", refreshSnapshots);
  $("refresh-backups").addEventListener("click", refreshBackups);
  $("files-refresh").addEventListener("click", refreshFiles);
  $("files-record").addEventListener("change", () => {
    state.filesRecordKey = $("files-record").value;
    state.filesPath = "";
    state.filesFilter = "";
    state.filesIgnoredOnly = false;
    $("files-filter").value = "";
    if ($("files-ignored-only")) $("files-ignored-only").checked = false;
    refreshFiles();
  });
  $("files-up").addEventListener("click", () => openFilesDir(filesParentPath(state.filesPath)));
  $("files-filter").addEventListener("input", (event) => {
    state.filesFilter = event.target.value;
    renderFiles();
  });
  $("files-ignored-only").addEventListener("change", (event) => {
    state.filesIgnoredOnly = !!event.target.checked;
    renderFiles();
  });
  $("files-remove-ignored").addEventListener("click", removeIgnoredFiles);
  $("files-breadcrumb").addEventListener("click", (event) => {
    const target = event.target.closest("[data-files-path]");
    if (target) openFilesDir(target.getAttribute("data-files-path"));
  });
  $("files-table").addEventListener("click", (event) => {
    const open = event.target.closest("[data-files-open]");
    if (open) {
      openFilesDir(open.getAttribute("data-files-open"));
      return;
    }
    const content = event.target.closest("[data-files-content]");
    if (content) {
      openFileContent(content.getAttribute("data-files-content"));
      return;
    }
    const fileDiff = event.target.closest("[data-files-diff]");
    if (fileDiff) {
      const path = fileDiff.getAttribute("data-files-diff");
      if (state.filesRecordKey === "partial") {
        openFileDiff(path, { partial: true });
      } else {
        openFileDiff(path, { record: state.filesRecordKey || "latest", previous: null });
      }
      return;
    }
    const versions = event.target.closest("[data-files-versions]");
    if (versions) {
      openRestoreDialog(versions.getAttribute("data-files-versions"));
      return;
    }
    const restore = event.target.closest("[data-files-restore]");
    if (restore) {
      openRestoreDialog(restore.getAttribute("data-files-restore"));
      return;
    }
    const ignore = event.target.closest("[data-files-ignore]");
    if (ignore) {
      ignoreFilesEntry(ignore.getAttribute("data-files-ignore"));
      return;
    }
    const remove = event.target.closest("[data-files-remove]");
    if (remove) removeFilesEntry(remove.getAttribute("data-files-remove"));
  });
  $("files-cancel").addEventListener("click", async () => {
    const restoring = !!state.status?.restoring;
    const ok = await confirmAction({
      title: restoring ? "Cancel restore?" : "Cancel backup?",
      message: restoring ? "Cancel the running restore?" : "Cancel the running backup?",
      detail: restoring
        ? "Files already written to the destination remain there."
        : "Already uploaded data is kept. The next run resumes from the saved progress.",
      confirmLabel: restoring ? "Cancel restore" : "Cancel backup",
      danger: true,
    });
    if (!ok) return;
    try { await invoke("cancel_backup"); } catch (e) { toast(String(e), true); }
  });
  $("files-dest-pick").addEventListener("click", () => pickRestoreDestination("files-dest"));
  $("restore-dest-pick").addEventListener("click", () => pickRestoreDestination("restore-dest"));
  $("files-restore-all").addEventListener("click", restoreEntireFiles);
  $("refresh-cost").addEventListener("click", () => refreshEstimate(true));
  $("diff-close").addEventListener("click", closeDiff);
  $("diff-modal").addEventListener("click", (event) => {
    if (event.target === $("diff-modal")) closeDiff();
  });
  $("diff-filter").addEventListener("input", (event) => {
    state.diffFilter = event.target.value;
    renderDiff();
  });
  $("diff-kind").addEventListener("change", renderDiff);
  $("diff-table").addEventListener("click", (event) => {
    const target = event.target.closest("[data-diff-file]");
    if (target) openFileDiff(target.getAttribute("data-diff-file"), { record: state.diff.recordKey, previous: state.diff.previousRecordKey || null });
  });
  $("file-diff-close").addEventListener("click", closeFileDiff);
  $("file-diff-modal").addEventListener("click", (event) => {
    if (event.target === $("file-diff-modal")) closeFileDiff();
  });
  $("content-close").addEventListener("click", closeFileContent);
  $("content-modal").addEventListener("click", (event) => {
    if (event.target === $("content-modal")) closeFileContent();
  });
  $("confirm-close").addEventListener("click", () => closeConfirm(false));
  $("confirm-cancel").addEventListener("click", () => closeConfirm(false));
  $("confirm-accept").addEventListener("click", () => closeConfirm(true));
  $("confirm-modal").addEventListener("click", (event) => {
    if (event.target === $("confirm-modal")) closeConfirm(false);
  });
  $("restore-close").addEventListener("click", closeRestoreDialog);
  $("restore-cancel").addEventListener("click", closeRestoreDialog);
  $("restore-confirm").addEventListener("click", restoreSelectedVersion);
  $("restore-modal").addEventListener("click", (event) => {
    if (event.target === $("restore-modal")) closeRestoreDialog();
  });
  $("restore-versions").addEventListener("change", (event) => {
    const input = event.target.closest('input[name="restore-version"]');
    if (!input) return;
    state.restoreSelectedKey = input.value;
    document.querySelectorAll("#restore-versions .version-row").forEach((row) => {
      row.classList.toggle("selected", !!row.querySelector("input")?.checked);
    });
    $("restore-confirm").disabled = !state.restoreSelectedKey;
  });
  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") return;
    if (!$("confirm-modal").classList.contains("hidden")) {
      closeConfirm(false);
      return;
    }
    if (!$("content-modal").classList.contains("hidden")) {
      closeFileContent();
      return;
    }
    if (!$("file-diff-modal").classList.contains("hidden")) {
      closeFileDiff();
      return;
    }
    if (!$("restore-modal").classList.contains("hidden")) {
      closeRestoreDialog();
      return;
    }
    if (!$("diff-modal").classList.contains("hidden")) closeDiff();
  });
  $("refresh-logs").addEventListener("click", refreshLogs);
  $("clear-logs").addEventListener("click", async () => {
    if (demo) return;
    const ok = await confirmAction({
      title: "Clear activity log?",
      message: "Clear the on-disk activity log?",
      detail: "This does not affect any backup data.",
      confirmLabel: "Clear log",
      danger: true,
    });
    if (!ok) return;
    try { await invoke("clear_logs"); await refreshLogs(); } catch (e) { toast(String(e), true); }
  });
  $("open-log").addEventListener("click", () => {
    if (!demo) invoke("open_log").catch((e) => toast(String(e), true));
  });
  $("open-config").addEventListener("click", () => {
    if (!demo) invoke("open_config_folder").catch((e) => toast(String(e), true));
  });

  $("prune-dry").addEventListener("click", async () => {
    if (demo) return toast("Preview mode");
    try {
      const r = await invoke("prune_backups", { dryRun: true });
      toast(`Would keep ${r.recordsKept}, delete ${r.recordsDeleted} records and ${r.objectsDeleted} objects`);
    } catch (e) { toast(String(e), true); }
  });
  $("delete-set").addEventListener("click", async () => {
    if (demo) return toast("Preview mode");
    const authed = await authorizeSensitive("delete_set");
    if (!authed) return;
    const ok = await confirmAction({
      title: "Delete the entire backup set?",
      message: "Delete every object in this backup set?",
      detail: "This cannot be undone. All backup records and referenced data will be removed.",
      confirmLabel: "Delete everything",
      danger: true,
    });
    if (!ok) {
      await clearSensitiveAuthorization();
      return;
    }
    try {
      const n = await invoke("delete_backup_set");
      toast(`Deleted ${n} objects`);
      await refreshBackups();
    } catch (e) { toast(String(e), true); }
  });
}

/* ---------------- preview data ---------------- */
function demoConfig() {
  return {
    version: 1,
    backupSetUuid: "5f1c1d3e-demo-4c3a-9f5e-000000000001",
    backupSetName: "MacBook Pro — Time Machine",
    bucket: "my-time-machine-backups",
    region: "eu-west-1",
    prefix: "qia-backups",
    accessKeyId: "AKIAIOSFODNN7EXAMPLE",
    secretAccessKey: "",
    sourcePath: "",
    sourceKind: "snapshot",
    snapshotVolume: "/System/Volumes/Data",
    includedFolders: [...DEFAULT_INCLUDED_FOLDERS],
    excludePatterns: [...DEFAULT_EXCLUDE_PATTERNS],
    scheduleMinutes: 60,
    retainHours: 24,
    retainDays: 30,
    retainWeeks: 8,
    retainMonths: 12,
    retainAll: false,
    encryptionEnabled: true,
    encryptionPassword: "",
    packSizeBytes: 67108864,
    chunkSize: 1048576,
    uploadLimitEnabled: true,
    uploadLimitBytesPerSec: DEFAULT_UPLOAD_LIMIT_BPS,
    lastBackupAt: new Date(Date.now() - 6 * 3600 * 1000).toISOString(),
  };
}
function demoStatus() {
  return {
    running: false,
    phase: "idle",
    detail: "",
    files: 0,
    bytesScanned: 0,
    bytesUploaded: 0,
    totalFiles: 184203,
    totalBytes: 1928374655,
    lastSummary: {
      files: 184203, bytesUploaded: 487210334, bytesScanned: 1928374655,
      blobsNew: 9182, blobsReused: 174021, errors: 0,
      finishedAt: new Date(Date.now() - 6 * 3600 * 1000).toISOString(),
    },
    schedulerPaused: true,
    uploadLimitEnabled: true,
    uploadLimitBytesPerSec: DEFAULT_UPLOAD_LIMIT_BPS,
  };
}
function demoLogs() {
  const t = new Date().toISOString().replace("T", " ").slice(0, 19);
  return [
    `[${t}] INFO  backup complete: 184203 files, 1928374655 bytes scanned, 487210334 bytes uploaded, 0 errors, 412.6s`,
    `[${t}] INFO  uploaded pack qia-backups/…/blobpacks/…blobpack (67108864 bytes)`,
    `[${t}] INFO  prune: kept 30 records, deleted 1 records and 0 objects`,
  ];
}
function demoBackups() {
  return Array.from({ length: 8 }, (_, i) => {
    const ts = new Date(Date.now() - i * 6 * 3600 * 1000).toISOString();
    return {
      timestamp: ts,
      key: `demo/backuprecords/${ts}.backuprecord`,
      size: 12000 + i * 13,
      isComplete: true,
      filesCount: 184203 - i * 12,
      bytesUploaded: 487210334 - i * 125000,
      recordName: `${ts}.backuprecord`,
    };
  });
}

function setupModalScrollLock() {
  let locked = false;
  let savedScrollY = 0;
  const apply = () => {
    const open = [...document.querySelectorAll(".modal-backdrop")].some(
      (el) => !el.classList.contains("hidden")
    );
    if (open === locked) return;
    locked = open;
    if (open) {
      // The document element is the actual scrolling container in WebKit, so
      // locking <body> alone still lets the page scroll behind a modal. A fixed
      // body plus a negative top keeps the background visually in place.
      savedScrollY = window.scrollY || document.documentElement.scrollTop || 0;
      document.documentElement.classList.add("modal-open");
      document.body.classList.add("modal-open");
      document.body.style.top = `-${savedScrollY}px`;
    } else {
      document.documentElement.classList.remove("modal-open");
      document.body.classList.remove("modal-open");
      document.body.style.top = "";
      window.scrollTo(0, savedScrollY);
    }
  };
  const observer = new window.MutationObserver(apply);
  document.querySelectorAll(".modal-backdrop").forEach((el) => {
    observer.observe(el, { attributes: true, attributeFilter: ["class"] });
  });
  apply();
}

async function init() {
  setupModalScrollLock();
  bind();
  await setupEvents();
  await loadState();
  await refreshSnapshots();
  setInterval(poll, 1500);
}

window.addEventListener("DOMContentLoaded", init);
