//! Correctness-first caching for declarative tasks.
//!
//! A cache entry is committed only after a successful task run, output
//! validation, and a second input snapshot. A per-task filesystem lock keeps
//! the lookup/run/commit sequence single-writer across devenv processes.

use crate::config::{CachePath, SnapshotMode};
use devenv_cache_core::{
    db::Database,
    error::{CacheError, CacheResult},
    file::{compute_file_hash, compute_string_hash},
    time,
};
use fd_lock::RwLock;
use ignore::Match;
use ignore::WalkBuilder;
use ignore::overrides::OverrideBuilder;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use tokio::sync::oneshot;

const GLOB_SPECIAL_CHARS: &[char] = &['*', '?', '[', '{'];

// Create a constant for embedded migrations.
pub const MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!();

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PathState {
    pub path: String,
    pub kind: String,
    pub content_hash: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeclaredPathSnapshot {
    pub declaration: CachePath,
    pub matches: Vec<PathState>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PathSnapshot {
    pub declarations: Vec<DeclaredPathSnapshot>,
}

impl PathSnapshot {
    pub fn missing_required(&self) -> Vec<&str> {
        self.declarations
            .iter()
            .filter(|entry| !entry.declaration.optional && entry.matches.is_empty())
            .map(|entry| entry.declaration.path.as_str())
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct CachedRun {
    pub definition_hash: String,
    pub input_snapshot: PathSnapshot,
    pub output_snapshot: PathSnapshot,
    pub output: Option<Value>,
}

pub fn snapshot_executable(command: &str) -> CacheResult<Option<PathState>> {
    let path = Path::new(command);
    match fs::symlink_metadata(path) {
        Ok(_) => snapshot_path(path, SnapshotMode::Content).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Holds an exclusive advisory lock until dropped.
pub struct TaskLock {
    release: Option<oneshot::Sender<()>>,
    _worker: tokio::task::JoinHandle<()>,
}

impl Drop for TaskLock {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

/// Acquire an exclusive cross-process advisory lock on `lock_path` without
/// blocking a Tokio worker.
async fn acquire_lock_file(lock_path: PathBuf) -> CacheResult<TaskLock> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)?;
    let (acquired_tx, acquired_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();

    let worker = tokio::task::spawn_blocking(move || {
        let mut lock = RwLock::new(file);
        match lock.write() {
            Ok(_guard) => {
                let _ = acquired_tx.send(Ok::<_, String>(()));
                let _ = release_rx.blocking_recv();
            }
            Err(error) => {
                let _ = acquired_tx.send(Err(error.to_string()));
            }
        }
    });

    match acquired_rx.await {
        Ok(Ok(())) => Ok(TaskLock {
            release: Some(release_tx),
            _worker: worker,
        }),
        Ok(Err(error)) => Err(CacheError::initialization(format!(
            "failed to lock {}: {error}",
            lock_path.display()
        ))),
        Err(error) => Err(CacheError::initialization(format!(
            "task lock worker stopped before locking {}: {error}",
            lock_path.display()
        ))),
    }
}

/// Task cache manager.
#[derive(Clone, Debug)]
pub struct TaskCache {
    db: Database,
    lock_dir: PathBuf,
}

impl TaskCache {
    pub async fn new(cache_dir: &Path) -> CacheResult<Self> {
        Self::with_db_path(cache_dir.join("tasks.db")).await
    }

    pub async fn with_db_path(db_path: PathBuf) -> CacheResult<Self> {
        let lock_dir = db_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("task-locks");
        fs::create_dir_all(&lock_dir)?;
        // A non-native process manager launches every per-process wrapper at
        // once; unserialized, they race to create, WAL-switch, and migrate
        // the shared database (SQLITE_IOERR at init). Upstream #2897
        // serializes only the devenv-CLI launch path, so hold a cross-process
        // lock here at the source.
        let init_lock = acquire_lock_file(lock_dir.join("__task-cache-init.lock")).await?;
        let db = Database::new(db_path, &MIGRATIONS).await?;
        drop(init_lock);
        Ok(Self { db, lock_dir })
    }

    pub fn pool(&self) -> &sqlx::SqlitePool {
        self.db.pool()
    }

    /// Acquire the task's cross-process lock without blocking a Tokio worker.
    pub async fn acquire_task_lock(&self, task_name: &str) -> CacheResult<TaskLock> {
        acquire_lock_file(self.lock_dir.join(format!("{task_name}.lock"))).await
    }

    pub fn snapshot_paths(
        &self,
        declarations: &[CachePath],
        cwd: Option<&str>,
    ) -> CacheResult<PathSnapshot> {
        let base = match cwd {
            Some(cwd) => PathBuf::from(cwd),
            None => std::env::current_dir()?,
        };

        let mut snapshots = Vec::with_capacity(declarations.len());
        for declaration in declarations {
            let mut matches = if declaration.paths_from {
                expand_paths_from(declaration, &base)?
            } else {
                expand_path(&declaration.path, &base)?
            };
            matches.sort();
            matches.dedup();

            let matches = matches
                .iter()
                .map(|path| snapshot_path(path, declaration.snapshot))
                .collect::<CacheResult<Vec<_>>>()?;
            snapshots.push(DeclaredPathSnapshot {
                declaration: declaration.clone(),
                matches,
            });
        }

        Ok(PathSnapshot {
            declarations: snapshots,
        })
    }

    pub async fn get_cached_run(&self, task_name: &str) -> CacheResult<Option<CachedRun>> {
        let row: Option<(String, String, String, Option<String>)> = sqlx::query_as(
            r#"
            SELECT definition_hash, input_snapshot, output_snapshot, output
            FROM task_cache_v2
            WHERE task_name = ?
            "#,
        )
        .bind(task_name)
        .fetch_optional(self.pool())
        .await?;

        row.map(
            |(definition_hash, input_snapshot, output_snapshot, output)| {
                Ok(CachedRun {
                    definition_hash,
                    input_snapshot: serde_json::from_str(&input_snapshot)?,
                    output_snapshot: serde_json::from_str(&output_snapshot)?,
                    output: output
                        .map(|value| serde_json::from_str(&value))
                        .transpose()?,
                })
            },
        )
        .transpose()
    }

    /// Atomically publish a complete cache entry.
    pub async fn commit_cached_run(
        &self,
        task_name: &str,
        definition_hash: &str,
        input_snapshot: &PathSnapshot,
        output_snapshot: &PathSnapshot,
        output: Option<&Value>,
    ) -> CacheResult<()> {
        let input_snapshot = serde_json::to_string(input_snapshot)?;
        let output_snapshot = serde_json::to_string(output_snapshot)?;
        let output = output.map(serde_json::to_string).transpose()?;
        let now = time::now_as_unix_seconds();
        let mut transaction = self.pool().begin().await?;

        sqlx::query(
            r#"
            INSERT INTO task_cache_v2 (
                task_name, definition_hash, input_snapshot, output_snapshot, output, last_run
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (task_name) DO UPDATE SET
                definition_hash = excluded.definition_hash,
                input_snapshot = excluded.input_snapshot,
                output_snapshot = excluded.output_snapshot,
                output = excluded.output,
                last_run = excluded.last_run
            "#,
        )
        .bind(task_name)
        .bind(definition_hash)
        .bind(input_snapshot)
        .bind(output_snapshot)
        .bind(output)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Status-only tasks retain their last output independently of cache v2.
    pub async fn store_task_output(&self, task_name: &str, output: &Value) -> CacheResult<()> {
        let output_json = serde_json::to_string(output)?;
        let now = time::now_as_unix_seconds();

        sqlx::query(
            r#"
            INSERT INTO task_run (task_name, last_run, output)
            VALUES (?, ?, ?)
            ON CONFLICT (task_name) DO UPDATE SET
                last_run = excluded.last_run,
                output = excluded.output
            "#,
        )
        .bind(task_name)
        .bind(now)
        .bind(output_json)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    pub async fn get_task_output(&self, task_name: &str) -> CacheResult<Option<Value>> {
        let result: Option<String> =
            sqlx::query_scalar("SELECT output FROM task_run WHERE task_name = ?")
                .bind(task_name)
                .fetch_optional(self.pool())
                .await?;
        result
            .map(|json| serde_json::from_str(&json))
            .transpose()
            .map_err(CacheError::from)
    }
}

fn is_glob(path: &str) -> bool {
    path.contains(GLOB_SPECIAL_CHARS)
}

fn extract_base_dir(pattern: &Path) -> PathBuf {
    let pattern = pattern.to_string_lossy();
    let first_special = pattern
        .char_indices()
        .find(|(_, character)| GLOB_SPECIAL_CHARS.contains(character))
        .map(|(index, _)| index)
        .unwrap_or(pattern.len());
    let prefix = Path::new(&pattern[..first_special]);
    if first_special == pattern.len() {
        return prefix
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
    }
    prefix
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf()
}

fn normalize_pattern(pattern: &Path, base: &Path) -> String {
    let relative = pattern.strip_prefix(base).unwrap_or(pattern);
    let value = relative.to_string_lossy();
    if value.contains('/') || value.starts_with('/') {
        value.into_owned()
    } else {
        format!("/{value}")
    }
}

fn expand_path(declaration: &str, cwd: &Path) -> CacheResult<Vec<PathBuf>> {
    let declared = Path::new(declaration);
    let resolved = if declared.is_absolute() {
        declared.to_path_buf()
    } else {
        cwd.join(declared)
    };

    if !is_glob(declaration) {
        return match fs::symlink_metadata(&resolved) {
            Ok(_) => Ok(vec![resolved]),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(error) => Err(error.into()),
        };
    }

    let base = extract_base_dir(&resolved);
    if !base.exists() {
        return Ok(Vec::new());
    }
    let normalized = normalize_pattern(&resolved, &base);
    let mut overrides = OverrideBuilder::new(&base);
    overrides
        .add(&normalized)
        .map_err(|error| CacheError::initialization(error.to_string()))?;
    let overrides = overrides
        .build()
        .map_err(|error| CacheError::initialization(error.to_string()))?;

    let mut paths = Vec::new();
    let mut walker = WalkBuilder::new(&base);
    walker
        .hidden(false)
        .ignore(false)
        .git_ignore(false)
        .git_global(false)
        .git_exclude(false)
        .parents(false)
        .follow_links(false);
    for entry in walker.build() {
        let entry = entry.map_err(|error| CacheError::initialization(error.to_string()))?;
        let is_dir = entry.file_type().is_some_and(|kind| kind.is_dir());
        if matches!(overrides.matched(entry.path(), is_dir), Match::Whitelist(_)) {
            paths.push(entry.into_path());
        }
    }
    Ok(paths)
}

/// Resolve a pathsFrom declaration: read the newline-delimited list the
/// declared file holds, resolving each entry like a declared path. A missing
/// list is either a stable empty set (optional) or a poisoned snapshot that
/// can never match a stored one (required): the list is the cache contract,
/// and an absent contract must run the task rather than silently pass it.
fn expand_paths_from(declaration: &CachePath, base: &Path) -> CacheResult<Vec<PathBuf>> {
    let declared = Path::new(&declaration.path);
    let list = if declared.is_absolute() {
        declared.to_path_buf()
    } else {
        base.join(declared)
    };
    let text = match fs::read_to_string(&list) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if declaration.optional {
                return Ok(Vec::new());
            }
            return Ok(vec![poison_marker(&list)]);
        }
        Err(error) => return Err(error.into()),
    };
    Ok(text
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            let entry = Path::new(line);
            if entry.is_absolute() {
                entry.to_path_buf()
            } else {
                base.join(entry)
            }
        })
        .collect())
}

/// A path that cannot exist, STABLE within this process and unique across
/// processes. Both halves are load-bearing: the runner snapshots inputs
/// before AND after a task and fails it when they differ, so a per-call
/// marker turned every missing-list run into "cache inputs changed while the
/// task was running" — the stored snapshot from an EARLIER process is what
/// must never match, and a per-process constant achieves exactly that.
fn poison_marker(list: &Path) -> PathBuf {
    static MARKER: std::sync::OnceLock<u128> = std::sync::OnceLock::new();
    let nanos = MARKER.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_else(|_| std::process::id() as u128)
    });
    list.join(format!(".devenv-missing-path-list-{nanos}"))
}

fn snapshot_path(path: &Path, mode: SnapshotMode) -> CacheResult<PathState> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // Reachable only via pathsFrom entries and the poison marker:
            // expand_path drops nonexistent literals before this point. A
            // listed-but-absent input is a real state worth recording (the
            // poison marker exploits exactly that to force a miss).
            return Ok(PathState {
                path: path.to_string_lossy().into_owned(),
                kind: "missing".to_string(),
                content_hash: compute_string_hash(&format!("missing:{}", path.display())),
            });
        }
        Err(error) => return Err(error.into()),
    };
    let file_type = metadata.file_type();
    if matches!(mode, SnapshotMode::Listing) {
        let (kind, content_hash) = if file_type.is_dir() {
            ("directory-listing", hash_directory_listing(path)?)
        } else {
            ("exists", compute_string_hash("exists"))
        };
        return Ok(PathState {
            path: path.to_string_lossy().into_owned(),
            kind: kind.to_string(),
            content_hash,
        });
    }
    let (kind, content_hash) = if file_type.is_symlink() {
        (
            "symlink",
            compute_string_hash(&fs::read_link(path)?.to_string_lossy()),
        )
    } else if file_type.is_dir() {
        ("directory", hash_directory(path)?)
    } else if file_type.is_file() {
        (
            "file",
            compute_string_hash(&format!(
                "{}:{}",
                metadata_mode(&metadata),
                compute_file_hash(path)?
            )),
        )
    } else {
        (
            "other",
            compute_string_hash(&format!("{}:{}", metadata_mode(&metadata), metadata.len())),
        )
    };

    Ok(PathState {
        path: path.to_string_lossy().into_owned(),
        kind: kind.to_string(),
        content_hash,
    })
}

/// Immediate child NAMES only, sorted — no contents, no recursion. An entry
/// appearing or vanishing changes the hash; editing one does not. Pairing
/// listings over a tree's directories with a content list of its files gives
/// exact inputs plus addition detection without re-stating any filter.
fn hash_directory_listing(root: &Path) -> CacheResult<String> {
    let mut names = fs::read_dir(root)?
        .map(|entry| entry.map(|e| e.file_name().to_string_lossy().into_owned()))
        .collect::<Result<Vec<_>, _>>()?;
    names.sort();
    Ok(compute_string_hash(&names.join("\n")))
}

fn hash_directory(root: &Path) -> CacheResult<String> {
    fn visit(root: &Path, directory: &Path, entries: &mut Vec<String>) -> CacheResult<()> {
        let mut children = fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
        children.sort_by_key(|entry| entry.file_name());
        for child in children {
            let path = child.path();
            let relative = path.strip_prefix(root).unwrap_or(&path).to_string_lossy();
            let metadata = fs::symlink_metadata(&path)?;
            let file_type = metadata.file_type();
            if file_type.is_symlink() {
                entries.push(format!(
                    "symlink:{relative}:{}",
                    fs::read_link(&path)?.to_string_lossy()
                ));
            } else if file_type.is_dir() {
                entries.push(format!("directory:{relative}:{}", metadata_mode(&metadata)));
                visit(root, &path, entries)?;
            } else if file_type.is_file() {
                entries.push(format!(
                    "file:{relative}:{}:{}",
                    metadata_mode(&metadata),
                    compute_file_hash(&path)?
                ));
            } else {
                entries.push(format!(
                    "other:{relative}:{}:{}",
                    metadata_mode(&metadata),
                    metadata.len()
                ));
            }
        }
        Ok(())
    }

    let mut entries = Vec::new();
    visit(root, root, &mut entries)?;
    Ok(compute_string_hash(&entries.join("\n")))
}

#[cfg(unix)]
fn metadata_mode(metadata: &fs::Metadata) -> u32 {
    use std::os::unix::fs::MetadataExt;
    metadata.mode()
}

#[cfg(not(unix))]
fn metadata_mode(metadata: &fs::Metadata) -> u32 {
    u32::from(metadata.permissions().readonly())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn snapshot_detects_content_and_symlink_target_changes() {
        let temp = TempDir::new().unwrap();
        let input = temp.path().join("input");
        let first = temp.path().join("first");
        let second = temp.path().join("second");
        fs::write(&first, "same").unwrap();
        fs::write(&second, "same").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&first, &input).unwrap();

        let before = snapshot_path(&input, SnapshotMode::Content).unwrap();
        fs::remove_file(&input).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&second, &input).unwrap();
        let after = snapshot_path(&input, SnapshotMode::Content).unwrap();

        assert_ne!(before, after);
        assert_eq!(after.kind, "symlink");
    }

    #[tokio::test]
    async fn cache_entry_round_trips_as_one_record() {
        let temp = TempDir::new().unwrap();
        let cache = TaskCache::with_db_path(temp.path().join("tasks.db"))
            .await
            .unwrap();
        let snapshot = PathSnapshot {
            declarations: Vec::new(),
        };
        let output = serde_json::json!({"value": 1});

        cache
            .commit_cached_run(
                "test:task",
                "definition",
                &snapshot,
                &snapshot,
                Some(&output),
            )
            .await
            .unwrap();
        let cached = cache.get_cached_run("test:task").await.unwrap().unwrap();

        assert_eq!(cached.definition_hash, "definition");
        assert_eq!(cached.input_snapshot, snapshot);
        assert_eq!(cached.output_snapshot, snapshot);
        assert_eq!(cached.output, Some(output));
    }
}

#[cfg(test)]
mod snapshot_mode_tests {
    use super::*;
    use crate::config::SnapshotMode;

    fn write(path: &Path, content: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    }

    #[test]
    fn listing_ignores_edits_but_sees_additions() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path().join("src");
        write(&d.join("a.txt"), "one");

        let before = snapshot_path(&d, SnapshotMode::Listing).unwrap();
        write(&d.join("a.txt"), "EDITED");
        let edited = snapshot_path(&d, SnapshotMode::Listing).unwrap();
        assert_eq!(
            before.content_hash, edited.content_hash,
            "an edit must not change a listing snapshot"
        );

        write(&d.join("b.txt"), "new");
        let added = snapshot_path(&d, SnapshotMode::Listing).unwrap();
        assert_ne!(
            before.content_hash, added.content_hash,
            "a new child must change a listing snapshot"
        );

        fs::remove_file(d.join("b.txt")).unwrap();
        let removed = snapshot_path(&d, SnapshotMode::Listing).unwrap();
        assert_eq!(
            before.content_hash, removed.content_hash,
            "removing the child must restore the listing snapshot"
        );
    }

    #[test]
    fn listing_is_not_recursive() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path().join("src");
        write(&d.join("sub/a.txt"), "one");
        let before = snapshot_path(&d, SnapshotMode::Listing).unwrap();
        write(&d.join("sub/b.txt"), "new");
        let after = snapshot_path(&d, SnapshotMode::Listing).unwrap();
        assert_eq!(
            before.content_hash, after.content_hash,
            "a listing covers immediate children only; declare each directory"
        );
    }

    #[test]
    fn paths_from_resolves_and_hashes_content() {
        let dir = tempfile::tempdir().unwrap();
        write(&dir.path().join("watched.txt"), "v1");
        write(&dir.path().join("list"), "watched.txt\n");
        let decl = CachePath {
            path: "list".to_string(),
            paths_from: true,
            ..Default::default()
        };

        let paths = expand_paths_from(&decl, dir.path()).unwrap();
        assert_eq!(paths, vec![dir.path().join("watched.txt")]);

        let before = snapshot_path(&paths[0], SnapshotMode::Content).unwrap();
        write(&dir.path().join("watched.txt"), "v2");
        let after = snapshot_path(&paths[0], SnapshotMode::Content).unwrap();
        assert_ne!(before.content_hash, after.content_hash);
    }

    #[test]
    fn missing_required_list_poisons_missing_optional_list_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let required = CachePath {
            path: "absent".to_string(),
            paths_from: true,
            ..Default::default()
        };
        let a = expand_paths_from(&required, dir.path()).unwrap();
        let b = expand_paths_from(&required, dir.path()).unwrap();
        assert_eq!(a.len(), 1);
        // Stable WITHIN a process: the runner snapshots before and after a
        // task and fails it on any difference, so the two must agree here.
        // Uniqueness is across processes (wall-clock seeded), which a unit
        // test cannot observe; the in-situ check is entry-after-entry miss.
        assert_eq!(a, b, "poison must be stable within a process");
        let pa = snapshot_path(&a[0], SnapshotMode::Content).unwrap();
        let pb = snapshot_path(&b[0], SnapshotMode::Content).unwrap();
        assert_eq!(pa.content_hash, pb.content_hash);

        let optional = CachePath {
            path: "absent".to_string(),
            paths_from: true,
            optional: true,
            ..Default::default()
        };
        assert!(expand_paths_from(&optional, dir.path()).unwrap().is_empty());
    }
}
