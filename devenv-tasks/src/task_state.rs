use crate::SudoContext;
use crate::config::TaskConfig;
use crate::executor::{ExecutionContext, OutputCallback};
use crate::task_cache::{PathState, TaskCache, snapshot_executable};
use crate::types::{
    Output, Outputs, Skipped, TaskCompleted, TaskFailure, TaskStatus, VerbosityLevel,
    get_or_create_devenv_env_mut, process_name,
};
use base64::Engine;
use devenv_activity::{Activity, ActivityInstrument, ActivityLevel};
use devenv_cache_core::file::compute_string_hash;
use devenv_processes::{NativeProcessManager, ProcessConfig};
use miette::{IntoDiagnostic, Result, WrapErr};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

impl std::fmt::Debug for TaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskState")
            .field("task", &self.task)
            .field("status", &self.status)
            .field("verbosity", &self.verbosity)
            .finish()
    }
}

/// OutputCallback implementation that forwards output to an Activity.
struct ActivityCallback<'a> {
    activity: &'a Activity,
}

impl<'a> ActivityCallback<'a> {
    fn new(activity: &'a Activity) -> Self {
        Self { activity }
    }
}

impl OutputCallback for ActivityCallback<'_> {
    fn on_stdout(&self, line: &str) {
        self.activity.log(line);
    }

    fn on_stderr(&self, line: &str) {
        self.activity.error(line);
    }
}

/// Info returned from `run_process` about how the process was launched.
pub struct ProcessLaunchInfo {
    /// Whether the process has auto start off (start.enable = false).
    pub auto_start_off: bool,
    /// Whether the process has a readiness probe that must be awaited.
    pub requires_ready_wait: bool,
    /// The process manager name (stripped `devenv:processes:` prefix).
    pub process_name: String,
}

pub struct TaskState {
    pub task: TaskConfig,
    pub status: TaskStatus,
    pub verbosity: VerbosityLevel,
    pub sudo_context: Option<SudoContext>,
}

impl TaskState {
    pub fn new(
        task: TaskConfig,
        verbosity: VerbosityLevel,
        sudo_context: Option<SudoContext>,
    ) -> Self {
        // Process tasks stay `Pending` while their launch is in flight or
        // their process is alive; the manager owns the live phase. The graph
        // only records terminal launch outcomes as `Completed`.
        let status = TaskStatus::Pending;
        Self {
            task,
            status,
            verbosity,
            sudo_context,
        }
    }

    /// Validate that the working directory exists and is a directory.
    fn validate_cwd(&self) -> Result<()> {
        if let Some(cwd) = &self.task.cwd {
            let cwd_path = std::path::Path::new(cwd);
            if !cwd_path.exists() {
                miette::bail!(
                    "Working directory for task '{}' does not exist: {}",
                    self.task.name,
                    cwd
                );
            }
            if !cwd_path.is_dir() {
                miette::bail!(
                    "Working directory for task '{}' is not a directory: {}",
                    self.task.name,
                    cwd
                );
            }
        }
        Ok(())
    }

    /// Try to get cached output for this task, logging any errors.
    async fn get_cached_output(&self, cache: &TaskCache) -> Option<serde_json::Value> {
        match cache.get_task_output(&self.task.name).await {
            Ok(Some(output)) => {
                tracing::trace!(
                    "Found cached output for task {} in database",
                    self.task.name
                );
                Some(output)
            }
            Ok(None) => {
                tracing::trace!("No cached output found for task {}", self.task.name);
                None
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to get cached output for task {}: {}",
                    self.task.name,
                    e
                );
                None
            }
        }
    }

    fn cache_definition_hash(
        &self,
        dependency_outputs: &Outputs,
        shell_env: &std::collections::HashMap<String, String>,
    ) -> Result<String> {
        #[derive(serde::Serialize)]
        struct Definition<'a> {
            schema: u8,
            command: &'a Option<String>,
            command_snapshot: Option<PathState>,
            status: &'a Option<String>,
            status_snapshot: Option<PathState>,
            input: &'a Option<serde_json::Value>,
            task_env: BTreeMap<&'a str, &'a str>,
            cwd: String,
            cache: &'a crate::config::TaskCacheConfig,
            inherited_env: BTreeMap<&'a str, Option<&'a str>>,
            shell_identity: BTreeMap<&'static str, Option<&'a str>>,
            dependency_outputs: &'a Outputs,
        }

        let cache = self
            .task
            .cache
            .as_ref()
            .expect("cache definition requested only for cached tasks");
        let task_env = self
            .task
            .env
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        let inherited_env = cache
            .env
            .iter()
            .map(|key| (key.as_str(), shell_env.get(key).map(String::as_str)))
            .collect();
        let shell_identity = ["DEVENV_ROOT", "DEVENV_DOTFILE", "DEVENV_PROFILE"]
            .into_iter()
            .map(|key| (key, shell_env.get(key).map(String::as_str)))
            .collect();
        let cwd = match &self.task.cwd {
            Some(cwd) => cwd.clone(),
            None => std::env::current_dir()
                .into_diagnostic()
                .wrap_err("Failed to resolve task working directory")?
                .to_string_lossy()
                .into_owned(),
        };
        let definition = Definition {
            schema: 2,
            command: &self.task.command,
            command_snapshot: self
                .task
                .command
                .as_deref()
                .map(snapshot_executable)
                .transpose()?
                .flatten(),
            status: &self.task.status,
            status_snapshot: self
                .task
                .status
                .as_deref()
                .map(snapshot_executable)
                .transpose()?
                .flatten(),
            input: &self.task.input,
            task_env,
            cwd,
            cache,
            inherited_env,
            shell_identity,
            dependency_outputs,
        };
        let json = serde_json::to_string(&definition)
            .into_diagnostic()
            .wrap_err("Failed to serialize task cache definition")?;
        Ok(compute_string_hash(&json))
    }

    /// Prepare environment variables for task execution.
    fn prepare_env(
        &self,
        outputs: &Outputs,
        shell_env: &std::collections::HashMap<String, String>,
    ) -> Result<BTreeMap<String, String>> {
        // Start with shell env as the base layer
        let mut env: BTreeMap<String, String> = shell_env
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Set DEVENV_TASK_INPUT
        if let Some(input) = &self.task.input {
            let input_json = serde_json::to_string(input)
                .into_diagnostic()
                .wrap_err("Failed to serialize task input to JSON")?;
            env.insert("DEVENV_TASK_INPUT".to_string(), input_json);
        }

        // Set environment variables from task outputs
        let env_exports = outputs.collect_env_exports();
        let mut devenv_env = String::new();
        for (env_key, env_str) in &env_exports {
            devenv_env.push_str(&format!(
                "export {}={}\n",
                env_key,
                shell_escape::escape(std::borrow::Cow::Borrowed(env_str))
            ));
        }
        env.extend(env_exports);
        // Internal for now
        env.insert("DEVENV_TASK_ENV".to_string(), devenv_env);

        // Merge per-task env vars (take precedence over upstream exports)
        for (key, value) in &self.task.env {
            env.insert(key.clone(), value.clone());
        }

        // Set DEVENV_TASKS_OUTPUTS
        let outputs_json = serde_json::to_string(outputs)
            .into_diagnostic()
            .wrap_err("Failed to serialize task outputs to JSON")?;
        env.insert("DEVENV_TASKS_OUTPUTS".to_string(), outputs_json);

        Ok(env)
    }

    /// Create a temporary file for task I/O.
    fn create_tempfile(prefix: &str, suffix: &str) -> Result<tempfile::NamedTempFile> {
        tempfile::Builder::new()
            .prefix(prefix)
            .suffix(suffix)
            .tempfile()
            .into_diagnostic()
            .wrap_err_with(|| format!("Failed to create temporary file ({prefix})"))
    }

    async fn get_outputs(
        outputs_file: &tempfile::NamedTempFile,
        exports_file: &tempfile::NamedTempFile,
        stdout_lines: &[(std::time::Instant, String)],
    ) -> Output {
        // Read both files concurrently
        let (output_data, export_data) = tokio::join!(
            tokio::fs::read(outputs_file.path()),
            tokio::fs::read(exports_file.path()),
        );

        // TODO: report JSON parsing errors
        let mut output: Option<serde_json::Value> = output_data
            .ok()
            .and_then(|data| serde_json::from_slice(&data).ok());

        // Collect exports from both the legacy stdout protocol (pre-2.0.4 Nix modules)
        // and the file based protocol (CLI 2.0.4+). File exports are applied last
        // so they take precedence over stdout exports.
        let stdout_exports = Self::parse_stdout_exports(stdout_lines);
        let file_exports = match export_data {
            Ok(data) if !data.is_empty() => Self::parse_exports(&data),
            _ => Vec::new(),
        };

        if !stdout_exports.is_empty() || !file_exports.is_empty() {
            let out = output.get_or_insert_with(|| serde_json::json!({}));
            if let Some(env_obj) = get_or_create_devenv_env_mut(out) {
                for (k, v) in stdout_exports.into_iter().chain(file_exports) {
                    env_obj.insert(k, serde_json::Value::String(v));
                }
            } else {
                tracing::warn!(
                    "Task output is not a JSON object, {} export(s) dropped",
                    stdout_exports.len() + file_exports.len()
                );
            }
        }

        Output(output)
    }

    /// Decode base64 bytes into a UTF-8 string, logging a warning on failure.
    fn decode_b64(data: &[u8], context: &str) -> Option<String> {
        match B64.decode(data) {
            Ok(bytes) => match String::from_utf8(bytes) {
                Ok(s) => Some(s),
                Err(e) => {
                    tracing::warn!("Skipping {context} with invalid UTF-8: {e}");
                    None
                }
            },
            Err(e) => {
                tracing::warn!("Skipping {context} with invalid base64: {e}");
                None
            }
        }
    }

    /// Parse DEVENV_EXPORT lines from stdout (legacy protocol for pre-2.0.4 Nix modules).
    /// Format: DEVENV_EXPORT:<base64-key>=<base64-value>
    fn parse_stdout_exports(
        stdout_lines: &[(std::time::Instant, String)],
    ) -> Vec<(String, String)> {
        let mut exports = Vec::new();
        for (_, line) in stdout_lines {
            if let Some(rest) = line.strip_prefix("DEVENV_EXPORT:") {
                // Base64 uses '=' for padding, so find the separator '=' at the
                // first position that is a multiple of 4 (end of a valid base64 string).
                let split_pos = (4..rest.len())
                    .step_by(4)
                    .find(|&i| rest.as_bytes()[i] == b'=');
                if let Some(pos) = split_pos
                    && let (Some(var), Some(val)) = (
                        Self::decode_b64(&rest.as_bytes()[..pos], "DEVENV_EXPORT key"),
                        Self::decode_b64(&rest.as_bytes()[pos + 1..], "DEVENV_EXPORT value"),
                    )
                {
                    exports.push((var, val));
                }
            }
        }
        exports
    }

    /// Parse null-separated name\0base64(value)\0 pairs from exports file.
    fn parse_exports(data: &[u8]) -> Vec<(String, String)> {
        let mut exports = Vec::new();
        let mut parts = data.split(|&b| b == 0);
        while let (Some(name), Some(value_b64)) = (parts.next(), parts.next()) {
            if !name.is_empty() {
                let name_str = match std::str::from_utf8(name) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!("Skipping export with invalid UTF-8 name: {e}");
                        continue;
                    }
                };
                if let Some(value) = Self::decode_b64(value_b64, name_str) {
                    exports.push((name_str.to_string(), value));
                }
            }
        }
        exports
    }

    /// Build a `ProcessConfig` from this task's config, merging environment variables.
    ///
    /// The process name is derived by stripping the `devenv:processes:` prefix
    /// from the task name (which all process tasks are expected to have).
    pub fn build_process_config(
        &self,
        env: &std::collections::HashMap<String, String>,
        bash: &str,
    ) -> Result<ProcessConfig> {
        let cmd = self
            .task
            .command
            .as_ref()
            .ok_or_else(|| miette::miette!("Process task {} has no command", self.task.name))?;

        let process_name = process_name(&self.task.name).to_string();

        let base = self.task.process.clone().unwrap_or_default();
        let mut config = ProcessConfig {
            name: process_name,
            exec: cmd.clone(),
            cwd: self.task.cwd.clone().map(std::path::PathBuf::from),
            ..base
        };

        // Merge devenv shell environment into process config
        // Task-level env takes precedence over shell env,
        // process-specific env takes precedence over both
        let mut merged_env = env.clone();
        merged_env.extend(self.task.env.clone());
        merged_env.extend(config.env.clone());
        config.env = merged_env;
        // Keep ProcessConfig's `bash` default when no path was resolved. Assigning
        // an empty string makes every exec probe fail to spawn with ENOENT, which
        // silently stalls `@ready` dependencies forever (#3030).
        if !bash.is_empty() {
            config.bash = bash.to_string();
        }

        Ok(config)
    }

    /// Launch a process task and return info about how it was launched.
    ///
    /// This spawns a process using NativeProcessManager but does not wait for
    /// readiness or set task status. The caller is responsible for status tracking.
    pub async fn run_process(
        &self,
        manager: &Arc<NativeProcessManager>,
        config: ProcessConfig,
    ) -> Result<ProcessLaunchInfo> {
        tracing::info!("Launching process task: {}", self.task.name);

        let requires_ready_wait = config.has_readiness_probe();
        let process_name = config.name.clone();

        // Launch the pre-registered waiting process.
        let started = manager.launch_waiting(&config.name).await?;

        let auto_start_off = started.is_none();
        if auto_start_off {
            tracing::info!("Process task {} has auto start off", self.task.name);
        }

        Ok(ProcessLaunchInfo {
            auto_start_off,
            requires_ready_wait,
            process_name,
        })
    }

    async fn check_status(
        &self,
        now: Instant,
        dependency_outputs: &Outputs,
        shell_env: &std::collections::HashMap<String, String>,
        task_activity: &Activity,
        cached_output: Option<serde_json::Value>,
    ) -> Result<Option<TaskCompleted>> {
        let Some(command) = &self.task.status else {
            return Ok(None);
        };

        self.validate_cwd()?;
        let env = self
            .prepare_env(dependency_outputs, shell_env)
            .wrap_err("Failed to prepare status command")?;
        let exports_file = Self::create_tempfile("devenv_task_exports", "")?;
        let context = ExecutionContext {
            command,
            cwd: self.task.cwd.as_deref(),
            env,
            use_sudo: self.sudo_context.is_some(),
            output_file_path: std::path::Path::new("/dev/null"),
            exports_file_path: exports_file.path(),
        };
        let mut command_process = context.build_command();
        let status_activity = devenv_activity::start!(
            Activity::command("check status")
                .command(command)
                .level(ActivityLevel::Debug)
        );

        match command_process.output().await {
            Ok(status) if status.status.success() => {
                let mut output = cached_output.unwrap_or_else(|| serde_json::json!({}));
                if let Ok(data) = tokio::fs::read(exports_file.path()).await {
                    let exports = Self::parse_exports(&data);
                    if let (false, Some(env)) = (
                        exports.is_empty(),
                        get_or_create_devenv_env_mut(&mut output),
                    ) {
                        for (key, value) in exports {
                            env.insert(key, serde_json::Value::String(value));
                        }
                    }
                }
                task_activity.cached();
                Ok(Some(TaskCompleted::Skipped(Skipped::Cached(Output(Some(
                    output,
                ))))))
            }
            Ok(_) => Ok(None),
            Err(error) => {
                status_activity.fail();
                Ok(Some(TaskCompleted::Failed(
                    now.elapsed(),
                    TaskFailure {
                        stdout: Vec::new(),
                        stderr: Vec::new(),
                        error: error.to_string(),
                    },
                )))
            }
        }
    }

    /// Run this task with a pre-assigned activity ID.
    /// The Task::Hierarchy event has already been emitted; this emits Task::Start.
    pub async fn run(
        &self,
        now: Instant,
        outputs: &Outputs,
        cache: &TaskCache,
        cancellation: CancellationToken,
        activity_id: u64,
        refresh_task_cache: bool,
        shell_env: &std::collections::HashMap<String, String>,
    ) -> Result<TaskCompleted> {
        // Create the Activity with the pre-assigned ID - this emits Task::Start
        let task_activity =
            devenv_activity::start!(Activity::task(&self.task.name).id(activity_id));

        // Run the entire task within the activity's scope for proper parent-child nesting
        self.run_inner(
            now,
            outputs,
            cache,
            cancellation,
            &task_activity,
            refresh_task_cache,
            shell_env,
        )
        .in_activity(&task_activity)
        .await
    }

    async fn run_inner(
        &self,
        now: Instant,
        outputs: &Outputs,
        cache: &TaskCache,
        cancellation: CancellationToken,
        task_activity: &Activity,
        refresh_task_cache: bool,
        shell_env: &std::collections::HashMap<String, String>,
    ) -> Result<TaskCompleted> {
        tracing::trace!(
            "running task '{}' with cache: {}, status: {}",
            self.task.name,
            self.task.cache.is_some(),
            self.task.status.is_some()
        );

        // The lock spans lookup, execution, validation, and commit. A waiter
        // therefore always rechecks the completed entry before it can execute.
        let cache_lock = if self.task.cache.is_some() {
            Some(
                cache
                    .acquire_task_lock(&self.task.name)
                    .await
                    .wrap_err_with(|| {
                        format!("Failed to acquire cache lock for task '{}'", self.task.name)
                    })?,
            )
        } else {
            None
        };

        let mut definition_hash = None;
        let mut input_before = None;
        if let (Some(cache_config), Some(_lock)) = (&self.task.cache, &cache_lock) {
            let definition = self.cache_definition_hash(outputs, shell_env)?;
            let inputs = cache.snapshot_paths(&cache_config.inputs, self.task.cwd.as_deref())?;

            if !refresh_task_cache && inputs.missing_required().is_empty() {
                match cache.get_cached_run(&self.task.name).await {
                    Ok(Some(cached))
                        if cached.definition_hash == definition
                            && cached.input_snapshot == inputs =>
                    {
                        match cache.snapshot_paths(&cache_config.outputs, self.task.cwd.as_deref())
                        {
                            Ok(outputs_now)
                                if outputs_now.missing_required().is_empty()
                                    && outputs_now == cached.output_snapshot =>
                            {
                                if self.task.status.is_none() {
                                    task_activity.cached();
                                    return Ok(TaskCompleted::Skipped(Skipped::Cached(Output(
                                        cached.output,
                                    ))));
                                }
                                if let Some(completed) = self
                                    .check_status(
                                        now,
                                        outputs,
                                        shell_env,
                                        task_activity,
                                        cached.output,
                                    )
                                    .await?
                                {
                                    return Ok(completed);
                                }
                            }
                            Ok(_) => {
                                tracing::trace!(task.name = %self.task.name, "declared outputs changed");
                            }
                            Err(error) => {
                                tracing::warn!(
                                    task.name = %self.task.name,
                                    %error,
                                    "failed to snapshot cached outputs; treating as a cache miss"
                                );
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(
                            task.name = %self.task.name,
                            %error,
                            "failed to read task cache; running task"
                        );
                    }
                }
            }

            definition_hash = Some(definition);
            input_before = Some(inputs);
        } else if !refresh_task_cache && self.task.status.is_some() {
            let cached_output = self.get_cached_output(cache).await;
            if let Some(completed) = self
                .check_status(now, outputs, shell_env, task_activity, cached_output)
                .await?
            {
                return Ok(completed);
            }
        }

        let Some(cmd) = &self.task.command else {
            task_activity.skipped();
            return Ok(TaskCompleted::Skipped(Skipped::NoCommand));
        };

        // Create a Command activity for the main execution (automatically parented to task_activity)
        let cmd_activity = devenv_activity::start!(
            Activity::command("execute command")
                .command(cmd)
                .level(ActivityLevel::Debug)
        );

        self.validate_cwd()?;

        // Prepare environment
        let env = self
            .prepare_env(outputs, shell_env)
            .wrap_err("Failed to prepare task environment")?;

        // Create temporary files for task output and exports
        let outputs_file = Self::create_tempfile("devenv_task_output", ".json")?;
        let exports_file = Self::create_tempfile("devenv_task_exports", "")?;

        // Build execution context
        let ctx = ExecutionContext {
            command: cmd,
            cwd: self.task.cwd.as_deref(),
            env,
            use_sudo: self.sudo_context.is_some(),
            output_file_path: outputs_file.path(),
            exports_file_path: exports_file.path(),
        };

        // Execute using the provided executor
        let callback = ActivityCallback::new(task_activity);
        let result = crate::executor::execute(ctx, &callback, cancellation).await;

        if result.error.as_deref() == Some("Task cancelled") {
            cmd_activity.cancel();
            task_activity.cancel();
            return Ok(TaskCompleted::Cancelled(Some(now.elapsed())));
        }

        if !result.success {
            cmd_activity.fail();
            task_activity.fail();
            return Ok(TaskCompleted::Failed(
                now.elapsed(),
                TaskFailure {
                    stdout: result.stdout_lines,
                    stderr: result.stderr_lines,
                    error: result.error.unwrap_or_else(|| "Unknown error".to_string()),
                },
            ));
        }

        let output = Self::get_outputs(&outputs_file, &exports_file, &result.stdout_lines).await;
        if let (Some(cache_config), Some(definition), Some(inputs_before)) = (
            &self.task.cache,
            definition_hash.as_deref(),
            input_before.as_ref(),
        ) {
            let inputs_after =
                cache.snapshot_paths(&cache_config.inputs, self.task.cwd.as_deref())?;
            let outputs_after =
                cache.snapshot_paths(&cache_config.outputs, self.task.cwd.as_deref())?;
            let missing_inputs = inputs_after.missing_required();
            let missing_outputs = outputs_after.missing_required();
            let contract_error = if !missing_inputs.is_empty() {
                Some(format!(
                    "required cache inputs are missing after task success: {}",
                    missing_inputs.join(", ")
                ))
            } else if !missing_outputs.is_empty() {
                Some(format!(
                    "required cache outputs are missing after task success: {}",
                    missing_outputs.join(", ")
                ))
            } else if inputs_before != &inputs_after {
                Some("cache inputs changed while the task was running".to_string())
            } else {
                None
            };

            if let Some(error) = contract_error {
                cmd_activity.fail();
                task_activity.fail();
                return Ok(TaskCompleted::Failed(
                    now.elapsed(),
                    TaskFailure {
                        stdout: result.stdout_lines,
                        stderr: result.stderr_lines,
                        error,
                    },
                ));
            }

            if let Err(error) = cache
                .commit_cached_run(
                    &self.task.name,
                    definition,
                    &inputs_after,
                    &outputs_after,
                    output.0.as_ref(),
                )
                .await
            {
                tracing::warn!(
                    task.name = %self.task.name,
                    %error,
                    "failed to commit task cache; task result remains successful"
                );
            }
        }

        Ok(TaskCompleted::Success(now.elapsed(), output))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use crate::TaskType;
    use base64::Engine;
    use proptest::prelude::*;
    use std::time::Instant;

    fn encode(s: &str) -> String {
        B64.encode(s)
    }

    fn make_line(key: &str, value: &str) -> (Instant, String) {
        (
            Instant::now(),
            format!("DEVENV_EXPORT:{}={}", encode(key), encode(value)),
        )
    }

    fn make_file_data(pairs: &[(&str, &str)]) -> Vec<u8> {
        let mut data = Vec::new();
        for (name, value) in pairs {
            data.extend_from_slice(name.as_bytes());
            data.push(0);
            data.extend_from_slice(B64.encode(value).as_bytes());
            data.push(0);
        }
        data
    }

    // -- parse_exports tests --

    #[test]
    fn parse_exports_empty() {
        assert!(TaskState::parse_exports(b"").is_empty());
    }

    #[test]
    fn parse_exports_single() {
        let data = make_file_data(&[("FOO", "bar")]);
        let result = TaskState::parse_exports(&data);
        assert_eq!(result, vec![("FOO".into(), "bar".into())]);
    }

    #[test]
    fn parse_exports_multiple() {
        let data = make_file_data(&[("A", "1"), ("B", "2"), ("C", "3")]);
        let result = TaskState::parse_exports(&data);
        assert_eq!(
            result,
            vec![
                ("A".into(), "1".into()),
                ("B".into(), "2".into()),
                ("C".into(), "3".into()),
            ]
        );
    }

    #[test]
    fn parse_exports_empty_value() {
        let data = make_file_data(&[("KEY", "")]);
        let result = TaskState::parse_exports(&data);
        assert_eq!(result, vec![("KEY".into(), String::new())]);
    }

    #[test]
    fn parse_exports_value_with_special_chars() {
        let data = make_file_data(&[("P", "hello world"), ("Q", "a=b=c"), ("R", "line\nnewline")]);
        let result = TaskState::parse_exports(&data);
        assert_eq!(
            result,
            vec![
                ("P".into(), "hello world".into()),
                ("Q".into(), "a=b=c".into()),
                ("R".into(), "line\nnewline".into()),
            ]
        );
    }

    #[test]
    fn parse_exports_skips_empty_name() {
        // Manually craft data with an empty name: \0<base64>\0
        let mut data = Vec::new();
        data.push(0);
        data.extend_from_slice(B64.encode("val").as_bytes());
        data.push(0);
        // Then a valid pair
        data.extend_from_slice(b"GOOD");
        data.push(0);
        data.extend_from_slice(B64.encode("ok").as_bytes());
        data.push(0);

        let result = TaskState::parse_exports(&data);
        assert_eq!(result, vec![("GOOD".into(), "ok".into())]);
    }

    #[test]
    fn parse_exports_invalid_base64_skipped() {
        let mut data = Vec::new();
        data.extend_from_slice(b"NAME");
        data.push(0);
        data.extend_from_slice(b"!!!not-base64!!!");
        data.push(0);

        let result = TaskState::parse_exports(&data);
        assert!(result.is_empty());
    }

    #[test]
    fn parse_exports_odd_field_ignored() {
        // A trailing name without a value pair is ignored
        let mut data = make_file_data(&[("A", "1")]);
        data.extend_from_slice(b"ORPHAN");
        let result = TaskState::parse_exports(&data);
        assert_eq!(result, vec![("A".into(), "1".into())]);
    }

    // -- parse_stdout_exports tests --

    #[test]
    fn parse_stdout_exports_empty() {
        assert!(TaskState::parse_stdout_exports(&[]).is_empty());
    }

    #[test]
    fn parse_stdout_exports_ignores_non_export_lines() {
        let lines = vec![
            (Instant::now(), "some normal output".into()),
            (Instant::now(), "building stuff...".into()),
        ];
        assert!(TaskState::parse_stdout_exports(&lines).is_empty());
    }

    #[test]
    fn parse_stdout_exports_single() {
        let lines = vec![make_line("MY_VAR", "my_value")];
        let result = TaskState::parse_stdout_exports(&lines);
        assert_eq!(result, vec![("MY_VAR".into(), "my_value".into())]);
    }

    #[test]
    fn parse_stdout_exports_mixed_lines() {
        let lines = vec![
            (Instant::now(), "before".into()),
            make_line("X", "1"),
            (Instant::now(), "middle".into()),
            make_line("Y", "2"),
            (Instant::now(), "after".into()),
        ];
        let result = TaskState::parse_stdout_exports(&lines);
        assert_eq!(
            result,
            vec![("X".into(), "1".into()), ("Y".into(), "2".into())]
        );
    }

    #[test]
    fn parse_stdout_exports_short_key() {
        // 1-char key "A" -> base64 "QQ==" (4 chars with padding)
        let lines = vec![make_line("A", "val")];
        let result = TaskState::parse_stdout_exports(&lines);
        assert_eq!(result, vec![("A".into(), "val".into())]);
    }

    #[test]
    fn parse_stdout_exports_empty_value() {
        let lines = vec![make_line("KEY", "")];
        let result = TaskState::parse_stdout_exports(&lines);
        assert_eq!(result, vec![("KEY".into(), String::new())]);
    }

    #[test]
    fn parse_stdout_exports_value_with_equals() {
        let lines = vec![make_line("PATH", "/usr/bin:/bin")];
        let result = TaskState::parse_stdout_exports(&lines);
        assert_eq!(result, vec![("PATH".into(), "/usr/bin:/bin".into())]);
    }

    // -- build_process_config bash resolution tests --

    fn process_task_state() -> TaskState {
        TaskState::new(
            TaskConfig {
                name: "devenv:processes:demo".to_string(),
                r#type: TaskType::Process,
                command: Some("sleep infinity".to_string()),
                ..Default::default()
            },
            VerbosityLevel::Normal,
            None,
        )
    }

    #[test]
    fn build_process_config_uses_resolved_bash() {
        let ts = process_task_state();
        let config = ts
            .build_process_config(
                &std::collections::HashMap::new(),
                "/nix/store/bash/bin/bash",
            )
            .unwrap();
        assert_eq!(config.bash, "/nix/store/bash/bin/bash");
    }

    /// An empty bash path must not overwrite the default: exec probes spawn the
    /// program named by this field, and `Command::new("")` fails with ENOENT on
    /// every attempt, stalling `@ready` dependencies forever (#3030).
    #[test]
    fn build_process_config_keeps_default_bash_when_unresolved() {
        let ts = process_task_state();
        let config = ts
            .build_process_config(&std::collections::HashMap::new(), "")
            .unwrap();
        assert_eq!(config.bash, "bash");
    }

    // -- proptest round-trip tests --

    proptest! {
        #[test]
        fn parse_exports_roundtrip(pairs in prop::collection::vec(("[A-Za-z_][A-Za-z0-9_]{0,30}", ".*"), 0..20)) {
            let refs: Vec<(&str, &str)> = pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            let data = make_file_data(&refs);
            let result = TaskState::parse_exports(&data);
            prop_assert_eq!(result, pairs);
        }

        #[test]
        fn parse_stdout_exports_roundtrip(pairs in prop::collection::vec(("[A-Za-z_][A-Za-z0-9_]{0,30}", ".*"), 0..20)) {
            let lines: Vec<(Instant, String)> = pairs.iter().map(|(k, v)| make_line(k, v)).collect();
            let result = TaskState::parse_stdout_exports(&lines);
            prop_assert_eq!(result, pairs);
        }
    }
}
