use crate::config::Config;
use crate::context_book::ContextBookHandle;
use anyhow::Result;
use chrono::Utc;
use std::future::Future;
use std::path::PathBuf;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

const STATUS_FLUSH_SECONDS: u64 = 5;
const SHUTDOWN_GRACE_MILLIS: u64 = 750;

/// Wait for shutdown signal (SIGINT or SIGTERM).
/// SIGHUP is explicitly ignored so the daemon survives terminal/SSH disconnects.
async fn wait_for_shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        let mut sighup = signal(SignalKind::hangup())?;

        loop {
            tokio::select! {
                _ = sigint.recv() => {
                    tracing::info!("Received SIGINT, shutting down...");
                    break;
                }
                _ = sigterm.recv() => {
                    tracing::info!("Received SIGTERM, shutting down...");
                    break;
                }
                _ = sighup.recv() => {
                    tracing::info!("Received SIGHUP, ignoring (daemon stays running)");
                }
            }
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        tracing::info!("Received Ctrl+C, shutting down...");
    }

    Ok(())
}

pub async fn run(config: Config, host: String, port: u16) -> Result<()> {
    let initial_backoff = config.reliability.channel_initial_backoff_secs.max(1);
    let max_backoff = config
        .reliability
        .channel_max_backoff_secs
        .max(initial_backoff);
    let context_book_handle = crate::context_book::bootstrap(&config).handle;
    let shutdown = CancellationToken::new();

    crate::health::mark_component_ok("daemon");

    if config.heartbeat.enabled {
        let _ =
            crate::heartbeat::engine::HeartbeatEngine::ensure_heartbeat_file(&config.workspace_dir)
                .await;
    }

    let mut handles: Vec<JoinHandle<()>> = vec![spawn_state_writer(
        config.clone(),
        Some(context_book_handle.clone()),
        shutdown.child_token(),
    )];

    {
        let gateway_cfg = config.clone();
        let gateway_host = host.clone();
        handles.push(spawn_component_supervisor(
            "gateway",
            initial_backoff,
            max_backoff,
            shutdown.child_token(),
            move || {
                let cfg = gateway_cfg.clone();
                let host = gateway_host.clone();
                async move { Box::pin(crate::gateway::run_gateway(&host, port, cfg)).await }
            },
        ));
    }

    {
        if has_supervised_channels(&config) {
            let channels_cfg = config.clone();
            handles.push(spawn_component_supervisor(
                "channels",
                initial_backoff,
                max_backoff,
                shutdown.child_token(),
                move || {
                    let cfg = channels_cfg.clone();
                    async move { Box::pin(crate::channels::start_channels(cfg)).await }
                },
            ));
        } else {
            crate::health::mark_component_ok("channels");
            tracing::info!("No real-time channels configured; channel supervisor disabled");
        }
    }

    if config.heartbeat.enabled {
        let heartbeat_cfg = config.clone();
        handles.push(spawn_component_supervisor(
            "heartbeat",
            initial_backoff,
            max_backoff,
            shutdown.child_token(),
            move || {
                let cfg = heartbeat_cfg.clone();
                async move { Box::pin(run_heartbeat_worker(cfg)).await }
            },
        ));
    }

    if config.cron.enabled {
        let scheduler_cfg = config.clone();
        handles.push(spawn_component_supervisor(
            "scheduler",
            initial_backoff,
            max_backoff,
            shutdown.child_token(),
            move || {
                let cfg = scheduler_cfg.clone();
                async move { Box::pin(crate::cron::scheduler::run(cfg)).await }
            },
        ));
    } else {
        crate::health::mark_component_ok("scheduler");
        tracing::info!("Cron disabled; scheduler supervisor not started");
    }

    if config.context_book.enabled {
        let context_book_cfg = config.clone();
        let handle = context_book_handle.clone();
        let context_book_shutdown = shutdown.child_token();
        handles.push(spawn_component_supervisor(
            "context_book",
            initial_backoff,
            max_backoff,
            shutdown.child_token(),
            move || {
                let cfg = context_book_cfg.clone();
                let handle = handle.clone();
                let shutdown = context_book_shutdown.child_token();
                async move {
                    Box::pin(crate::context_book::worker::run(
                        cfg,
                        handle,
                        Some(shutdown),
                    ))
                    .await
                }
            },
        ));
    } else {
        context_book_handle.mark_disabled();
        crate::health::mark_component_ok("context_book");
        tracing::info!("Context Book disabled; worker supervisor not started");
    }

    println!("🧠 ZeroClaw daemon started");
    println!("   Gateway:  http://{host}:{port}");
    println!("   Components: gateway, channels, heartbeat, scheduler, context_book");
    if config.gateway.require_pairing {
        println!("   Pairing:    enabled (code appears in gateway output above)");
    }
    println!("   Ctrl+C or SIGTERM to stop");

    // Wait for shutdown signal (SIGINT or SIGTERM)
    wait_for_shutdown_signal().await?;
    crate::health::mark_component_error("daemon", "shutdown requested");
    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(SHUTDOWN_GRACE_MILLIS)).await;

    for handle in &handles {
        handle.abort();
    }
    for handle in handles {
        let _ = handle.await;
    }

    Ok(())
}

pub fn state_file_path(config: &Config) -> PathBuf {
    config
        .config_path
        .parent()
        .map_or_else(|| PathBuf::from("."), PathBuf::from)
        .join("daemon_state.json")
}

fn spawn_state_writer(
    config: Config,
    context_book: Option<ContextBookHandle>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let path = state_file_path(&config);
        if let Some(parent) = path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }

        let mut interval = tokio::time::interval(Duration::from_secs(STATUS_FLUSH_SECONDS));
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                _ = interval.tick() => {}
            }
            let mut json = crate::health::snapshot_json();
            if let Some(obj) = json.as_object_mut() {
                obj.insert(
                    "written_at".into(),
                    serde_json::json!(Utc::now().to_rfc3339()),
                );
                if let Some(handle) = &context_book {
                    let context_book_json = serde_json::to_value(handle.status_report())
                        .unwrap_or_else(|_| serde_json::json!({"error": "failed to serialize"}));
                    obj.insert("context_book".into(), context_book_json);
                }
            }
            let data = serde_json::to_vec_pretty(&json).unwrap_or_else(|_| b"{}".to_vec());
            let _ = tokio::fs::write(&path, data).await;
        }
    })
}

fn spawn_component_supervisor<F, Fut>(
    name: &'static str,
    initial_backoff_secs: u64,
    max_backoff_secs: u64,
    shutdown: CancellationToken,
    mut run_component: F,
) -> JoinHandle<()>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    tokio::spawn(async move {
        let mut backoff = initial_backoff_secs.max(1);
        let max_backoff = max_backoff_secs.max(backoff);

        loop {
            if shutdown.is_cancelled() {
                tracing::info!("Daemon component '{name}' stop requested");
                break;
            }

            crate::health::mark_component_ok(name);
            match run_component().await {
                Ok(()) => {
                    crate::health::mark_component_error(name, "component exited unexpectedly");
                    tracing::warn!("Daemon component '{name}' exited unexpectedly");
                    // Clean exit — reset backoff since the component ran successfully
                    backoff = initial_backoff_secs.max(1);
                }
                Err(e) => {
                    crate::health::mark_component_error(name, e.to_string());
                    tracing::error!("Daemon component '{name}' failed: {e}");
                }
            }

            crate::health::bump_component_restart(name);
            tokio::select! {
                () = shutdown.cancelled() => {
                    tracing::info!("Daemon component '{name}' stopping before restart");
                    break;
                }
                () = tokio::time::sleep(Duration::from_secs(backoff)) => {}
            }
            // Double backoff AFTER sleeping so first error uses initial_backoff
            backoff = backoff.saturating_mul(2).min(max_backoff);
        }
    })
}

async fn run_heartbeat_worker(config: Config) -> Result<()> {
    use crate::heartbeat::engine::{
        HeartbeatEngine, HeartbeatTask, TaskPriority, TaskStatus, compute_adaptive_interval,
    };
    use std::sync::Arc;

    let observer: std::sync::Arc<dyn crate::observability::Observer> =
        std::sync::Arc::from(crate::observability::create_observer(&config.observability));
    let engine = HeartbeatEngine::new(
        config.heartbeat.clone(),
        config.workspace_dir.clone(),
        observer,
    );
    let metrics = engine.metrics();
    let delivery = resolve_heartbeat_delivery(&config)?;
    let two_phase = config.heartbeat.two_phase;
    let adaptive = config.heartbeat.adaptive;
    let start_time = std::time::Instant::now();

    // ── Deadman watcher ──────────────────────────────────────────
    let deadman_timeout = config.heartbeat.deadman_timeout_minutes;
    if deadman_timeout > 0 {
        let dm_metrics = Arc::clone(&metrics);
        let dm_config = config.clone();
        let dm_delivery = delivery.clone();
        tokio::spawn(async move {
            let check_interval = Duration::from_secs(60);
            let timeout = chrono::Duration::minutes(i64::from(deadman_timeout));
            loop {
                tokio::time::sleep(check_interval).await;
                let last_tick = dm_metrics.lock().last_tick_at;
                if let Some(last) = last_tick {
                    if chrono::Utc::now() - last > timeout {
                        let alert = format!(
                            "⚠️ Heartbeat dead-man's switch: no tick in {deadman_timeout} minutes"
                        );
                        let (channel, target) =
                            if let Some(ch) = &dm_config.heartbeat.deadman_channel {
                                let to = dm_config
                                    .heartbeat
                                    .deadman_to
                                    .as_deref()
                                    .or(dm_config.heartbeat.to.as_deref())
                                    .unwrap_or_default();
                                (ch.clone(), to.to_string())
                            } else if let Some((ch, to)) = &dm_delivery {
                                (ch.clone(), to.clone())
                            } else {
                                continue;
                            };
                        let _ = crate::cron::scheduler::deliver_announcement(
                            &dm_config, &channel, &target, &alert,
                        )
                        .await;
                    }
                }
            }
        });
    }

    let base_interval = config.heartbeat.interval_minutes.max(5);
    let mut sleep_mins = base_interval;

    loop {
        tokio::time::sleep(Duration::from_secs(u64::from(sleep_mins) * 60)).await;

        // Update uptime
        {
            let mut m = metrics.lock();
            m.uptime_secs = start_time.elapsed().as_secs();
        }

        let tick_start = std::time::Instant::now();

        // Collect runnable tasks (active only, sorted by priority)
        let mut tasks = engine.collect_runnable_tasks().await?;
        let has_high_priority = tasks.iter().any(|t| t.priority == TaskPriority::High);

        if tasks.is_empty() {
            if let Some(fallback) = config
                .heartbeat
                .message
                .as_deref()
                .map(str::trim)
                .filter(|m| !m.is_empty())
            {
                tasks.push(HeartbeatTask {
                    text: fallback.to_string(),
                    priority: TaskPriority::Medium,
                    status: TaskStatus::Active,
                });
            } else {
                #[allow(clippy::cast_precision_loss)]
                let elapsed = tick_start.elapsed().as_millis() as f64;
                metrics.lock().record_success(elapsed);
                continue;
            }
        }

        // ── Phase 1: LLM decision (two-phase mode) ──────────────
        let tasks_to_run = if two_phase {
            let decision_prompt = format!(
                "[Heartbeat Task | decision] {}",
                HeartbeatEngine::build_decision_prompt(&tasks),
            );
            match Box::pin(crate::agent::run(
                config.clone(),
                Some(decision_prompt),
                None,
                None,
                0.0,
                vec![],
                false,
                None,
                None,
            ))
            .await
            {
                Ok(response) => {
                    let indices = HeartbeatEngine::parse_decision_response(&response, tasks.len());
                    if indices.is_empty() {
                        tracing::info!("💓 Heartbeat Phase 1: skip (nothing to do)");
                        crate::health::mark_component_ok("heartbeat");
                        #[allow(clippy::cast_precision_loss)]
                        let elapsed = tick_start.elapsed().as_millis() as f64;
                        metrics.lock().record_success(elapsed);
                        continue;
                    }
                    tracing::info!(
                        "💓 Heartbeat Phase 1: run {} of {} tasks",
                        indices.len(),
                        tasks.len()
                    );
                    indices
                        .into_iter()
                        .filter_map(|i| tasks.get(i).cloned())
                        .collect()
                }
                Err(e) => {
                    tracing::warn!("💓 Heartbeat Phase 1 failed, running all tasks: {e}");
                    tasks
                }
            }
        } else {
            tasks
        };

        // ── Phase 2: Execute selected tasks ─────────────────────
        // Re-read session context on every tick so we pick up messages
        // that arrived since the daemon started.
        let session_context = if config.heartbeat.load_session_context {
            load_heartbeat_session_context(&config)
        } else {
            None
        };
        let context_book_focus = tasks_to_run
            .iter()
            .map(|task| task.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        let context_book_context =
            load_context_book_prompt_block(&config, &context_book_focus).await;

        // Create memory once per tick for recall + consolidation.
        let heartbeat_memory: Option<Box<dyn crate::memory::Memory>> =
            crate::memory::create_memory(
                &config.memory,
                &config.workspace_dir,
                config.api_key.as_deref(),
            )
            .ok();

        let mut tick_had_error = false;
        for task in &tasks_to_run {
            let task_start = std::time::Instant::now();
            let task_prompt = format!("[Heartbeat Task | {}] {}", task.priority, task.text);

            // Recall relevant memories so heartbeat tasks have context awareness.
            let memory_context = if let Some(ref mem) = heartbeat_memory {
                match mem.recall(&task.text, 5, None, None, None).await {
                    Ok(entries) if !entries.is_empty() => {
                        let ctx: String = entries
                            .iter()
                            .map(|e| format!("- {}: {}", e.key, e.content))
                            .collect::<Vec<_>>()
                            .join("\n");
                        Some(format!("[Memory context]\n{ctx}\n"))
                    }
                    _ => None,
                }
            } else {
                None
            };

            let prompt = compose_heartbeat_task_prompt(
                &task_prompt,
                session_context.as_deref(),
                memory_context.as_deref(),
                context_book_context.as_deref(),
            );
            let temp = config.default_temperature;
            match Box::pin(crate::agent::run(
                config.clone(),
                Some(prompt),
                None,
                None,
                temp,
                vec![],
                false,
                None,
                None,
            ))
            .await
            {
                Ok(output) => {
                    crate::health::mark_component_ok("heartbeat");
                    #[allow(clippy::cast_possible_truncation)]
                    let duration_ms = task_start.elapsed().as_millis() as i64;
                    let now = chrono::Utc::now();
                    let _ = crate::heartbeat::store::record_run(
                        &config.workspace_dir,
                        &task.text,
                        &task.priority.to_string(),
                        now - chrono::Duration::milliseconds(duration_ms),
                        now,
                        "ok",
                        Some(output.as_str()),
                        duration_ms,
                        config.heartbeat.max_run_history,
                    );
                    // Consolidate heartbeat output to memory for cross-session awareness.
                    if config.memory.auto_save && output.chars().count() >= 50 {
                        if let Some(ref mem) = heartbeat_memory {
                            let key = format!("heartbeat_{}", uuid::Uuid::new_v4());
                            let summary = if output.len() > 500 {
                                // Find a valid UTF-8 char boundary at or before 500.
                                let mut end = 500;
                                while end > 0 && !output.is_char_boundary(end) {
                                    end -= 1;
                                }
                                &output[..end]
                            } else {
                                &output
                            };
                            let _ = mem
                                .store(
                                    &key,
                                    &format!("Heartbeat task '{}': {}", task.text, summary),
                                    crate::memory::MemoryCategory::Daily,
                                    None,
                                )
                                .await;
                        }
                    }

                    let announcement = if output.trim().is_empty() {
                        format!("💓 heartbeat task completed: {}", task.text)
                    } else {
                        output
                    };
                    if let Some((channel, target)) = &delivery {
                        if let Err(e) = crate::cron::scheduler::deliver_announcement(
                            &config,
                            channel,
                            target,
                            &announcement,
                        )
                        .await
                        {
                            crate::health::mark_component_error(
                                "heartbeat",
                                format!("delivery failed: {e}"),
                            );
                            tracing::warn!("Heartbeat delivery failed: {e}");
                        }
                    }
                }
                Err(e) => {
                    tick_had_error = true;
                    #[allow(clippy::cast_possible_truncation)]
                    let duration_ms = task_start.elapsed().as_millis() as i64;
                    let now = chrono::Utc::now();
                    let _ = crate::heartbeat::store::record_run(
                        &config.workspace_dir,
                        &task.text,
                        &task.priority.to_string(),
                        now - chrono::Duration::milliseconds(duration_ms),
                        now,
                        "error",
                        Some(&e.to_string()),
                        duration_ms,
                        config.heartbeat.max_run_history,
                    );
                    crate::health::mark_component_error("heartbeat", e.to_string());
                    tracing::warn!("Heartbeat task failed: {e}");
                }
            }
        }

        // Update metrics
        #[allow(clippy::cast_precision_loss)]
        let tick_elapsed = tick_start.elapsed().as_millis() as f64;
        {
            let mut m = metrics.lock();
            if tick_had_error {
                m.record_failure(tick_elapsed);
            } else {
                m.record_success(tick_elapsed);
            }
        }

        // Compute next sleep interval
        if adaptive {
            let failures = metrics.lock().consecutive_failures;
            sleep_mins = compute_adaptive_interval(
                base_interval,
                config.heartbeat.min_interval_minutes,
                config.heartbeat.max_interval_minutes,
                failures,
                has_high_priority,
            );
        } else {
            sleep_mins = base_interval;
        }
    }
}

/// Resolve delivery target: explicit config > auto-detect first configured channel.
fn resolve_heartbeat_delivery(config: &Config) -> Result<Option<(String, String)>> {
    let channel = config
        .heartbeat
        .target
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let target = config
        .heartbeat
        .to
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    match (channel, target) {
        // Both explicitly set — validate and use.
        (Some(channel), Some(target)) => {
            validate_heartbeat_channel_config(config, channel)?;
            Ok(Some((channel.to_string(), target.to_string())))
        }
        // Only one set — error.
        (Some(_), None) => anyhow::bail!("heartbeat.to is required when heartbeat.target is set"),
        (None, Some(_)) => anyhow::bail!("heartbeat.target is required when heartbeat.to is set"),
        // Neither set — try auto-detect the first configured channel.
        (None, None) => Ok(auto_detect_heartbeat_channel(config)),
    }
}

/// Load recent conversation history for the heartbeat's delivery target and
/// format it as a text preamble to inject into the task prompt.
///
/// Scans `{workspace}/sessions/` for JSONL files whose name starts with
/// `{channel}_` and ends with `_{to}.jsonl` (or exactly `{channel}_{to}.jsonl`),
/// then picks the most recently modified match. This handles session key
/// formats such as `telegram_diskiller.jsonl` and
/// `telegram_5673725398_diskiller.jsonl`.
/// Returns `None` when `target`/`to` are not configured or no session exists.
const HEARTBEAT_SESSION_CONTEXT_MESSAGES: usize = 20;

fn load_heartbeat_session_context(config: &Config) -> Option<String> {
    use crate::providers::traits::ChatMessage;

    let channel = config
        .heartbeat
        .target
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())?;
    let to = config
        .heartbeat
        .to
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())?;

    if channel.contains('/') || channel.contains('\\') || to.contains('/') || to.contains('\\') {
        tracing::warn!("heartbeat session context: channel/to contains path separators, skipping");
        return None;
    }

    let sessions_dir = config.workspace_dir.join("sessions");

    // Find the most recently modified JSONL file that belongs to this target.
    // Matches both `{channel}_{to}.jsonl` and `{channel}_{anything}_{to}.jsonl`.
    let prefix = format!("{channel}_");
    let suffix = format!("_{to}.jsonl");
    let exact = format!("{channel}_{to}.jsonl");
    let mid_prefix = format!("{channel}_{to}_");

    let path = std::fs::read_dir(&sessions_dir)
        .ok()?
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            name.ends_with(".jsonl")
                && (name == exact
                    || (name.starts_with(&prefix) && name.ends_with(&suffix))
                    || name.starts_with(&mid_prefix))
        })
        .max_by_key(|e| {
            e.metadata()
                .and_then(|m| m.modified())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
        })
        .map(|e| e.path())?;

    if !path.exists() {
        tracing::debug!("💓 Heartbeat session context: no session file found for {channel}/{to}");
        return None;
    }

    let messages = load_jsonl_messages(&path);
    if messages.is_empty() {
        return None;
    }

    let recent: Vec<&ChatMessage> = messages
        .iter()
        .filter(|m| m.role == "user" || m.role == "assistant")
        .rev()
        .take(HEARTBEAT_SESSION_CONTEXT_MESSAGES)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();

    // Only inject context if there is at least one real user message in the
    // window. If the JSONL contains only assistant messages (e.g. previous
    // heartbeat outputs with no reply yet), skip context to avoid feeding
    // Monika's own messages back to her in a loop.
    let has_user_message = recent.iter().any(|m| m.role == "user");
    if !has_user_message {
        tracing::debug!(
            "💓 Heartbeat session context: no user messages in recent history — skipping"
        );
        return None;
    }

    // Use the session file's mtime as a proxy for when the last message arrived.
    let last_message_age = std::fs::metadata(&path)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|mtime| mtime.elapsed().ok());

    let silence_note = match last_message_age {
        Some(age) => {
            let mins = age.as_secs() / 60;
            if mins < 60 {
                format!("(last message ~{mins} minutes ago)\n")
            } else {
                let hours = mins / 60;
                let rem = mins % 60;
                if rem == 0 {
                    format!("(last message ~{hours}h ago)\n")
                } else {
                    format!("(last message ~{hours}h {rem}m ago)\n")
                }
            }
        }
        None => String::new(),
    };

    tracing::debug!(
        "💓 Heartbeat session context: {} messages from {}, silence: {}",
        recent.len(),
        path.display(),
        silence_note.trim(),
    );

    let mut ctx = format!(
        "[Recent conversation history — use this for context when composing your message] {silence_note}",
    );
    for msg in &recent {
        let label = if msg.role == "user" { "User" } else { "You" };
        // Truncate very long messages to avoid bloating the prompt.
        // Use char_indices to avoid panicking on multi-byte UTF-8 characters.
        let content = if msg.content.len() > 500 {
            let truncate_at = msg
                .content
                .char_indices()
                .map(|(i, _)| i)
                .take_while(|&i| i <= 500)
                .last()
                .unwrap_or(0);
            format!("{}…", &msg.content[..truncate_at])
        } else {
            msg.content.clone()
        };
        ctx.push_str(label);
        ctx.push_str(": ");
        ctx.push_str(&content);
        ctx.push('\n');
    }

    Some(ctx)
}

async fn load_context_book_prompt_block(config: &Config, focus: &str) -> Option<String> {
    if !config.context_book.enabled {
        return None;
    }

    match crate::context_book::bootstrap(config)
        .service
        .build_policy_reference(
            crate::context_book::ContextBookPolicyReadMode::Auto,
            Some(focus),
        )
        .await
    {
        Ok(Some(reference)) if reference.has_actionable_items() => Some(reference.prompt_block),
        Ok(_) => None,
        Err(error) => {
            tracing::warn!("context_book heartbeat helper skipped: {error}");
            None
        }
    }
}

fn compose_heartbeat_task_prompt(
    task_prompt: &str,
    session_context: Option<&str>,
    memory_context: Option<&str>,
    context_book_context: Option<&str>,
) -> String {
    let mut sections = Vec::new();
    if let Some(memory_context) = memory_context {
        sections.push(memory_context.trim_end().to_string());
    }
    if let Some(context_book_context) = context_book_context {
        sections.push(context_book_context.trim_end().to_string());
    }
    if let Some(session_context) = session_context {
        sections.push(session_context.trim_end().to_string());
    }
    sections.push(task_prompt.to_string());
    sections.join("\n\n")
}

/// Read the last `HEARTBEAT_SESSION_CONTEXT_MESSAGES` `ChatMessage` lines from
/// a JSONL session file using a bounded rolling window so we never hold the
/// entire file in memory.
fn load_jsonl_messages(path: &std::path::Path) -> Vec<crate::providers::traits::ChatMessage> {
    use std::collections::VecDeque;
    use std::io::BufRead;

    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return Vec::new(),
    };
    let reader = std::io::BufReader::new(file);
    let mut window: VecDeque<crate::providers::traits::ChatMessage> =
        VecDeque::with_capacity(HEARTBEAT_SESSION_CONTEXT_MESSAGES + 1);
    for line in reader.lines() {
        let Ok(line) = line else { continue };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Ok(msg) = serde_json::from_str::<crate::providers::traits::ChatMessage>(trimmed) {
            window.push_back(msg);
            if window.len() > HEARTBEAT_SESSION_CONTEXT_MESSAGES {
                window.pop_front();
            }
        }
    }
    window.into_iter().collect()
}

/// Auto-detect the best channel for heartbeat delivery by checking which
/// channels are configured. Returns the first match in priority order.
fn auto_detect_heartbeat_channel(config: &Config) -> Option<(String, String)> {
    // Priority order: telegram > discord > slack > mattermost
    if let Some(tg) = &config.channels_config.telegram {
        // Use the first allowed_user as target, or fall back to empty (broadcast)
        let target = tg.allowed_users.first().cloned().unwrap_or_default();
        if !target.is_empty() {
            return Some(("telegram".to_string(), target));
        }
    }
    if config.channels_config.discord.is_some() {
        // Discord requires explicit target — can't auto-detect
        return None;
    }
    if config.channels_config.slack.is_some() {
        // Slack requires explicit target
        return None;
    }
    if config.channels_config.mattermost.is_some() {
        // Mattermost requires explicit target
        return None;
    }
    None
}

fn validate_heartbeat_channel_config(config: &Config, channel: &str) -> Result<()> {
    match channel.to_ascii_lowercase().as_str() {
        "telegram" => {
            if config.channels_config.telegram.is_none() {
                anyhow::bail!(
                    "heartbeat.target is set to telegram but channels_config.telegram is not configured"
                );
            }
        }
        "discord" => {
            if config.channels_config.discord.is_none() {
                anyhow::bail!(
                    "heartbeat.target is set to discord but channels_config.discord is not configured"
                );
            }
        }
        "slack" => {
            if config.channels_config.slack.is_none() {
                anyhow::bail!(
                    "heartbeat.target is set to slack but channels_config.slack is not configured"
                );
            }
        }
        "mattermost" => {
            if config.channels_config.mattermost.is_none() {
                anyhow::bail!(
                    "heartbeat.target is set to mattermost but channels_config.mattermost is not configured"
                );
            }
        }
        other => anyhow::bail!("unsupported heartbeat.target channel: {other}"),
    }

    Ok(())
}

fn has_supervised_channels(config: &Config) -> bool {
    config
        .channels_config
        .channels_except_webhook()
        .iter()
        .any(|(_, ok)| *ok)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::profiles::{AuthProfile, AuthProfileKind, AuthProfilesStore, profile_id};
    use crate::auth::state_dir_from_config;
    use axum::{Json, Router, routing::get};
    use chrono::Utc;
    use serde_json::json;
    use std::collections::BTreeMap;
    use tempfile::TempDir;
    use tokio::net::TcpListener;

    fn test_config(tmp: &TempDir) -> Config {
        let config = Config {
            workspace_dir: tmp.path().join("workspace"),
            config_path: tmp.path().join("config.toml"),
            ..Config::default()
        };
        std::fs::create_dir_all(&config.workspace_dir).unwrap();
        config
    }

    async fn seed_token_profile(config: &Config, agent_id: &str) {
        let state_dir = state_dir_from_config(config);
        let store = AuthProfilesStore::new(&state_dir, config.secrets.encrypt);
        store
            .upsert_profile(
                AuthProfile {
                    id: profile_id("context-book", "default"),
                    provider: "context-book".into(),
                    profile_name: "default".into(),
                    kind: AuthProfileKind::Token,
                    account_id: None,
                    workspace_id: None,
                    token_set: None,
                    token: Some("service-token".into()),
                    metadata: BTreeMap::from([("agent_id".to_string(), agent_id.to_string())]),
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                },
                true,
            )
            .await
            .expect("seed token profile");
    }

    #[test]
    fn state_file_path_uses_config_directory() {
        let tmp = TempDir::new().unwrap();
        let config = test_config(&tmp);

        let handle = crate::context_book::shared_handle(&config);
        let path = state_file_path(&config);
        assert_eq!(path, tmp.path().join("daemon_state.json"));
        let status = handle.status_report();
        assert!(!status.runtime.enabled);
    }

    #[tokio::test]
    async fn supervisor_marks_error_and_restart_on_failure() {
        let handle = spawn_component_supervisor(
            "daemon-test-fail",
            1,
            1,
            CancellationToken::new(),
            || async { anyhow::bail!("boom") },
        );

        tokio::time::sleep(Duration::from_millis(50)).await;
        handle.abort();
        let _ = handle.await;

        let snapshot = crate::health::snapshot_json();
        let component = &snapshot["components"]["daemon-test-fail"];
        assert_eq!(component["status"], "error");
        assert!(component["restart_count"].as_u64().unwrap_or(0) >= 1);
        assert!(
            component["last_error"]
                .as_str()
                .unwrap_or("")
                .contains("boom")
        );
    }

    #[tokio::test]
    async fn supervisor_marks_unexpected_exit_as_error() {
        let handle = spawn_component_supervisor(
            "daemon-test-exit",
            1,
            1,
            CancellationToken::new(),
            || async { Ok(()) },
        );

        tokio::time::sleep(Duration::from_millis(50)).await;
        handle.abort();
        let _ = handle.await;

        let snapshot = crate::health::snapshot_json();
        let component = &snapshot["components"]["daemon-test-exit"];
        assert_eq!(component["status"], "error");
        assert!(component["restart_count"].as_u64().unwrap_or(0) >= 1);
        assert!(
            component["last_error"]
                .as_str()
                .unwrap_or("")
                .contains("component exited unexpectedly")
        );
    }

    #[test]
    fn detects_no_supervised_channels() {
        let config = Config::default();
        assert!(!has_supervised_channels(&config));
    }

    #[test]
    fn detects_supervised_channels_present() {
        let mut config = Config::default();
        config.channels_config.telegram = Some(crate::config::TelegramConfig {
            bot_token: "token".into(),
            allowed_users: vec![],
            stream_mode: crate::config::StreamMode::default(),
            draft_update_interval_ms: 1000,
            interrupt_on_new_message: false,
            mention_only: false,
            ack_reactions: None,
            proxy_url: None,
        });
        assert!(has_supervised_channels(&config));
    }

    #[test]
    fn detects_dingtalk_as_supervised_channel() {
        let mut config = Config::default();
        config.channels_config.dingtalk = Some(crate::config::schema::DingTalkConfig {
            client_id: "client_id".into(),
            client_secret: "client_secret".into(),
            allowed_users: vec!["*".into()],
            proxy_url: None,
        });
        assert!(has_supervised_channels(&config));
    }

    #[test]
    fn detects_mattermost_as_supervised_channel() {
        let mut config = Config::default();
        config.channels_config.mattermost = Some(crate::config::schema::MattermostConfig {
            url: "https://mattermost.example.com".into(),
            bot_token: "token".into(),
            channel_id: Some("channel-id".into()),
            allowed_users: vec!["*".into()],
            thread_replies: Some(true),
            mention_only: Some(false),
            interrupt_on_new_message: false,
            proxy_url: None,
        });
        assert!(has_supervised_channels(&config));
    }

    #[test]
    fn detects_qq_as_supervised_channel() {
        let mut config = Config::default();
        config.channels_config.qq = Some(crate::config::schema::QQConfig {
            app_id: "app-id".into(),
            app_secret: "app-secret".into(),
            allowed_users: vec!["*".into()],
            proxy_url: None,
        });
        assert!(has_supervised_channels(&config));
    }

    #[test]
    fn detects_nextcloud_talk_as_supervised_channel() {
        let mut config = Config::default();
        config.channels_config.nextcloud_talk = Some(crate::config::schema::NextcloudTalkConfig {
            base_url: "https://cloud.example.com".into(),
            app_token: "app-token".into(),
            webhook_secret: None,
            allowed_users: vec!["*".into()],
            proxy_url: None,
            bot_name: None,
        });
        assert!(has_supervised_channels(&config));
    }

    #[test]
    fn resolve_delivery_none_when_unset() {
        let config = Config::default();
        let target = resolve_heartbeat_delivery(&config).unwrap();
        assert!(target.is_none());
    }

    #[test]
    fn resolve_delivery_requires_to_field() {
        let mut config = Config::default();
        config.heartbeat.target = Some("telegram".into());
        let err = resolve_heartbeat_delivery(&config).unwrap_err();
        assert!(
            err.to_string()
                .contains("heartbeat.to is required when heartbeat.target is set")
        );
    }

    #[test]
    fn resolve_delivery_requires_target_field() {
        let mut config = Config::default();
        config.heartbeat.to = Some("123456".into());
        let err = resolve_heartbeat_delivery(&config).unwrap_err();
        assert!(
            err.to_string()
                .contains("heartbeat.target is required when heartbeat.to is set")
        );
    }

    #[test]
    fn resolve_delivery_rejects_unsupported_channel() {
        let mut config = Config::default();
        config.heartbeat.target = Some("email".into());
        config.heartbeat.to = Some("ops@example.com".into());
        let err = resolve_heartbeat_delivery(&config).unwrap_err();
        assert!(
            err.to_string()
                .contains("unsupported heartbeat.target channel")
        );
    }

    #[test]
    fn resolve_delivery_requires_channel_configuration() {
        let mut config = Config::default();
        config.heartbeat.target = Some("telegram".into());
        config.heartbeat.to = Some("123456".into());
        let err = resolve_heartbeat_delivery(&config).unwrap_err();
        assert!(
            err.to_string()
                .contains("channels_config.telegram is not configured")
        );
    }

    #[test]
    fn resolve_delivery_accepts_telegram_configuration() {
        let mut config = Config::default();
        config.heartbeat.target = Some("telegram".into());
        config.heartbeat.to = Some("123456".into());
        config.channels_config.telegram = Some(crate::config::TelegramConfig {
            bot_token: "bot-token".into(),
            allowed_users: vec![],
            stream_mode: crate::config::StreamMode::default(),
            draft_update_interval_ms: 1000,
            interrupt_on_new_message: false,
            mention_only: false,
            ack_reactions: None,
            proxy_url: None,
        });

        let target = resolve_heartbeat_delivery(&config).unwrap();
        assert_eq!(target, Some(("telegram".to_string(), "123456".to_string())));
    }

    #[test]
    fn auto_detect_telegram_when_configured() {
        let mut config = Config::default();
        config.channels_config.telegram = Some(crate::config::TelegramConfig {
            bot_token: "bot-token".into(),
            allowed_users: vec!["user123".into()],
            stream_mode: crate::config::StreamMode::default(),
            draft_update_interval_ms: 1000,
            interrupt_on_new_message: false,
            mention_only: false,
            ack_reactions: None,
            proxy_url: None,
        });

        let target = resolve_heartbeat_delivery(&config).unwrap();
        assert_eq!(
            target,
            Some(("telegram".to_string(), "user123".to_string()))
        );
    }

    #[test]
    fn auto_detect_none_when_no_channels() {
        let config = Config::default();
        let target = auto_detect_heartbeat_channel(&config);
        assert!(target.is_none());
    }

    #[test]
    fn compose_heartbeat_prompt_includes_context_book_section() {
        let prompt = compose_heartbeat_task_prompt(
            "[Heartbeat Task | medium] Review launch",
            Some("[Recent conversation history]\nUser: ping"),
            Some("[Memory context]\n- task: launch"),
            Some("[Context Book policy helper]\ncast opportunities:\n- vote-1"),
        );

        assert!(prompt.contains("[Memory context]"));
        assert!(prompt.contains("[Context Book policy helper]"));
        assert!(prompt.contains("[Recent conversation history]"));
        assert!(prompt.contains("[Heartbeat Task | medium] Review launch"));
    }

    #[tokio::test]
    async fn load_context_book_prompt_block_reads_remote_policy_reference() {
        async fn contexts() -> Json<serde_json::Value> {
            Json(json!([
                {
                    "contextId": "peer_ctx_launch",
                    "authorAgentId": "peer-a",
                    "title": "Launch Plan",
                    "contents": "Ship launch plan today",
                    "tag": "eng",
                    "status": "Published",
                    "createdAt": "2026-03-29T00:00:00Z",
                    "updatedAt": "2026-03-29T00:00:01Z"
                }
            ]))
        }

        async fn votes() -> Json<serde_json::Value> {
            Json(json!([
                {
                    "voteId": "peer_vote_launch",
                    "ownerAgentId": "peer-a",
                    "voteScore": 1,
                    "voteContext": "Approve launch plan from peer_ctx_launch",
                    "voterAgentIds": [],
                    "requiredScore": 2,
                    "executable": false,
                    "createdAt": "2026-03-29T00:00:00Z",
                    "updatedAt": "2026-03-29T00:00:02Z"
                }
            ]))
        }

        let app = Router::new()
            .route("/contexts", get(contexts))
            .route("/votes", get(votes));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve axum");
        });

        let tmp = TempDir::new().unwrap();
        let mut config = test_config(&tmp);
        config.context_book.enabled = true;
        config.context_book.discovery_enabled = false;
        config.context_book.manual_url = Some(format!("http://{addr}"));
        config.context_book.allowed_hosts = vec!["127.0.0.1".into()];
        config.context_book.allow_private_hosts = true;
        seed_token_profile(&config, "workspace").await;

        let block = load_context_book_prompt_block(&config, "launch")
            .await
            .expect("context book prompt block");
        assert!(block.contains("[Context Book policy helper]"));
        assert!(block.contains("cast opportunities"));

        server.abort();
        let _ = server.await;
    }

    /// Verify that SIGHUP does not cause shutdown — the daemon should ignore it
    /// and only terminate on SIGINT or SIGTERM.
    #[cfg(unix)]
    #[tokio::test]
    async fn sighup_does_not_shut_down_daemon() {
        use libc;
        use tokio::time::{Duration, timeout};

        let handle = tokio::spawn(wait_for_shutdown_signal());

        // Give the signal handler time to register
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Send SIGHUP to ourselves — should be ignored by the handler
        unsafe { libc::raise(libc::SIGHUP) };

        // The future should NOT complete within a short window
        let result = timeout(Duration::from_millis(200), handle).await;
        assert!(
            result.is_err(),
            "wait_for_shutdown_signal should not return after SIGHUP"
        );
    }
}
