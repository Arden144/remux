use anyhow::Result;
use axum::{Router, ServiceExt, routing::get};
use axum_anyhow::on_error;
pub mod result_ext;
use chrono::Utc;
use config;
use remux_utils::Store;
pub use result_ext::{IntoApiError, OptionExt, ResultExt};
use serde::{Deserialize, Serialize};
use std::{
    self, env,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tower::{Layer, util::MapRequestLayer};
use tower_http::{
    cors::{Any, CorsLayer},
    services::{ServeDir, ServeFile},
};
use tracing::{self, debug, error, info, warn};
use tracing_subscriber::{
    EnvFilter, Layer as TracingLayer, fmt, layer::SubscriberExt,
    util::SubscriberInitExt,
};

mod conversions;
pub mod device_profile;
mod errors;
mod keyed_lock;
pub mod sdks {
    pub use remux_sdks::*;
}
mod addons;
pub mod api;
mod common;
pub mod jellyfin_client;
pub use common::stable_media_uuid;
pub mod db;
#[cfg(feature = "desktop")]
pub mod embedded_static;
pub mod ids;
pub mod intro;
mod iptv;
pub mod localization;
pub mod logging;
pub mod playback;
pub mod playback_session;
pub mod services;
pub mod signals;
pub mod store_keys;
pub mod stream;
mod subtitle_selection;
pub mod tasks;
pub mod tools;
mod torrent;
mod web_client;
mod web_patches;
mod web_transform;
mod ws;

/// Paths to web assets served from the filesystem (non-desktop builds).
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct FilesystemPaths {
    #[serde(default = "default_web_path")]
    pub web_path: String,
    #[serde(default = "default_dashboard_path")]
    pub dashboard_path: String,
    /// The Remux web client, served at `/remux/`. Optional: without it the
    /// server runs Jellyfin Web only.
    #[serde(default = "default_remux_web_path")]
    pub remux_web_path: String,
}

impl Default for FilesystemPaths {
    fn default() -> Self {
        Self {
            web_path: default_web_path(),
            dashboard_path: default_dashboard_path(),
            remux_web_path: default_remux_web_path(),
        }
    }
}

/// Opaque service type for the `/admin` static file handler.
pub type AdminService = tower::util::BoxCloneSyncService<
    axum::extract::Request,
    axum::response::Response,
    std::convert::Infallible,
>;

/// Build an `AdminService` that serves dashboard files from the filesystem.
/// Does not inject user custom JS (admin panel is remux's own UI).
pub fn admin_from_filesystem(dashboard_path: &str) -> AdminService {
    let index = format!("{dashboard_path}/index.html");
    tower::util::BoxCloneSyncService::new(
        web_transform::TransformLayer::new(None)
            .layer(ServeDir::new(dashboard_path).fallback(ServeFile::new(index))),
    )
}

pub use web_client::{RemuxWeb, WebClientService};

/// Route auto-registration via `#[get("/path")]`, `#[post("/path")]`, etc.
pub struct RouteRegistration(pub fn(axum::Router<AppState>) -> axum::Router<AppState>);
inventory::collect!(RouteRegistration);

pub fn collect_routes() -> axum::Router<AppState> {
    let mut router = axum::Router::new();
    for entry in inventory::iter::<RouteRegistration> {
        router = (entry.0)(router);
    }
    router
}

/// [`init_app`] with the web assets read from the filesystem paths in
/// `config.paths`.
async fn init_app_from_filesystem(config: Config) -> Result<(Router, AppContext)> {
    let paths = config
        .paths
        .clone();
    let admin = admin_from_filesystem(&paths.dashboard_path);
    let web_path = paths
        .web_path
        .clone();
    let remux_web = RemuxWeb::from_filesystem(&paths.remux_web_path);
    init_app(config, Some(paths), admin, Some(remux_web), move |pool| {
        WebClientService::from_filesystem(&web_path, pool)
    })
    .await
}

pub async fn init_app_with_config(config: Config) -> Result<Router> {
    Ok(init_app_from_filesystem(config)
        .await?
        .0)
}

pub async fn init_app_with_ctx(config: Config) -> Result<(Router, AppContext)> {
    init_app_from_filesystem(config).await
}

/// Start the HTTP server with web assets served from the filesystem.
/// Binds to `0.0.0.0:{port}` (default 3000, or `PORT` env var).
pub async fn serve(config: Config) -> Result<()> {
    let addr = std::net::SocketAddr::new(config.host, config.port);
    let (router, ctx) = init_app_from_filesystem(config).await?;
    serve_until_shutdown(router, addr, ctx, std::future::pending()).await
}

pub async fn serve_until_shutdown(
    router: Router,
    addr: std::net::SocketAddr,
    ctx: AppContext,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<()> {
    match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => serve_listener(router, listener, &ctx, shutdown).await,
        Err(error) => {
            ctx.close()
                .await;
            Err(error.into())
        }
    }
}

async fn serve_listener(
    router: Router,
    listener: tokio::net::TcpListener,
    ctx: &AppContext,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<()> {
    info!(addr = %listener.local_addr()?, "starting webserver");
    let app = MapRequestLayer::new(rewrite_request_uri).layer(router);
    let handle = axum_server::Handle::new();
    let server = axum_server::from_tcp(listener.into_std()?)?
        .handle(handle.clone())
        .serve(app.into_make_service());
    tokio::pin!(server);
    let result = tokio::select! {
        biased;
        _ = ctx.shutdown_requested.cancelled() => Ok::<(), anyhow::Error>(()),
        _ = shutdown => Ok::<(), anyhow::Error>(()),
        result = shutdown_signal() => result.map_err(anyhow::Error::from),
        result = &mut server => {
            ctx.close().await;
            return result.map_err(anyhow::Error::from);
        }
    };
    info!("shutting down");
    ctx.shutdown_requested
        .cancel();
    let timeout = std::time::Duration::from_secs(
        ctx.config
            .shutdown_timeout_secs,
    );
    let deadline = tokio::time::Instant::now() + timeout;
    handle.graceful_shutdown(Some(timeout));
    let mut server_finished = false;
    let cleanup = async {
        let serve = async {
            let result = (&mut server).await;
            server_finished = true;
            result
        };
        let (server_result, ()) = tokio::join!(serve, ctx.shutdown_until(deadline));
        server_result?;
        while handle.connection_count() != 0 {
            tokio::task::yield_now().await;
        }
        ctx.checkpoint_and_close_database()
            .await;
        Ok::<_, anyhow::Error>(())
    };
    match tokio::time::timeout_at(deadline, cleanup).await {
        Ok(result) => result?,
        Err(_) => {
            handle.shutdown();
            if !server_finished {
                let _ = server.await;
            }
            while handle.connection_count() != 0 {
                tokio::task::yield_now().await;
            }
            ctx.abort_task_shutdown()
                .await;
            anyhow::bail!("server shutdown timed out");
        }
    }
    info!("server shutdown complete");
    result?;
    if ctx
        .restart_requested
        .load(Ordering::Acquire)
    {
        restart_current_process()?;
    }
    Ok(())
}

fn restart_current_process() -> Result<()> {
    let current_exe = std::env::current_exe()?;
    let mut command = std::process::Command::new(&current_exe);
    command.args(std::env::args_os().skip(1));
    info!(executable = ?current_exe, "restarting server");

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        return Err(command
            .exec()
            .into());
    }

    #[cfg(not(unix))]
    {
        common::HideConsole::hide_console(&mut command);
        let child = command.spawn()?;
        info!(pid = child.id(), "replacement server started");
        Ok(())
    }
}

#[cfg(test)]
mod connection_shutdown_tests {
    use super::*;

    #[tokio::test]
    async fn timeout_drops_stalled_request_and_closes_listener() {
        let (_, guard) = integration_test::new_test_server()
            .await
            .unwrap();
        let mut ctx = guard
            .0
            .clone();
        ctx.config
            .shutdown_timeout_secs = 1;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let addr = listener
            .local_addr()
            .unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel::<()>();
        let state = Arc::new(tokio::sync::Mutex::new(Some((entered_tx, dropped_tx))));
        let router = Router::new().route(
            "/stalled",
            get(move || {
                let state = state.clone();
                async move {
                    let (entered, dropped) = state
                        .lock()
                        .await
                        .take()
                        .unwrap();
                    let _dropped = dropped;
                    let _ = entered.send(());
                    std::future::pending::<&'static str>().await
                }
            }),
        );
        let (quit_tx, quit_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            serve_listener(router, listener, &ctx, async {
                let _ = quit_rx.await;
            })
            .await
        });
        let request = tokio::spawn(async move {
            reqwest::get(format!("http://{addr}/stalled")).await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), entered_rx)
            .await
            .unwrap()
            .unwrap();
        quit_tx
            .send(())
            .unwrap();
        let error = tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("shutdown timed out")
        );
        assert!(
            dropped_rx
                .await
                .is_err()
        );
        assert!(
            tokio::net::TcpStream::connect(addr)
                .await
                .is_err()
        );
        assert!(
            request
                .await
                .unwrap()
                .is_err()
        );
    }
}

/// Resolves on Ctrl-C, SIGTERM on Unix, or console close on Windows.
async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    let mut terminate =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    #[cfg(windows)]
    let mut terminate = tokio::signal::windows::ctrl_close()?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

/// Loads `Config` from `env`, layered over the compiled-in defaults. Shared by
/// every binary embedding this crate (the headless server binary, the desktop
/// tray app) so they all resolve settings — `HOST`/`PORT`/etc. — the same way,
/// rather than each reimplementing (or skipping) the config/env merge.
pub fn load_config(
    env: config::Environment,
) -> std::result::Result<Config, config::ConfigError> {
    config::Config::builder()
        .add_source(env.try_parsing(true))
        .build()?
        .try_deserialize()
}

/// [`load_config`] against the process environment — the common case for a
/// binary with no need to inject a custom source (tests aside).
pub fn load_config_from_env() -> std::result::Result<Config, config::ConfigError> {
    load_config(config::Environment::default())
}

#[cfg(unix)]
const TARGET_OPEN_FILE_LIMIT: libc::rlim_t = 8192;

#[cfg(unix)]
fn desired_open_file_limit(
    current_soft: libc::rlim_t,
    current_hard: libc::rlim_t,
) -> Option<libc::rlim_t> {
    let desired = TARGET_OPEN_FILE_LIMIT.min(current_hard);
    (current_soft < desired).then_some(desired)
}

#[cfg(unix)]
fn raise_open_file_limit() {
    // Torrent peers, tracker sockets, HTTP clients, and transcoders all share
    // the process descriptor table. Some desktop launch environments provide a
    // soft limit too small for concurrent playback, so raise only the soft
    // limit while preserving the administrator-controlled hard limit.
    unsafe {
        let mut limits = std::mem::MaybeUninit::<libc::rlimit>::uninit();
        if libc::getrlimit(libc::RLIMIT_NOFILE, limits.as_mut_ptr()) != 0 {
            warn!(
                error = %std::io::Error::last_os_error(),
                "failed to read open-file limit"
            );
            return;
        }

        let mut limits = limits.assume_init();
        let Some(desired) = desired_open_file_limit(limits.rlim_cur, limits.rlim_max)
        else {
            return;
        };
        let previous = limits.rlim_cur;
        limits.rlim_cur = desired;
        if libc::setrlimit(libc::RLIMIT_NOFILE, &limits) != 0 {
            warn!(
                previous,
                desired,
                hard = limits.rlim_max,
                error = %std::io::Error::last_os_error(),
                "failed to raise open-file limit"
            );
        }
    }
}

#[cfg(not(unix))]
fn raise_open_file_limit() {}

pub async fn init_app(
    config: Config,
    web_paths: Option<FilesystemPaths>,
    admin: AdminService,
    remux_web: Option<RemuxWeb>,
    make_web_client: impl FnOnce(sqlx::SqlitePool) -> WebClientService,
) -> Result<(Router, AppContext)> {
    init_app_with_shutdown(
        config,
        web_paths,
        admin,
        remux_web,
        make_web_client,
        tokio_util::sync::CancellationToken::new(),
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("startup cancelled"))
}

/// Initialize the application, cleaning up resources if shutdown is requested.
pub async fn init_app_with_shutdown(
    config: Config,
    web_paths: Option<FilesystemPaths>,
    admin: AdminService,
    remux_web: Option<RemuxWeb>,
    make_web_client: impl FnOnce(sqlx::SqlitePool) -> WebClientService,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<Option<(Router, AppContext)>> {
    if shutdown.is_cancelled() {
        return Ok(None);
    }
    raise_open_file_limit();
    info!("starting remux {}", env!("CARGO_PKG_VERSION"));
    info!("config: {}", serde_json::to_string_pretty(&config).unwrap());

    let cleanup_timeout = std::time::Duration::from_secs(config.shutdown_timeout_secs);
    let close_database = |pool: sqlx::SqlitePool| async move {
        if tokio::time::timeout(cleanup_timeout, pool.close())
            .await
            .is_err()
        {
            warn!("database close timed out during startup cleanup");
        }
    };
    let Some(conn) = shutdown
        .run_until_cancelled(db::connect(
            config
                .database_url
                .as_deref()
                .expect("Config::resolve() must be called before init_app"),
            config.slow_query_threshold_ms,
        ))
        .await
        .transpose()?
    else {
        return Ok(None);
    };

    info!("Running database migrations. Do not interrupt!");
    if let Err(error) = db::migrate(&conn).await {
        close_database(conn.clone()).await;
        return Err(error);
    }
    info!("migrations complete");

    let prepare = async {
        // Checkpoint the WAL before accepting any requests. At this point no
        // other readers exist, so TRUNCATE is guaranteed to succeed and the WAL
        // is cleared to zero — preventing large WALs left over from previous
        // write-heavy tasks (metadata refresh, library scan) from slowing down
        // the first queries after a restart.
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&conn)
            .await
            .ok();
        crate::db::Settings::init_server_id(&conn).await?;

        // Probe hardware and persist results at startup.
        // vaapi_driver is always re-detected (regardless of auto_detect) because
        // it is a runtime property of the host, not a user preference.
        {
            let mut enc_opts = db::Settings::get_encoding_config(&conn).await?;
            if enc_opts
                .auto_detect_hardware_acceleration
                .unwrap_or(true)
            {
                let detected =
                    crate::playback::engine::detect_hardware_acceleration().await;
                enc_opts.hardware_acceleration_type = Some(detected);
            }
            let device = enc_opts
                .vaapi_device
                .as_deref()
                .unwrap_or("/dev/dri/renderD128");
            let driver = crate::playback::engine::detect_vaapi_driver(device).await;
            let opencl =
                crate::playback::engine::detect_opencl_tonemap(device, &driver).await;
            tracing::info!(opencl_tonemap = opencl, "OpenCL tone mapping probed");
            crate::playback::hw_accel::set_opencl_tonemap_available(opencl);
            enc_opts.vaapi_driver = Some(driver);
            db::Settings::set_encoding_config(&conn, &enc_opts).await?;
        }

        let saved_config = db::Settings::get_config(&conn).await?;

        let addons = addons::AddonService::from_db(&conn, &config).await?;
        let torrent_mgr = if saved_config
            .p2p_enabled
            .unwrap_or(true)
        {
            let mgr = torrent::TorrentManager::from_config(&config).await?;
            mgr.update_limits(
                saved_config
                    .p2p_upload_speed_kbps
                    .unwrap_or(0),
                saved_config
                    .p2p_download_speed_kbps
                    .unwrap_or(0),
            );
            Some(mgr)
        } else {
            None
        };

        Ok::<_, anyhow::Error>((saved_config, addons, torrent_mgr))
    };
    let prepared = shutdown
        .run_until_cancelled(prepare)
        .await
        .transpose();
    let (saved_config, addons, torrent_mgr) = match prepared {
        Ok(Some(prepared)) => prepared,
        result => {
            close_database(conn.clone()).await;
            return result.map(|_| None);
        }
    };

    let web_client = make_web_client(conn.clone());
    // The Remux client has its own UI, so no branding scripts are injected.
    let remux_web_installed = remux_web
        .as_ref()
        .is_some_and(|r| r.installed);
    let remux_web = remux_web.map(|r| r.service);

    let transcode_sessions_dir = resolve_transcode_dir(&config.data_dir);
    let (ws_tx, _) = tokio::sync::broadcast::channel::<ws::WsEvent>(128);
    let mut signals = signals::Signals::default();
    signals.register(ws::WebSocketSubscriber {
        ws_tx: ws_tx.clone(),
    });

    let mut ctx = AppContext {
        config,
        db: conn.clone(),
        store: Store::new_weighted(128 * 1024 * 1024),
        sessions: playback_session::PlaybackSessionManager::new(transcode_sessions_dir),
        torrent: Arc::new(tokio::sync::RwLock::new(torrent_mgr.map(Arc::new))),
        ws_tx,
        default_web_client: Arc::new(tokio::sync::RwLock::new(
            web_client::normalize_web_client(saved_config.default_web_client)
                .as_str()
                .to_string(),
        )),
        web_paths,
        remux_web_installed,
        addons,
        signals,
        started_at: Utc::now(),
        shutdown_requested: tokio_util::sync::CancellationToken::new(),
        restart_requested: Arc::new(AtomicBool::new(false)),
        task_shutdown: Arc::new(tokio::sync::Mutex::new(None)),
        task_shutdown_abort: Arc::new(tokio::sync::Mutex::new(None)),
        shutdown_complete: Arc::new(tokio::sync::OnceCell::new()),
    };
    ctx.signals
        .register(services::media_tracker::MediaTrackerSubscriber { ctx: ctx.clone() });
    ctx.signals
        .register(api::webhooks::WebhookSubscriber { ctx: ctx.clone() });
    ctx.signals
        .register(services::four_k_capability::FourKCapabilitySubscriber::new(
            ctx.clone(),
        ));

    let finish = async {
        let prepare_tasks = async {
            // Sync intro items at startup (best-effort; errors are logged not fatal).
            if let Err(e) = intro::sync_intros(&ctx).await {
                warn!(err = ?e, "intro sync failed at startup");
            }

            db::StreamGroup::migrate_from_settings(&conn).await;

            tasks::TaskService::new(ctx.clone()).await
        };
        let Some(task_service) = shutdown
            .run_until_cancelled(prepare_tasks)
            .await
            .transpose()?
        else {
            return Ok(None);
        };
        task_service
            .start()
            .await?;
        let finish = async {
            if !ctx
                .config
                .disable_startup_tasks
            {
                task_service
                    .run_startup_tasks()
                    .await?;
            }

            let state = AppState {
                ctx: ctx.clone(),
                tasks: task_service,
            };

            let cors = CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any)
                .expose_headers(Any);

            let base = Router::new()
                .route("/websocket", get(ws::ws_handler))
                .route("/socket", get(ws::ws_handler))
                .route("/", get(web_client::root_redirect))
                .route("/serviceworker.js", get(web_client::root_serviceworker))
                .merge(collect_routes());

            let mut router = base
                .nest_service("/admin", admin)
                .nest_service("/web", web_client.clone())
                .nest_service("/jellyfin", web_client);
            if let Some(remux_web) = remux_web {
                router = router.nest_service("/remux", remux_web);
            }
            let router = router.with_state(state);

            let router = router
        .layer(on_error(log_api_error))
        .layer(
            tower_http::trace::TraceLayer::new_for_http()
                .make_span_with(|_request: &axum::http::Request<axum::body::Body>| {
                    // let uri = _request.uri();
                    // let path = uri.path();
                    // let uri_str = match uri.query() {
                    //     Some(q) => format!("{path}?{q}"),
                    //     None => path.to_string(),
                    // };
                    tracing::info_span!("request", user = tracing::field::Empty)
                })
                .on_request(|request: &axum::http::Request<axum::body::Body>, _span: &tracing::Span| {
                    let full = logging::loggable_uri(request.uri());
                    debug!(target: "remux_server::request", method = %request.method(), uri = %full, "→");
                })
                .on_response(|response: &axum::http::Response<axum::body::Body>, latency: std::time::Duration, _span: &tracing::Span| {
                    debug!(target: "remux_server::request", status = %response.status().as_u16(), latency_ms = %latency.as_millis(), "←");
                })
                .on_failure(()),
        )
        .layer(cors);

            Ok::<_, anyhow::Error>(router)
        };
        shutdown
            .run_until_cancelled(finish)
            .await
            .transpose()
    };
    match finish.await {
        Ok(Some(router)) => {
            ctx.sessions
                .clone()
                .spawn_cleanup_task(
                    std::time::Duration::from_secs(60),
                    std::time::Duration::from_secs(60 * 15),
                );
            Ok(Some((router, ctx)))
        }
        result => {
            ctx.shutdown()
                .await;
            close_database(conn.clone()).await;
            result.map(|_| None)
        }
    }
}

#[cfg(test)]
mod startup_tests {
    use super::*;

    #[tokio::test]
    async fn cancelled_startup_skips_database_and_services() {
        let shutdown = tokio_util::sync::CancellationToken::new();
        shutdown.cancel();
        let config = Config {
            database_url: Some("not-a-database-url".into()),
            ..Config::default()
        };
        let result = init_app_with_shutdown(
            config,
            None,
            admin_from_filesystem(""),
            None,
            |_| panic!("web client must not be initialized"),
            shutdown,
        )
        .await
        .unwrap();
        assert!(result.is_none());
    }
}

#[derive(Clone)]
pub struct AppContext {
    pub config: Config,
    pub db: sqlx::SqlitePool,
    pub store: Store,
    pub sessions: playback_session::PlaybackSessionManager,
    pub torrent: Arc<tokio::sync::RwLock<Option<Arc<torrent::TorrentManager>>>>,
    pub ws_tx: tokio::sync::broadcast::Sender<ws::WsEvent>,
    pub default_web_client: Arc<tokio::sync::RwLock<String>>,
    /// Present in filesystem builds; `None` in desktop (assets are embedded).
    pub web_paths: Option<FilesystemPaths>,
    /// Whether Remux Web is served at `/remux` with an `index.html` to open.
    pub remux_web_installed: bool,
    pub addons: addons::AddonService,
    pub signals: signals::Signals,
    /// When this server process started.
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub shutdown_requested: tokio_util::sync::CancellationToken,
    restart_requested: Arc<AtomicBool>,
    pub(crate) task_shutdown:
        Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    pub(crate) task_shutdown_abort:
        Arc<tokio::sync::Mutex<Option<tokio::task::AbortHandle>>>,
    shutdown_complete: Arc<tokio::sync::OnceCell<()>>,
}

impl AppContext {
    /// Stop every playback session (exiting runs no destructors, so ffmpeg
    /// would outlive remux) and refuse new transcodes, then shut down
    /// background services (torrent DHT, etc.) to release sockets immediately.
    /// Call this when the server is stopping.
    async fn close(&self) {
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_secs(
                self.config
                    .shutdown_timeout_secs,
            );
        let cleanup = async {
            self.shutdown_until(deadline)
                .await;
            self.checkpoint_and_close_database()
                .await;
        };
        if tokio::time::timeout_at(deadline, cleanup)
            .await
            .is_err()
        {
            self.abort_task_shutdown()
                .await;
            error!("server cleanup timed out");
        }
    }

    pub fn request_restart(&self) {
        self.restart_requested
            .store(true, Ordering::Release);
        self.shutdown_requested
            .cancel();
    }

    async fn checkpoint_and_close_database(&self) {
        if let Err(error) = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&self.db)
            .await
        {
            warn!(%error, "final WAL checkpoint failed");
        }
        self.db
            .close()
            .await;
    }

    pub async fn shutdown(&self) {
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_secs(
                self.config
                    .shutdown_timeout_secs,
            );
        self.shutdown_until(deadline)
            .await;
    }

    async fn shutdown_until(&self, deadline: tokio::time::Instant) {
        self.shutdown_requested
            .cancel();
        if tokio::time::timeout_at(
            deadline,
            self.shutdown_complete
                .get_or_init(|| self.shutdown_services()),
        )
        .await
        .is_err()
        {
            error!("service shutdown timed out");
            self.abort_task_shutdown()
                .await;
        }
    }

    async fn abort_task_shutdown(&self) {
        if let Some(abort) = self
            .task_shutdown_abort
            .lock()
            .await
            .as_ref()
        {
            abort.abort();
        }
        let mut task = self
            .task_shutdown
            .lock()
            .await;
        if let Some(handle) = task.as_mut() {
            handle.abort();
            let _ = handle.await;
        }
        task.take();
    }

    async fn shutdown_services(&self) {
        let tasks = async {
            let mut task_shutdown = self
                .task_shutdown
                .lock()
                .await;
            if let Some(handle) = task_shutdown.as_mut() {
                if let Err(error) = handle.await {
                    error!(%error, "task shutdown failed");
                }
            }
            task_shutdown.take();
        };
        let torrent = async {
            let old = self
                .torrent
                .write()
                .await
                .take();
            if let Some(mgr) = old {
                mgr.shutdown()
                    .await;
            }
        };
        tokio::join!(
            self.sessions
                .stop_all(),
            tasks,
            torrent
        );
    }

    pub async fn set_p2p_enabled(&self, enabled: bool) -> anyhow::Result<()> {
        let mut current = self
            .torrent
            .write()
            .await;
        anyhow::ensure!(
            !self
                .shutdown_requested
                .is_cancelled(),
            "server is shutting down"
        );
        match (enabled, current.is_some()) {
            (true, false) => {
                let mgr = tokio::select! {
                    biased;
                    _ = self.shutdown_requested.cancelled() => anyhow::bail!("server is shutting down"),
                    result = torrent::TorrentManager::from_config(&self.config) => Arc::new(result?),
                };
                if self
                    .shutdown_requested
                    .is_cancelled()
                {
                    drop(mgr);
                    anyhow::bail!("server is shutting down");
                }
                *current = Some(mgr);
            }
            (false, true) => {
                if let Some(mgr) = current.take() {
                    mgr.shutdown()
                        .await;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct AppState {
    pub ctx: AppContext,
    pub tasks: tasks::TaskService,
}

fn default_data_dir() -> std::path::PathBuf {
    dirs::data_dir()
        .map(|d| d.join("remux"))
        .unwrap_or_else(|| std::path::PathBuf::from("/data"))
}

fn default_web_path() -> String {
    default_data_dir()
        .join("jellyfin-web")
        .to_str()
        .map(str::to_owned)
        .unwrap_or_else(|| "/data/jellyfin-web".to_string())
}

fn default_remux_web_path() -> String {
    default_data_dir()
        .join("remux-web")
        .to_str()
        .map(str::to_owned)
        .unwrap_or_else(|| "/data/remux-web".to_string())
}

fn default_dashboard_path() -> String {
    default_data_dir()
        .join("dashboard")
        .to_str()
        .map(str::to_owned)
        .unwrap_or_else(|| "/data/dashboard".to_string())
}

fn default_host() -> std::net::IpAddr {
    std::net::Ipv4Addr::UNSPECIFIED.into()
}

fn default_shutdown_timeout_secs() -> u64 {
    30
}

fn default_port() -> u16 {
    3000
}

fn default_torrent_http_port() -> u16 {
    9876
}

/// Serializes a set secret as `"<redacted>"`, so the logged config doesn't
/// carry it.
fn serialize_redacted<S: serde::Serializer>(
    value: &Option<String>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    match value {
        Some(_) => serializer.serialize_some("<redacted>"),
        None => serializer.serialize_none(),
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Config {
    #[serde(default = "default_data_dir")]
    pub data_dir: std::path::PathBuf,
    /// `None` means derive from `data_dir` — call `resolve()` after loading.
    pub database_url: Option<String>,
    /// `None` means derive from `data_dir` — call `resolve()` after loading.
    pub torrent_data_dir: Option<String>,
    /// Address the HTTP server listens on. Defaults to every IPv4 interface;
    /// `127.0.0.1` keeps it behind a reverse proxy on the same host, `::`
    /// listens on IPv6 as well.
    #[serde(default = "default_host")]
    pub host: std::net::IpAddr,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_shutdown_timeout_secs")]
    pub shutdown_timeout_secs: u64,
    /// Explicit port for the internal torrent HTTP server.
    /// When absent the OS picks a free ephemeral port.
    #[serde(default = "default_torrent_http_port_opt")]
    pub torrent_http_port: Option<u16>,
    /// Log queries that exceed this threshold in milliseconds. Defaults to 10 000 ms.
    #[serde(default = "default_slow_query_threshold_ms")]
    pub slow_query_threshold_ms: u64,
    /// Disable the DHT gossip socket. Useful when no Torznab sources are
    /// configured or when running in a restricted network environment.
    #[serde(default)]
    pub disable_dht: bool,
    /// Skip running `StartupTrigger`-tagged tasks (e.g. the default library
    /// refresh) during `init_app`. Only meant for test/benchmark harnesses
    /// that bulk-write their own fixture data immediately after startup —
    /// letting the real startup tasks run concurrently races the same
    /// tables and can corrupt fixture writes.
    #[serde(default)]
    pub disable_startup_tasks: bool,
    /// TCP port range for librqbit peer connections.  Announced to trackers so
    /// they return us in peer lists.  Defaults to 6881.  Does not need to be
    /// forwarded/open for outbound-only operation, but must be a real port
    /// (not 0) or many trackers will reject the announce.
    #[serde(default = "default_torrent_peer_port")]
    pub torrent_peer_port: Option<u16>,
    /// Path to the bgutil-pot binary used by yt-dlp for YouTube POT token generation.
    #[serde(default = "default_bgutil_script_path")]
    pub bgutil_script_path: std::path::PathBuf,
    /// Extra arguments passed to every yt-dlp call (`YTDLP_EXTRA_ARGS`), split
    /// on whitespace. Can carry credentials (`--password`, proxy URLs), so
    /// the config log shows only whether it is set.
    #[serde(default, serialize_with = "serialize_redacted")]
    pub ytdlp_extra_args: Option<String>,
    /// Web asset locations (`WEB_PATH`, `DASHBOARD_PATH`, `REMUX_WEB_PATH`).
    #[serde(flatten, default)]
    pub paths: FilesystemPaths,
    /// Base URL for the TMDB API. Overridable for testing.
    #[serde(default = "default_tmdb_base_url")]
    pub tmdb_base_url: String,
    /// Base URL for Cinemeta, the source of TVDB episode numbering for
    /// stream ids. Overridable for testing.
    #[serde(default = "default_cinemeta_base_url")]
    pub cinemeta_base_url: String,
    /// Base URL for remuxdb. When set, probe results are submitted after each live probe.
    #[serde(default = "default_remuxdb_url")]
    pub remuxdb_url: Option<String>,
    #[serde(default = "default_activity_log_retention_days")]
    pub activity_log_retention_days: u32,
    #[serde(default = "default_jellyfin_version")]
    pub jellyfin_version: String,
    /// Log file output is best-effort; stdout logging remains enabled when false.
    #[serde(default = "default_log_file_enabled")]
    pub log_file_enabled: bool,
    /// Optional log directory; defaults to `<data_dir>/logs` after `resolve()`.
    #[serde(default)]
    pub log_dir: Option<std::path::PathBuf>,
    /// Maximum number of rolling log files retained. Zero disables the count cap.
    #[serde(default = "default_log_file_max_files")]
    pub log_file_max_files: usize,
    /// Age-based log retention in days; zero disables age-based cleanup.
    #[serde(default = "default_log_file_retention_days")]
    pub log_file_retention_days: u32,
    /// OTLP gRPC endpoint (e.g. `http://jaeger-collector:4317`) to export
    /// tracing spans to. `None` (the default) disables tracing export
    /// entirely — normal log output is unaffected either way.
    pub otlp_endpoint: Option<String>,
}

fn default_jellyfin_version() -> String {
    "12.4.0".to_string()
}

fn default_remuxdb_url() -> Option<String> {
    Some("https://remuxdb.1632022.xyz".to_string())
}

fn default_activity_log_retention_days() -> u32 {
    90
}

fn default_log_file_enabled() -> bool {
    true
}

fn default_log_file_max_files() -> usize {
    5
}

fn default_log_file_retention_days() -> u32 {
    5
}

fn default_tmdb_base_url() -> String {
    "https://api.themoviedb.org/3/".to_string()
}

fn default_cinemeta_base_url() -> String {
    sdks::cinemeta::DEFAULT_BASE_URL.to_string()
}

fn default_bgutil_script_path() -> std::path::PathBuf {
    std::path::PathBuf::from("/usr/local/bin/bgutil-pot")
}

fn default_slow_query_threshold_ms() -> u64 {
    10_000
}

fn ensure_writable_directory(path: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(path)?;
    let _probe = tempfile::NamedTempFile::new_in(path)?;
    Ok(())
}

fn select_transcode_dir(
    primary: std::path::PathBuf,
    fallback: std::path::PathBuf,
) -> std::path::PathBuf {
    let primary_error = match ensure_writable_directory(&primary) {
        Ok(()) => return primary,
        Err(error) => error,
    };

    match ensure_writable_directory(&fallback) {
        Ok(()) => {
            warn!(
                primary = %primary.display(),
                fallback = %fallback.display(),
                error = %primary_error,
                "transcode directory is not writable; using temporary storage"
            );
            fallback
        }
        Err(fallback_error) => {
            warn!(
                primary = %primary.display(),
                fallback = %fallback.display(),
                primary_error = %primary_error,
                fallback_error = %fallback_error,
                "transcode and fallback directories are not writable"
            );
            primary
        }
    }
}

fn resolve_transcode_dir(data_dir: &std::path::Path) -> std::path::PathBuf {
    select_transcode_dir(
        data_dir.join("transcode_sessions"),
        std::env::temp_dir().join("remux-transcode"),
    )
}

fn default_torrent_http_port_opt() -> Option<u16> {
    Some(default_torrent_http_port())
}

fn default_torrent_peer_port() -> Option<u16> {
    Some(6881)
}

impl Config {
    /// Fill in `None` fields that derive from `data_dir`. Call once after loading.
    pub fn resolve(mut self) -> Self {
        if self
            .database_url
            .is_none()
        {
            self.database_url = Some(format!(
                "sqlite://{}?mode=rwc",
                self.data_dir
                    .join("db.sqlite")
                    .display()
            ));
        }
        if self
            .torrent_data_dir
            .is_none()
        {
            self.torrent_data_dir = Some(
                self.data_dir
                    .join("torrents")
                    .to_string_lossy()
                    .into_owned(),
            );
        }
        if self
            .log_dir
            .is_none()
        {
            self.log_dir = Some(
                self.data_dir
                    .join("logs"),
            );
        }
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            ytdlp_extra_args: None,
            paths: FilesystemPaths::default(),
            data_dir: default_data_dir(),
            database_url: None,
            torrent_data_dir: None,
            host: default_host(),
            port: default_port(),
            shutdown_timeout_secs: default_shutdown_timeout_secs(),
            torrent_http_port: default_torrent_http_port_opt(),
            slow_query_threshold_ms: default_slow_query_threshold_ms(),
            disable_dht: false,
            disable_startup_tasks: false,
            torrent_peer_port: default_torrent_peer_port(),
            bgutil_script_path: default_bgutil_script_path(),
            tmdb_base_url: default_tmdb_base_url(),
            cinemeta_base_url: default_cinemeta_base_url(),
            remuxdb_url: Some("https://remuxdb.1632022.xyz".to_string()),
            activity_log_retention_days: default_activity_log_retention_days(),
            jellyfin_version: default_jellyfin_version(),
            log_file_enabled: default_log_file_enabled(),
            log_dir: None,
            log_file_max_files: default_log_file_max_files(),
            log_file_retention_days: default_log_file_retention_days(),
            otlp_endpoint: None,
        }
        .resolve()
    }
}

pub fn rewrite_request_uri<B>(mut req: http::Request<B>) -> http::Request<B> {
    let uri = req.uri();
    let path = uri
        .path()
        .replace("/emby", "");

    // Trim trailing slashes, but keep "/" for root, and preserve the trailing slash
    // on SPA mount prefixes (/web/, /admin/, /jellyfin/, /remux/) so WebClientService and
    // ServeDir can properly resolve relative asset URLs without infinite redirect loops.
    let trimmed = path.trim_end_matches('/');
    let path = if trimmed.is_empty() {
        "/".to_string()
    } else if trimmed.eq_ignore_ascii_case("/web")
        || trimmed.eq_ignore_ascii_case("/admin")
        || trimmed.eq_ignore_ascii_case("/jellyfin")
        || trimmed.eq_ignore_ascii_case("/remux")
    {
        if path.ends_with('/') {
            format!("{trimmed}/")
        } else {
            trimmed.to_string()
        }
    } else {
        trimmed.to_string()
    };

    // Keep file paths case-sensitive (Linux filesystems are case-sensitive).
    // Only normalize API-style routes that don't look like files, plus known
    // API file endpoints (for example /Videos/.../Stream.vtt).
    let last_segment = path
        .rsplit('/')
        .next()
        .unwrap_or_default();
    let is_file_like = last_segment.contains('.');
    let lower_path = path.to_ascii_lowercase();
    let api_file_like = is_file_like
        && (lower_path.starts_with("/videos/")
            || lower_path.starts_with("/audio/")
            || lower_path.starts_with("/items/")
            || lower_path.starts_with("/mediasegments/")
            || lower_path.starts_with("/sessions/"));

    let smart_lower_path: String = path
        .split('/')
        .map(|seg| {
            if seg
                .chars()
                .all(|c| c.is_ascii_alphanumeric())
            {
                seg.to_ascii_lowercase()
            } else {
                seg.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("/");

    // Remux Web has path routes whose segments are case-sensitive (names,
    // ids), unlike the API keywords normalised below. Leave its paths alone.
    let is_remux_web = path.eq_ignore_ascii_case("/remux")
        || path
            .to_ascii_lowercase()
            .starts_with("/remux/");

    let new_path = if path == "/" || is_remux_web {
        path
    } else if is_file_like && !api_file_like {
        // Static file — preserve original casing.
        path
    } else if api_file_like {
        // Known API file endpoint (HLS, VTT …) — full lowercase so the route
        // and file-extension both match our lowercase handler definitions.
        lower_path
    } else {
        // Regular API route — lowercase route keywords, preserve param values.
        smart_lower_path
    };

    let query = uri
        .query()
        .map(|q| format!("?{}", q))
        .unwrap_or_default();

    let new_uri = http::Uri::builder()
        .path_and_query(format!("{}{}", new_path, query))
        .build()
        .unwrap_or_else(|_| uri.clone());

    *req.uri_mut() = new_uri;
    req
}

/// Builds the OTLP (gRPC) tracing layer and registers its tracer provider
/// globally so it stays alive for the process lifetime — dropping it would
/// silently stop span export. `endpoint` is e.g. `http://jaeger:4317`.
fn build_otel_layer<S>(
    endpoint: &str,
) -> Option<impl tracing_subscriber::Layer<S> + Send + Sync + 'static>
where
    S: tracing::Subscriber
        + Send
        + Sync
        + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_otlp::WithExportConfig;

    let exporter = match opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
    {
        Ok(exporter) => exporter,
        Err(e) => {
            eprintln!("failed to build OTLP exporter for {endpoint}: {e}");
            return None;
        }
    };

    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name("remux-server")
                .build(),
        )
        .build();

    let tracer = provider.tracer("remux-server");
    opentelemetry::global::set_tracer_provider(provider);
    Some(tracing_opentelemetry::layer().with_tracer(tracer))
}

/// `otlp_endpoint`: when set (e.g. `http://jaeger-collector:4317`), tracing spans are
/// also exported via OTLP — see `Config::otlp_endpoint`. Gated by the same
/// `RUST_LOG`/`EnvFilter` as normal log output, so raising the level to see
/// more logs also sends more spans. File logging is best-effort: an unavailable
/// directory or rolling appender leaves stdout logging active.
pub fn setup_logging(
    log_dir: Option<&std::path::Path>,
    max_log_files: usize,
    otlp_endpoint: Option<&str>,
) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("warn,remux=info"));

    let fmt_layer = fmt::layer()
        .with_timer(fmt::time::ChronoLocal::new("%H:%M:%S".to_string()))
        .with_target(true)
        .with_line_number(true)
        .with_file(false)
        .compact();

    let file_layer = log_dir.and_then(|dir| {
        let appender = tracing_appender::rolling::Builder::new()
            .rotation(tracing_appender::rolling::Rotation::DAILY)
            .filename_prefix("remux")
            .filename_suffix("log")
            .max_log_files(max_log_files)
            .build(dir);
        match appender {
            Ok(appender) => Some(
                fmt::layer()
                    .with_ansi(false)
                    // Daily rolling filenames are UTC; explicit UTC timestamps keep
                    // entries immediately after midnight on the matching date.
                    .with_timer(fmt::time::ChronoUtc::new("%+".to_string()))
                    .with_target(true)
                    .with_line_number(true)
                    .with_file(false)
                    .compact()
                    .with_writer(appender),
            ),
            Err(error) => {
                eprintln!("Remux file logging unavailable; continuing with stdout only: {error}");
                None
            }
        }
    });

    // FmtSpan::CLOSE makes every span passing through a layer log its own
    // duration on exit. Scoped to the `remux_server::metadata_refresh`
    // target only (every refresh-pipeline span sets this explicitly) via a
    // dedicated layer + per-layer filter, rather than applied to fmt_layer/
    // file_layer above — those cover the whole app, and every other
    // `#[instrument]`'d function (playback, subtitles, etc.) would otherwise
    // start emitting close events too, at whatever level it happens to be
    // instrumented at (often its default of INFO).
    let refresh_span_layer = fmt::layer()
        .with_timer(fmt::time::ChronoLocal::new("%H:%M:%S".to_string()))
        .with_target(true)
        .with_line_number(true)
        .with_file(false)
        .with_span_events(fmt::format::FmtSpan::CLOSE)
        .compact()
        .with_filter(
            tracing_subscriber::filter::Targets::new()
                .with_target("remux_server::metadata_refresh", tracing::Level::TRACE),
        );

    let otel_layer = otlp_endpoint.and_then(build_otel_layer);

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(file_layer)
        .with(refresh_span_layer)
        .with(otel_layer)
        .try_init()
        .ok(); // try_init + ok() so tests don't panic on repeated calls
}

fn log_api_error(err: &axum_anyhow::ApiError) {
    let status = err.status();
    let is_server_error = status.is_server_error();
    if let Some(cause) = err.error() {
        if is_server_error {
            error!(
                status = %status,
                title = %err.title(),
                detail = %err.detail(),
                cause = %format!("{:#}", cause),
                "api error"
            );
        } else {
            debug!(
                status = %status,
                title = %err.title(),
                detail = %err.detail(),
                cause = %format!("{:#}", cause),
                "api error"
            );
        }
    } else if is_server_error {
        error!(
            status = %status,
            title = %err.title(),
            detail = %err.detail(),
            "api error"
        );
    } else {
        debug!(
            status = %status,
            title = %err.title(),
            detail = %err.detail(),
            "api error"
        );
    }
}

#[cfg(test)]
pub mod integration_test;

#[cfg(test)]
mod rewrite_uri_tests {
    use super::rewrite_request_uri;

    fn rewrite(path: &str) -> String {
        let req = http::Request::builder()
            .method("GET")
            .uri(path)
            .body(())
            .unwrap();
        rewrite_request_uri(req)
            .uri()
            .path()
            .to_string()
    }

    /// `/remux/` lost its trailing slash here, so the web client service
    /// redirected it back to `/remux/` and the browser looped.
    #[test]
    fn keeps_the_remux_web_mount_and_its_paths_untouched() {
        assert_eq!(rewrite("/remux/"), "/remux/");
        assert_eq!(rewrite("/remux"), "/remux");
        assert_eq!(rewrite("/remux/assets/App.js"), "/remux/assets/App.js");
        assert_eq!(
            rewrite("/remux/library/Some-Name"),
            "/remux/library/Some-Name"
        );
        assert_eq!(
            rewrite("/remux/library/Some-Name/"),
            "/remux/library/Some-Name"
        );
    }

    #[test]
    fn lowercases_alpha_keyword_segments() {
        assert_eq!(rewrite("/Items/Sessions"), "/items/sessions");
        assert_eq!(rewrite("/Genres"), "/genres");
    }

    #[test]
    fn lowercases_keyword_with_digits() {
        assert_eq!(rewrite("/Items/Filters2"), "/items/filters2");
        assert_eq!(rewrite("/Hls1"), "/hls1");
    }

    #[test]
    fn preserves_uuid_param_values() {
        assert_eq!(
            rewrite("/Items/f27caa37-e514-2225-cced-ed48f6553502"),
            "/items/f27caa37-e514-2225-cced-ed48f6553502"
        );
        assert_eq!(
            rewrite("/Items/F27CAA37-E514-2225-CCED-ED48F6553502"),
            "/items/F27CAA37-E514-2225-CCED-ED48F6553502"
        );
    }

    #[test]
    fn leaves_special_char_device_ids_alone() {
        let path = "/Sessions/Play/YWJjMTIz%7Cabc";
        let rewritten = rewrite(path);
        assert!(rewritten.starts_with("/sessions/play/"));
        assert!(rewritten.contains("YWJjMTIz%7Cabc"));
    }

    #[test]
    fn strips_trailing_slashes_from_api_routes() {
        assert_eq!(rewrite("/Items/"), "/items");
        assert_eq!(rewrite("/items/"), "/items");
        assert_eq!(rewrite("/Search/Hints/"), "/search/hints");
        assert_eq!(rewrite("/Genres/"), "/genres");
        assert_eq!(rewrite("/emby/Items/"), "/items");
        assert_eq!(
            rewrite("/Items/f27caa37-e514-2225-cced-ed48f6553502/"),
            "/items/f27caa37-e514-2225-cced-ed48f6553502"
        );
        assert_eq!(
            rewrite("/Users/f27caa37-e514-2225-cced-ed48f6553502/Items/"),
            "/users/f27caa37-e514-2225-cced-ed48f6553502/items"
        );
    }

    #[test]
    fn strips_multiple_trailing_slashes() {
        assert_eq!(rewrite("/Items///"), "/items");
        assert_eq!(rewrite("/Genres//"), "/genres");
    }

    #[test]
    fn preserves_root_and_emby_root() {
        assert_eq!(rewrite("/"), "/");
        assert_eq!(rewrite("///"), "/");
        assert_eq!(rewrite("/emby"), "/");
        assert_eq!(rewrite("/emby/"), "/");
    }

    #[test]
    fn preserves_spa_mount_trailing_slashes() {
        assert_eq!(rewrite("/web"), "/web");
        assert_eq!(rewrite("/web/"), "/web/");
        assert_eq!(rewrite("/admin"), "/admin");
        assert_eq!(rewrite("/admin/"), "/admin/");
        assert_eq!(rewrite("/jellyfin"), "/jellyfin");
        assert_eq!(rewrite("/jellyfin/"), "/jellyfin/");
    }

    #[test]
    fn preserves_query_string_with_trailing_slash() {
        let req = http::Request::builder()
            .method("GET")
            .uri("/Items/?Limit=50&ParentId=123")
            .body(())
            .unwrap();
        let rewritten = rewrite_request_uri(req);
        assert_eq!(
            rewritten
                .uri()
                .path(),
            "/items"
        );
        assert_eq!(
            rewritten
                .uri()
                .query(),
            Some("Limit=50&ParentId=123")
        );
    }

    #[test]
    fn normalizes_file_endpoints_with_trailing_slash() {
        assert_eq!(rewrite("/Videos/123/Stream.vtt/"), "/videos/123/stream.vtt");
    }
}

#[cfg(test)]
mod transcode_dir_tests {
    use super::select_transcode_dir;

    #[test]
    fn uses_writable_primary_directory() {
        let temp = tempfile::tempdir().unwrap();
        let primary = temp
            .path()
            .join("primary");
        let fallback = temp
            .path()
            .join("fallback");

        assert_eq!(select_transcode_dir(primary.clone(), fallback), primary);
    }

    #[test]
    fn falls_back_when_primary_directory_cannot_be_created() {
        let temp = tempfile::tempdir().unwrap();
        let blocking_file = temp
            .path()
            .join("not-a-directory");
        std::fs::write(&blocking_file, b"blocked").unwrap();
        let primary = blocking_file.join("transcode_sessions");
        let fallback = temp
            .path()
            .join("fallback");

        assert_eq!(select_transcode_dir(primary, fallback.clone()), fallback);
    }
}

#[cfg(all(test, unix))]
mod open_file_limit_tests {
    use super::{TARGET_OPEN_FILE_LIMIT, desired_open_file_limit};

    #[test]
    fn raises_soft_limit_to_target_without_changing_hard_limit() {
        assert_eq!(
            desired_open_file_limit(256, libc::RLIM_INFINITY),
            Some(TARGET_OPEN_FILE_LIMIT)
        );
        assert_eq!(
            desired_open_file_limit(TARGET_OPEN_FILE_LIMIT, libc::RLIM_INFINITY),
            None
        );
    }

    #[test]
    fn clamps_soft_limit_to_the_existing_hard_limit() {
        assert_eq!(desired_open_file_limit(256, 4096), Some(4096));
        assert_eq!(desired_open_file_limit(4096, 4096), None);
    }
}

#[cfg(test)]
mod config_log_tests {
    use super::Config;

    #[test]
    fn the_logged_config_hides_ytdlp_extra_args() {
        let config = Config {
            ytdlp_extra_args: Some("--password hunter2".into()),
            ..Default::default()
        };
        let logged = serde_json::to_string(&config).unwrap();
        assert!(!logged.contains("hunter2"), "{logged}");
        assert!(
            logged.contains(r#""ytdlp_extra_args":"<redacted>""#),
            "{logged}"
        );
    }
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;
    use std::time::Duration;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn shutdown_drains_in_flight_requests() {
        let (_app, guard) = integration_test::new_test_server()
            .await
            .unwrap();
        let ctx = guard
            .0
            .clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let addr = listener
            .local_addr()
            .unwrap();
        let (entered_tx, entered_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let channels = Arc::new(tokio::sync::Mutex::new(Some((entered_tx, finish_rx))));
        let router = Router::new().route(
            "/slow",
            get(move || {
                let channels = channels.clone();
                async move {
                    let (entered_tx, finish_rx) = channels
                        .lock()
                        .await
                        .take()
                        .unwrap();
                    let _ = entered_tx.send(());
                    let _ = finish_rx.await;
                    "finished"
                }
            }),
        );
        let (quit_tx, quit_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            serve_listener(router, listener, &ctx, async {
                let _ = quit_rx.await;
            })
            .await
        });
        let request = tokio::spawn(async move {
            reqwest::get(format!("http://{addr}/slow"))
                .await
                .unwrap()
                .text()
                .await
                .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(5), entered_rx)
            .await
            .unwrap()
            .unwrap();
        quit_tx
            .send(())
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            guard
                .0
                .shutdown_requested
                .cancelled(),
        )
        .await
        .unwrap();
        assert!(!server.is_finished());
        finish_tx
            .send(())
            .unwrap();
        assert_eq!(
            request
                .await
                .unwrap(),
            "finished"
        );
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn shutdown_refuses_p2p_changes() {
        let (_app, guard) = integration_test::new_test_server()
            .await
            .unwrap();
        guard
            .0
            .shutdown()
            .await;
        assert!(
            guard
                .0
                .set_p2p_enabled(true)
                .await
                .is_err()
        );
        assert!(
            guard
                .0
                .torrent
                .read()
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn shutdown_bounds_blocked_task_cleanup() {
        let (_app, mut guard) = integration_test::new_test_server()
            .await
            .unwrap();
        guard
            .0
            .config
            .shutdown_timeout_secs = 1;
        let watcher = guard
            .0
            .task_shutdown
            .lock()
            .await
            .take()
            .unwrap();
        watcher.abort();
        let blocked = tokio::spawn(std::future::pending::<()>());
        let abort = blocked.abort_handle();
        *guard
            .0
            .task_shutdown_abort
            .lock()
            .await = Some(abort.clone());
        *guard
            .0
            .task_shutdown
            .lock()
            .await = Some(blocked);
        tokio::time::timeout(
            Duration::from_secs(3),
            guard
                .0
                .shutdown(),
        )
        .await
        .unwrap();
        assert!(
            guard
                .0
                .shutdown_complete
                .get()
                .is_none()
        );
        tokio::time::timeout(
            Duration::from_secs(3),
            guard
                .0
                .shutdown(),
        )
        .await
        .unwrap();
        assert!(
            guard
                .0
                .shutdown_complete
                .get()
                .is_some()
        );
        assert!(abort.is_finished());
        assert!(
            guard
                .0
                .torrent
                .read()
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn shutdown_without_connections_bounds_blocked_task_cleanup() {
        let (_app, mut guard) = integration_test::new_test_server()
            .await
            .unwrap();
        guard
            .0
            .config
            .shutdown_timeout_secs = 1;
        let ctx = &guard.0;
        let watcher = ctx
            .task_shutdown
            .lock()
            .await
            .take()
            .unwrap();
        watcher.abort();
        let _ = watcher.await;
        let blocked = tokio::spawn(std::future::pending::<()>());
        let abort = blocked.abort_handle();
        *ctx.task_shutdown_abort
            .lock()
            .await = Some(abort.clone());
        *ctx.task_shutdown
            .lock()
            .await = Some(blocked);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();

        let error = tokio::time::timeout(
            Duration::from_secs(3),
            serve_listener(Router::new(), listener, ctx, async {}),
        )
        .await
        .unwrap()
        .unwrap_err();

        assert_eq!(error.to_string(), "server shutdown timed out");
        assert!(abort.is_finished());
        assert!(
            ctx.task_shutdown
                .lock()
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn concurrent_shutdown_aborts_task_at_earlier_deadline() {
        let (_app, guard) = integration_test::new_test_server()
            .await
            .unwrap();
        let ctx = &guard.0;
        let watcher = ctx
            .task_shutdown
            .lock()
            .await
            .take()
            .unwrap();
        watcher.abort();
        let _ = watcher.await;

        let blocked = tokio::spawn(std::future::pending::<()>());
        let abort = blocked.abort_handle();
        *ctx.task_shutdown_abort
            .lock()
            .await = Some(abort.clone());
        *ctx.task_shutdown
            .lock()
            .await = Some(blocked);

        let first_ctx = ctx.clone();
        let first = tokio::spawn(async move {
            first_ctx
                .shutdown_until(tokio::time::Instant::now() + Duration::from_secs(60))
                .await;
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while ctx
                .task_shutdown
                .try_lock()
                .is_ok()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        tokio::time::timeout(Duration::from_secs(3), async {
            ctx.shutdown_until(tokio::time::Instant::now() + Duration::from_millis(50))
                .await;
            first
                .await
                .unwrap();
        })
        .await
        .unwrap();
        assert!(abort.is_finished());
        assert!(
            ctx.task_shutdown
                .lock()
                .await
                .is_none()
        );
        assert!(
            ctx.shutdown_complete
                .get()
                .is_some()
        );
    }

    #[tokio::test]
    async fn bind_failure_cleans_up_services() {
        let (_app, guard) = integration_test::new_test_server()
            .await
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let result = serve_until_shutdown(
            Router::new(),
            listener
                .local_addr()
                .unwrap(),
            guard
                .0
                .clone(),
            std::future::pending(),
        )
        .await;
        assert!(result.is_err());
        assert!(
            guard
                .0
                .db
                .is_closed()
        );
        assert!(
            guard
                .0
                .torrent
                .read()
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn bind_failure_bounds_cleanup_with_held_pool_connection() {
        let (_app, mut guard) = integration_test::new_test_server()
            .await
            .unwrap();
        guard
            .0
            .config
            .shutdown_timeout_secs = 1;
        let connection = guard
            .0
            .db
            .acquire()
            .await
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();

        let error = tokio::time::timeout(
            Duration::from_secs(3),
            serve_until_shutdown(
                Router::new(),
                listener
                    .local_addr()
                    .unwrap(),
                guard
                    .0
                    .clone(),
                std::future::pending(),
            ),
        )
        .await
        .unwrap()
        .unwrap_err();

        assert_eq!(
            error
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .kind(),
            std::io::ErrorKind::AddrInUse
        );
        assert!(
            guard
                .0
                .db
                .is_closed()
        );
        drop(connection);
    }

    #[tokio::test]
    async fn shutdown_requested_before_serving_is_retained() {
        let (_app, guard) = integration_test::new_test_server()
            .await
            .unwrap();
        let db = guard
            .0
            .db
            .clone();
        let (quit_tx, quit_rx) = oneshot::channel();
        quit_tx
            .send(())
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            serve_until_shutdown(
                Router::new(),
                "127.0.0.1:0"
                    .parse()
                    .unwrap(),
                guard
                    .0
                    .clone(),
                async {
                    let _ = quit_rx.await;
                },
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(db.is_closed());
        assert!(
            guard
                .0
                .shutdown_requested
                .is_cancelled()
        );
    }
}
