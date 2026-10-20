//! The daemon's protocol v8 endpoint (RFC 0003), served beside the legacy
//! protocol until the clients move over (#53).
//!
//! - **WebSocket**: `GET /v8` with subprotocol `waku.v8` on the daemon's TCP
//!   listener; `/v1` stays the legacy endpoint.
//! - **Local socket**: the same Unix socket or named pipe as the legacy
//!   protocol. A v8 client opens with the 8-byte `WAKU` preface, a legacy
//!   client with an HTTP upgrade, so the first four bytes tell them apart.
//!
//! Methods start as adapters over the existing [`Backend`]; orchestration
//! (`orchestration.dispatch`, the `shell` and `thread` streams) lands with
//! #54 on the event store.

mod connection;
mod methods;
mod streams;
mod transport;

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio::sync::watch;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;
use waku_protocol::v8::frame::{PrefaceCheck, check_preface};
use waku_protocol::v8::{
    AppGrant, Encoding, EnvironmentInfo, HELLO_DEADLINE_MS, Limits, PREFACE, Settings,
    SettingsItem, codec,
};

use crate::model::ProviderKind;
use crate::protocol::{Command, Request, ResponsePayload};
use crate::server::{Backend, EventSink, ShutdownSignal};
use crate::settings::DaemonSettings;

pub(crate) use connection::Budget;

/// Everything a v8 connection needs from the daemon.
pub(crate) struct V8Context {
    pub(crate) token: String,
    pub(crate) backend: BackendHandle,
    pub(crate) settings: SettingsBus,
    pub(crate) shutdown: ShutdownSignal,
    pub(crate) limits: Limits,
    pub(crate) budget: Budget,
    pub(crate) hello_deadline: Duration,
    pub(crate) environment: EnvironmentInfo,
}

impl V8Context {
    pub(crate) fn new(
        token: String,
        backend: BackendHandle,
        settings: SettingsBus,
        shutdown: ShutdownSignal,
    ) -> Self {
        Self {
            token,
            backend,
            settings,
            shutdown,
            limits: Limits::default(),
            budget: Budget::default(),
            hello_deadline: Duration::from_millis(u64::from(HELLO_DEADLINE_MS)),
            environment: environment(),
        }
    }
}

fn environment() -> EnvironmentInfo {
    EnvironmentInfo {
        // TODO(#54): the persistent environment id and the event store's id
        // (RFC 0001). Until then cursors never span daemon restarts.
        id: process_id(),
        name: host_name(),
        host_os: std::env::consts::OS.into(),
        host_arch: std::env::consts::ARCH.into(),
        store_id: process_id(),
    }
}

fn process_id() -> Uuid {
    static ID: OnceLock<Uuid> = OnceLock::new();
    *ID.get_or_init(Uuid::new_v4)
}

fn host_name() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "localhost".into())
}

/// Calls into the daemon's [`Backend`] on behalf of v8 methods. They are not
/// runtime commands, so they use no session.
#[derive(Clone)]
pub(crate) struct BackendHandle {
    backend: Arc<dyn Backend>,
    events: EventSink,
}

impl BackendHandle {
    pub(crate) fn new(backend: Arc<dyn Backend>, events: EventSink) -> Self {
        Self { backend, events }
    }

    pub(crate) fn call(&self, command: Command) -> anyhow::Result<ResponsePayload> {
        self.backend.handle(
            Request {
                request_id: Uuid::new_v4(),
                session_id: Uuid::nil(),
                runtime_id: Uuid::nil(),
                command,
            },
            self.events.clone(),
        )
    }

    /// The daemon settings as clients see them, published to `bus`.
    pub(crate) fn load_settings(&self, bus: &SettingsBus) -> anyhow::Result<Arc<SettingsState>> {
        match self.call(Command::GetSettings)? {
            ResponsePayload::Settings { settings } => Ok(bus.publish(wire_settings(&settings))),
            other => anyhow::bail!("unexpected settings response {}", payload_name(&other)),
        }
    }
}

fn payload_name(payload: &ResponsePayload) -> String {
    serde_json::to_value(payload)
        .ok()
        .and_then(|value| value.get("type")?.as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// One published settings revision. Its `updated` item is encoded at most
/// once per encoding and shared by every subscriber (RFC 0003 §2.4).
pub(crate) struct SettingsState {
    pub(crate) revision: u64,
    pub(crate) settings: Settings,
    updated: [OnceLock<Arc<[u8]>>; 2],
}

impl SettingsState {
    pub(crate) fn updated_item(&self, encoding: Encoding) -> Arc<[u8]> {
        let slot = match encoding {
            Encoding::MessagePack => &self.updated[0],
            Encoding::Json => &self.updated[1],
        };
        slot.get_or_init(|| {
            codec::to_vec(
                encoding,
                &SettingsItem::Updated {
                    revision: self.revision,
                    settings: self.settings.clone(),
                },
            )
            .expect("settings items encode")
            .into()
        })
        .clone()
    }
}

/// The latest daemon settings, for the `settings` stream. Settings changed
/// through either protocol are published here.
#[derive(Clone)]
pub(crate) struct SettingsBus {
    sender: Arc<watch::Sender<Option<Arc<SettingsState>>>>,
}

impl Default for SettingsBus {
    fn default() -> Self {
        Self {
            sender: Arc::new(watch::channel(None).0),
        }
    }
}

impl SettingsBus {
    pub(crate) fn current(&self) -> Option<Arc<SettingsState>> {
        self.sender.borrow().clone()
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<Option<Arc<SettingsState>>> {
        self.sender.subscribe()
    }

    pub(crate) fn has_subscribers(&self) -> bool {
        self.sender.receiver_count() > 0
    }

    /// Publish `settings`; subscribers are woken only when the revision
    /// changes.
    pub(crate) fn publish(&self, settings: Settings) -> Arc<SettingsState> {
        let revision = settings_revision(&settings);
        let mut published = None;
        self.sender.send_if_modified(|current| {
            if let Some(current) = current
                && current.revision == revision
            {
                published = Some(current.clone());
                return false;
            }
            let state = Arc::new(SettingsState {
                revision,
                settings,
                updated: Default::default(),
            });
            *current = Some(state.clone());
            published = Some(state);
            true
        });
        published.expect("set by the closure")
    }
}

/// A settings revision: a content hash, so equal revisions mean equal
/// settings across daemon restarts too. 53 bits, so JavaScript holds it.
pub(crate) fn settings_revision(settings: &Settings) -> u64 {
    let bytes = codec::to_vec(Encoding::MessagePack, settings).expect("settings encode");
    // FNV-1a.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash & waku_protocol::v8::MAX_SAFE_INTEGER
}

/// The legacy settings document as v8 clients see it.
pub(crate) fn wire_settings(settings: &DaemonSettings) -> Settings {
    Settings {
        computer_use_enabled: settings.computer_use_enabled,
        computer_use_allowed_apps: settings
            .computer_use_allowed_apps
            .iter()
            .map(|grant| AppGrant {
                bundle_id: grant.bundle_id.clone(),
                app_name: grant.app_name.clone(),
            })
            .collect(),
        disabled_providers: settings
            .disabled_providers
            .iter()
            .map(provider_name)
            .collect(),
        provider_binary_overrides: settings
            .provider_binary_overrides
            .iter()
            .map(|(provider, binary)| (provider_name(provider), binary.clone()))
            .collect(),
    }
}

fn provider_name(provider: &ProviderKind) -> String {
    serde_json::to_value(provider)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// The legacy protocol changed the settings: re-read and publish them for
/// v8 subscribers. Runs on the blocking pool after `UpdateSettings`.
pub(crate) fn settings_changed(backend: &BackendHandle, bus: &SettingsBus) {
    if !bus.has_subscribers() {
        return;
    }
    if let Err(error) = backend.load_settings(bus) {
        eprintln!("waku-daemon could not publish settings to v8 clients: {error:#}");
    }
}

/// Serve a v8 client on the local socket. `magic` is the four bytes already
/// read to tell it from a legacy client.
pub(crate) async fn serve_local<S>(mut stream: S, magic: [u8; 4], context: Arc<V8Context>)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut preface = [0; 8];
    preface[..4].copy_from_slice(&magic);
    match tokio::time::timeout(context.hello_deadline, stream.read_exact(&mut preface[4..])).await {
        Ok(Ok(_)) => {}
        _ => return,
    }
    match check_preface(&preface) {
        PrefaceCheck::NotWaku => return,
        // Answer with our preface so the client can report the mismatch.
        PrefaceCheck::OtherMajor(_) => {
            let _ = stream.write_all(&PREFACE).await;
            let _ = stream.shutdown().await;
            return;
        }
        PrefaceCheck::Match => {}
    }
    if stream.write_all(&PREFACE).await.is_err() || stream.flush().await.is_err() {
        return;
    }
    let (reader, writer) = transport::local(stream);
    connection::serve(reader, writer, context, || {}).await;
}

/// Serve a v8 client on an upgraded `/v8` WebSocket. `authenticated` runs
/// once its `hello` checks out (it lifts the pre-auth read cap).
pub(crate) async fn serve_websocket<S>(
    socket: WebSocketStream<S>,
    context: Arc<V8Context>,
    authenticated: impl FnOnce(),
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (reader, writer) = transport::websocket(socket);
    connection::serve(reader, writer, context, authenticated).await;
}

#[cfg(all(test, unix))]
mod tests;
