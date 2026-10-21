//! Launch ordering (#29).
//!
//! Launch used to be serial on the main thread: spawn the daemon and wait for
//! its ready line, handshake, read its settings, then load task state,
//! composer drafts and the selected session, and only then initialize GPUI
//! and open the window. Each daemon round trip is a full RPC, so the window
//! waited for all of them.
//!
//! Now [`Bootstrap::spawn`] runs that whole sequence on the `waku-startup`
//! thread from the first line of `waku::run`, while the main thread
//! initializes GPUI and opens the window. The window's root is [`AppRoot`]:
//!
//! - When the bootstrap has already landed by the time the window opens, it
//!   builds [`Waku`] at once and no other frame is ever drawn.
//! - Otherwise it paints a shell from the sidebar the previous launch left
//!   behind ([`snapshot`]) and swaps in [`Waku`] when the bootstrap arrives.
//! - When the daemon cannot start, the shell shows the error. Before, the
//!   process panicked before any window existed.
//!
//! Work nothing on the first frame depends on — the updater, the analytics
//! worker's network stack, provider detection and the other prefetches — runs
//! [`after_first_frame`].

mod shell;
pub mod snapshot;

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use anyhow::Context as _;
use gpui::{
    App, Context, Entity, IntoElement, ParentElement as _, Render, Styled as _, Window, div, px,
};

use crate::app::Waku;
use crate::persistence::{ComposerDrafts, PersistedState, StateStore};
use snapshot::SidebarSnapshot;
use waku_client::surfaces::drafts::DraftStore;
use waku_client::surfaces::settings::SettingsSync;

/// How long [`after_first_frame`] waits for a frame before running anyway,
/// so a window that launches hidden still gets its deferred work.
const FIRST_FRAME_FALLBACK: Duration = Duration::from_millis(750);
/// The longest the analytics worker holds its setup back for the first frame.
const LAUNCH_SETTLED_TIMEOUT: Duration = Duration::from_secs(5);

/// Everything `Waku::new` used to fetch from the daemon on the main thread.
pub struct Loaded {
    pub daemon: waku_client::DaemonSupervisor,
    /// The protocol v8 connection of every non-session surface.
    pub daemon_v8: crate::daemon_v8::DaemonV8,
    /// The daemon's settings as it holds them.
    pub daemon_settings: waku_client::DaemonSettings,
    /// Writes the desktop's daemon settings and follows other clients'.
    pub settings_sync: SettingsSync,
    /// The store that performed the load, so its "task state was loaded"
    /// guard carries over to every later save.
    pub store: StateStore,
    pub state: PersistedState,
    pub composer_draft_store: DraftStore,
    pub composer_drafts: ComposerDrafts,
}

/// The daemon start and the initial loads, running off the main thread.
pub struct Bootstrap {
    result: smol::channel::Receiver<anyhow::Result<Loaded>>,
}

impl Bootstrap {
    pub fn spawn() -> Self {
        let (sender, result) = smol::channel::bounded(1);
        let thread_sender = sender.clone();
        let spawned = std::thread::Builder::new()
            .name("waku-startup".into())
            .spawn(move || {
                let loaded = load();
                perf_count!(daemon_ready);
                let _ = thread_sender.send_blocking(loaded);
            });
        if let Err(error) = spawned {
            let _ =
                sender
                    .try_send(Err(anyhow::Error::from(error)
                        .context("could not start the Waku startup thread")));
        }
        Self { result }
    }

    fn try_take(&self) -> Option<anyhow::Result<Loaded>> {
        self.result.try_recv().ok()
    }
}

fn load() -> anyhow::Result<Loaded> {
    let daemon = crate::daemon::start_process().context("failed to start the Waku daemon")?;
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let store = StateStore::remote(daemon.clone());
    let daemon_v8 = crate::daemon_v8::for_supervisor(&daemon);
    let composer_draft_store = DraftStore::new(daemon_v8.clone());
    // The drafts and settings do not depend on the task state; overlap
    // their v8 round trips with its load instead of paying them back to
    // back.
    let v8_loads = std::thread::Builder::new()
        .name("waku-startup-v8".into())
        .spawn({
            let daemon_v8 = daemon_v8.clone();
            let store = composer_draft_store.clone();
            move || {
                let settings = waku_client::surfaces::settings::get(&daemon_v8).map(|snapshot| {
                    waku_client::surfaces::settings::settings_from_wire(&snapshot.settings)
                });
                let drafts = store
                    .load()
                    .map_err(|error| eprintln!("could not read the composer drafts: {error:#}"))
                    .ok();
                (settings, drafts)
            }
        });
    let state = store.load_or_fresh(cwd);
    let (daemon_settings, composer_drafts) = match v8_loads {
        Ok(loads) => loads
            .join()
            .map_err(|_| anyhow::anyhow!("the startup thread panicked"))?,
        Err(_) => (
            waku_client::surfaces::settings::get(&daemon_v8).map(|snapshot| {
                waku_client::surfaces::settings::settings_from_wire(&snapshot.settings)
            }),
            composer_draft_store.load().ok(),
        ),
    };
    // The desktop edits a copy of these; without them a save would write
    // defaults over the daemon's settings.
    let daemon_settings = daemon_settings.context("could not read the daemon settings")?;
    let composer_drafts = composer_drafts.unwrap_or_default();
    let settings_sync = SettingsSync::start(daemon_v8.clone(), Some(daemon_settings.clone()))
        .context("could not start the daemon settings writer")?;
    Ok(Loaded {
        daemon,
        daemon_v8,
        daemon_settings,
        settings_sync,
        store,
        state,
        composer_draft_store,
        composer_drafts,
    })
}

/// The main window's root view: the launch shell until the bootstrap lands,
/// then [`Waku`].
pub struct AppRoot {
    content: Content,
}

enum Content {
    Shell(Option<SidebarSnapshot>),
    Failed {
        snapshot: Option<SidebarSnapshot>,
        message: String,
    },
    Ready(Entity<Waku>),
}

impl AppRoot {
    pub fn new(bootstrap: Bootstrap, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let mut root = Self {
            content: Content::Shell(None),
        };
        if let Some(result) = bootstrap.try_take() {
            root.finish(result, window, cx);
            return root;
        }
        // The daemon is still starting. Paint what the last launch showed.
        let snapshot = snapshot::read(&snapshot::default_path(), &snapshot::daemon_key());
        let settings = snapshot
            .as_ref()
            .map(SidebarSnapshot::settings)
            .unwrap_or_else(shell::default_settings);
        crate::i18n::set_language(settings.language);
        window.set_rem_size(px(waku_client::persistence::sanitized_ui_font_size(
            settings.ui_font_size,
        )));
        crate::theme::apply_theme_preference(settings.theme, window, cx);
        crate::platform::set_sidebar_material_width(window, shell::sidebar_width(settings));
        root.content = Content::Shell(snapshot);
        cx.spawn_in(window, async move |this, cx| {
            let result = match bootstrap.result.recv().await {
                Ok(result) => result,
                Err(_) => Err(anyhow::anyhow!(
                    "the Waku startup thread exited without a result"
                )),
            };
            this.update_in(cx, |this, window, cx| {
                this.finish(result, window, cx);
                cx.notify();
            })
            .ok();
        })
        .detach();
        root
    }

    /// The app view, once it exists.
    pub fn waku(&self) -> Option<&Entity<Waku>> {
        match &self.content {
            Content::Ready(waku) => Some(waku),
            _ => None,
        }
    }

    fn finish(&mut self, result: anyhow::Result<Loaded>, window: &mut Window, cx: &mut App) {
        match result {
            Ok(loaded) => {
                let waku = Waku::new(window, cx, loaded);
                let composer_focus = waku.read(cx).composer_focus(cx);
                window.focus(&composer_focus, cx);
                self.content = Content::Ready(waku.clone());
                // A Linux handoff keeps the previous prefix until the new build
                // has shown its real window; a daemon that cannot start must
                // still roll the update back.
                crate::updater::signal_relaunch_ready();
                schedule_launch_work(waku, window, cx);
            }
            Err(error) => {
                eprintln!("{error:#}");
                let snapshot = match std::mem::replace(&mut self.content, Content::Shell(None)) {
                    Content::Shell(snapshot) => snapshot,
                    _ => None,
                };
                self.content = Content::Failed {
                    snapshot,
                    message: format!("{error:#}"),
                };
            }
        }
    }
}

impl Render for AppRoot {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        match &self.content {
            // `Waku::render` counts its own frames.
            Content::Ready(waku) => div().size_full().child(waku.clone()).into_any_element(),
            Content::Shell(snapshot) => {
                perf_count!(frame);
                shell::render(snapshot.as_ref(), None, cx)
            }
            Content::Failed { snapshot, message } => {
                perf_count!(frame);
                shell::render(snapshot.as_ref(), Some(message), cx)
            }
        }
    }
}

/// Run `work` once the window has drawn its next frame: from the frame tick
/// after the one that draws it, or after [`FIRST_FRAME_FALLBACK`] if no frame
/// comes (a window launched hidden), whichever is first.
pub fn after_first_frame(
    window: &mut Window,
    cx: &mut App,
    work: impl FnOnce(&mut Window, &mut App) + 'static,
) {
    type Work = Box<dyn FnOnce(&mut Window, &mut App)>;
    let work: Rc<RefCell<Option<Work>>> = Rc::new(RefCell::new(Some(Box::new(work))));
    let run = |work: &Rc<RefCell<Option<Work>>>, window: &mut Window, cx: &mut App| {
        let work = work.borrow_mut().take();
        if let Some(work) = work {
            work(window, cx);
        }
    };
    {
        let work = work.clone();
        // Next-frame callbacks run at the start of a frame tick, before that
        // tick draws, so the work goes one tick further out.
        window.on_next_frame(move |window, _| {
            window.on_next_frame(move |window, cx| run(&work, window, cx));
        });
    }
    let handle = window.window_handle();
    cx.spawn(async move |cx| {
        cx.background_executor().timer(FIRST_FRAME_FALLBACK).await;
        let _ = handle.update(cx, |_, window, cx| run(&work, window, cx));
    })
    .detach();
}

fn schedule_launch_work(waku: Entity<Waku>, window: &mut Window, cx: &mut App) {
    let after_load = waku.downgrade();
    after_first_frame(window, cx, move |_, cx| {
        mark_launch_settled();
        // Sparkle is loaded with `dlopen` and builds its controllers on the
        // main thread; nothing on the first frame needs it.
        let updater = crate::updater::Updater::init();
        let updater_available = updater.is_some();
        cx.set_global(crate::updater::UpdaterState(updater));
        if updater_available {
            crate::set_app_menus(cx, true);
            after_load
                .update(cx, |waku, cx| waku.attach_updater(cx))
                .ok();
        }
        // Refresh the snapshot from the freshly loaded sidebar, so the next
        // launch has one even if this process never quits cleanly.
        if let Ok(Some(snapshot)) = after_load.read_with(cx, |waku, _| waku.startup_snapshot()) {
            write_snapshot_in_background(snapshot, cx);
        }
    });

    let on_quit = waku.downgrade();
    cx.on_app_quit(move |cx| {
        let write = on_quit
            .read_with(cx, |waku, _| waku.startup_snapshot())
            .ok()
            .flatten()
            .map(|snapshot| {
                cx.background_executor()
                    .spawn(async move { write_snapshot(&snapshot) })
            });
        async move {
            if let Some(write) = write {
                write.await;
            }
        }
    })
    .detach();
}

fn write_snapshot_in_background(snapshot: SidebarSnapshot, cx: &App) {
    cx.background_executor()
        .spawn(async move { write_snapshot(&snapshot) })
        .detach();
}

fn write_snapshot(snapshot: &SidebarSnapshot) {
    if let Err(error) = snapshot::write(&snapshot::default_path(), snapshot) {
        eprintln!("could not write the startup sidebar snapshot: {error}");
    }
}

static LAUNCH_SETTLED: (Mutex<bool>, Condvar) = (Mutex::new(false), Condvar::new());

fn mark_launch_settled() {
    let (settled, changed) = &LAUNCH_SETTLED;
    if let Ok(mut settled) = settled.lock() {
        *settled = true;
        changed.notify_all();
    }
}

/// Block a background thread until the window has drawn its first frame (or
/// [`LAUNCH_SETTLED_TIMEOUT`] passed), so start-up work that is only CPU-
/// and I/O-hungry stays out of the way of the first frame.
pub fn wait_until_launch_settled() {
    let (settled, changed) = &LAUNCH_SETTLED;
    let Ok(guard) = settled.lock() else {
        return;
    };
    let _ = changed.wait_timeout_while(guard, LAUNCH_SETTLED_TIMEOUT, |settled| !*settled);
}
