//! Provider-neutral ACP session discovery and transcript replay.
//!
//! ACP agents own their storage migrations and visible-history projection. A
//! one-shot `session/list` or `session/load` therefore stays more accurate than
//! reading their private stores, while typed updates let Waku discard private
//! reasoning and provider-only tool records by construction.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::schema::v1::{
    ClientCapabilities, Implementation, InitializeRequest, ListSessionsRequest,
};
use agent_client_protocol::{Agent, Client, ConnectionTo};
use anyhow::{Context as _, anyhow};

use crate::model::{ProviderKind, ProviderResumeCursor, ProviderSessionSummary};

const MAX_CATALOG_PAGES: usize = 20;
const MAX_CATALOG_WORKSPACES: usize = 100;
const ACP_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

/// Empty, app-owned cwd for provider catalog subprocesses.
///
/// Agent CLIs may index their process cwd during startup. Launching a catalog
/// from `$HOME` therefore makes a harmless session-list request recursively
/// touch Desktop, Documents, Downloads, Photos, and other macOS TCC locations.
/// Keeping discovery in an isolated temp directory prevents that implicit
/// workspace scan; actual session loading still receives the selected cwd.
pub(crate) fn catalog_working_directory() -> anyhow::Result<PathBuf> {
    let directory = std::env::temp_dir().join(format!(
        "waku-provider-session-catalog-{}",
        std::process::id()
    ));
    fs::create_dir_all(&directory).with_context(|| {
        format!(
            "could not create provider-session catalog directory {}",
            directory.display()
        )
    })?;
    Ok(directory)
}

fn initialize_request() -> InitializeRequest {
    InitializeRequest::new(ProtocolVersion::V1)
        .client_capabilities(ClientCapabilities::new().terminal(false))
        .client_info(Implementation::new("waku", env!("CARGO_PKG_VERSION")))
}

fn timestamp(value: Option<&str>) -> u64 {
    value
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .and_then(|value| u64::try_from(value.timestamp()).ok())
        .unwrap_or_default()
}

pub(crate) fn session_title(
    provider: ProviderKind,
    title: Option<&str>,
    session_id: &str,
) -> String {
    title
        .map(str::trim)
        .filter(|title| !title.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| {
            let short_id = session_id.chars().take(8).collect::<String>();
            format!("{} session {short_id}", provider.short_name())
        })
}

/// Lists every session exposed by an ACP agent. An empty `cwd_filters` list
/// requests the provider's global catalog; providers such as Kimi that require
/// a cwd pass their native workspace index here instead.
pub fn list_provider_sessions(
    provider: ProviderKind,
    binary: &Path,
    cwd_filters: &[PathBuf],
    limit: usize,
) -> anyhow::Result<Vec<ProviderSessionSummary>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let agent = crate::driver::catalog_agent(provider, binary, &catalog_working_directory()?)?;
    let filters = if cwd_filters.is_empty() {
        vec![None]
    } else {
        cwd_filters
            .iter()
            .take(MAX_CATALOG_WORKSPACES)
            .cloned()
            .map(Some)
            .collect()
    };

    let request = Client.builder().name("waku-session-catalog").connect_with(
        agent,
        async move |connection: ConnectionTo<Agent>| {
            let initialize = connection
                .send_request(initialize_request())
                .block_task()
                .await?;
            if initialize
                .agent_capabilities
                .session_capabilities
                .list
                .is_none()
            {
                return Ok(Vec::new());
            }

            let mut found = Vec::new();
            let mut seen = HashSet::new();
            for cwd in filters {
                let mut cursor = None;
                let mut seen_cursors = HashSet::new();
                for _ in 0..MAX_CATALOG_PAGES {
                    let mut request = ListSessionsRequest::new().cursor(cursor.clone());
                    if let Some(cwd) = cwd.clone() {
                        request = request.cwd(cwd);
                    }
                    let response = connection.send_request(request).block_task().await?;
                    for session in response.sessions {
                        let session_id = session.session_id.to_string();
                        if session_id.trim().is_empty()
                            || !session.cwd.is_absolute()
                            || !seen.insert(session_id.clone())
                        {
                            continue;
                        }
                        let updated_at = timestamp(session.updated_at.as_deref());
                        found.push(ProviderSessionSummary {
                            cursor: ProviderResumeCursor::from_session_id(
                                provider,
                                session_id.clone(),
                            ),
                            title: session_title(provider, session.title.as_deref(), &session_id),
                            cwd: session.cwd,
                            created_at: updated_at,
                            updated_at,
                        });
                    }
                    if found.len() >= limit {
                        break;
                    }
                    let Some(next) = response.next_cursor.filter(|next| !next.is_empty()) else {
                        break;
                    };
                    if !seen_cursors.insert(next.clone()) {
                        break;
                    }
                    cursor = Some(next);
                }
                if found.len() >= limit {
                    break;
                }
            }
            found.sort_by_key(|session| std::cmp::Reverse(session.updated_at));
            found.truncate(limit);
            Ok(found)
        },
    );
    smol::block_on(smol::future::race(
        async move { request.await.map_err(anyhow::Error::new) },
        async move {
            smol::Timer::after(ACP_REQUEST_TIMEOUT).await;
            Err(anyhow!(
                "{} ACP session catalog timed out",
                provider.display_name()
            ))
        },
    ))
    .with_context(|| format!("{} could not list ACP sessions", provider.display_name()))
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn catalog_processes_use_an_isolated_temp_working_directory() {
        let directory = catalog_working_directory().unwrap();
        assert!(directory.starts_with(std::env::temp_dir()));
        assert!(directory.is_dir());
        if let Some(home) = dirs::home_dir() {
            assert_ne!(directory, home);
            for protected in [
                "Desktop",
                "Documents",
                "Downloads",
                "Movies",
                "Music",
                "Pictures",
            ] {
                assert!(!directory.starts_with(home.join(protected)));
            }
        }
    }
}
