//! Validated runtime configuration for live ingestion.
//!
//! The config file never carries secrets: only the *name* of the environment
//! variable that holds the access token. Every value is validated before a
//! client is built or an archive is opened, and the token is wrapped so it can
//! never reach a `Debug` rendering, log line, error message or tracked file.

use std::fmt;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Name of the environment variable read when the config does not name one.
pub const DEFAULT_TOKEN_ENV: &str = "MLN_ACCESS_TOKEN";

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("reading config {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    /// The source snippet is deliberately not retained: a TOML error can
    /// contain secret-looking values, and it must never be echoed.
    #[error("invalid TOML in {path}{line}")]
    Parse { path: PathBuf, line: String },
    #[error("invalid config: {0}")]
    Invalid(String),
    #[error("storage check failed: {0}")]
    Storage(String),
}

/// An access token that is never printed.
#[derive(Clone)]
pub struct Token(String);

impl Token {
    pub fn new(token: impl Into<String>) -> Self {
        Self(token.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Token(<redacted>)")
    }
}

/// A room in the explicit allowlist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoomSelector {
    /// An exact room id, which is the room's identity.
    Id(String),
    /// A room alias; resolved once and pinned so drift cannot widen the set.
    Alias(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomConfig {
    pub selector: RoomSelector,
    /// Explicit opt-in to join this configured room when the archive has no
    /// observed membership for it. A leave/ban is never auto-reversed.
    pub join: bool,
    /// Optional explicit routing hints (`via`) for joining this room, for a
    /// cold homeserver that cannot route an unhinted room id.
    pub via: Vec<String>,
}

/// Validated configuration used by the live runtime.
#[derive(Debug, Clone)]
pub struct Config {
    pub homeserver: String,
    pub user_id: String,
    pub device_id: String,
    pub token_env: String,
    pub data_dir: PathBuf,
    pub database: PathBuf,
    pub history_interval_ms: u64,
    pub history_limit: u32,
    pub sync_timeout_ms: u64,
    pub rooms: Vec<RoomConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigFile {
    homeserver: String,
    user_id: String,
    device_id: String,
    #[serde(default = "default_token_env")]
    token_env: String,
    #[serde(default = "default_data_dir")]
    data_dir: PathBuf,
    #[serde(default = "default_database")]
    database: PathBuf,
    #[serde(default = "default_history_interval_ms")]
    history_interval_ms: u64,
    #[serde(default = "default_history_limit")]
    history_limit: u32,
    #[serde(default = "default_sync_timeout_ms")]
    sync_timeout_ms: u64,
    #[serde(default)]
    rooms: Vec<RoomFile>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoomFile {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    alias: Option<String>,
    #[serde(default)]
    join: bool,
    #[serde(default)]
    via: Vec<String>,
}

fn default_token_env() -> String {
    DEFAULT_TOKEN_ENV.to_owned()
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("~/.local/share/mainlinenerd")
}

fn default_database() -> PathBuf {
    PathBuf::from("archive.sqlite3")
}

fn default_history_interval_ms() -> u64 {
    1_000
}

fn default_history_limit() -> u32 {
    50
}

fn default_sync_timeout_ms() -> u64 {
    30_000
}

impl Config {
    /// Read and validate a config file.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let file: ConfigFile = toml::from_str(&text).map_err(|error| {
            let line = error
                .span()
                .map(|span| line_number(&text, span.start))
                .map(|line| format!(" at line {line}"))
                .unwrap_or_default();
            ConfigError::Parse {
                path: path.to_path_buf(),
                line,
            }
        })?;
        let base = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        Self::from_file(file, &base)
    }

    fn from_file(file: ConfigFile, base: &Path) -> Result<Self, ConfigError> {
        let homeserver = validate_homeserver(&file.homeserver)?;

        let user_id = file.user_id.trim().to_owned();
        ruma_common::UserId::parse(&user_id).map_err(|error| {
            ConfigError::Invalid(format!(
                "user_id {user_id:?} is not a valid Matrix user id: {error}"
            ))
        })?;

        let device_id = file.device_id.trim().to_owned();
        if device_id.is_empty() {
            return Err(ConfigError::Invalid(
                "device_id must not be empty".to_owned(),
            ));
        }

        let token_env = file.token_env.trim().to_owned();
        if !valid_env_name(&token_env) {
            return Err(ConfigError::Invalid(format!(
                "token_env {token_env:?} is not a valid environment variable name"
            )));
        }

        if file.rooms.is_empty() {
            return Err(ConfigError::Invalid(
                "at least one [[rooms]] entry is required; ingestion has no discovery crawl"
                    .to_owned(),
            ));
        }
        let mut rooms = Vec::with_capacity(file.rooms.len());
        let mut seen = std::collections::BTreeSet::new();
        for room in file.rooms {
            let selector = match (room.id, room.alias) {
                (Some(id), None) => {
                    let id = id.trim().to_owned();
                    ruma_common::RoomId::parse(&id).map_err(|error| {
                        ConfigError::Invalid(format!("room id {id:?} is not valid: {error}"))
                    })?;
                    RoomSelector::Id(id)
                }
                (None, Some(alias)) => {
                    let alias = alias.trim().to_owned();
                    ruma_common::RoomAliasId::parse(&alias).map_err(|error| {
                        ConfigError::Invalid(format!("room alias {alias:?} is not valid: {error}"))
                    })?;
                    RoomSelector::Alias(alias)
                }
                (Some(_), Some(_)) => {
                    return Err(ConfigError::Invalid(
                        "a room entry must set exactly one of id or alias, not both".to_owned(),
                    ));
                }
                (None, None) => {
                    return Err(ConfigError::Invalid(
                        "a room entry must set either id or alias".to_owned(),
                    ));
                }
            };
            let key = selector_key(&selector);
            if !seen.insert(key) {
                return Err(ConfigError::Invalid(format!(
                    "duplicate room {} in the allowlist",
                    selector_display(&selector)
                )));
            }
            let mut via = Vec::with_capacity(room.via.len());
            for server in room.via {
                let server = server.trim().to_owned();
                ruma_common::ServerName::parse(&server).map_err(|_| {
                    ConfigError::Invalid("a room `via` entry is not a valid server name".to_owned())
                })?;
                via.push(server);
            }
            rooms.push(RoomConfig {
                selector,
                join: room.join,
                via,
            });
        }

        if file.history_limit == 0 || file.history_limit > 1_000 {
            return Err(ConfigError::Invalid(
                "history_limit must be between 1 and 1000".to_owned(),
            ));
        }
        if file.sync_timeout_ms == 0 {
            return Err(ConfigError::Invalid(
                "sync_timeout_ms must be at least 1".to_owned(),
            ));
        }
        if file.history_interval_ms == 0 {
            return Err(ConfigError::Invalid(
                "history_interval_ms must be at least 1; history is always paced".to_owned(),
            ));
        }

        let data_dir = resolve_path(base, &expand_tilde(&file.data_dir));
        let database = resolve_path(&data_dir, &file.database);

        Ok(Self {
            homeserver,
            user_id,
            device_id,
            token_env,
            data_dir,
            database,
            history_interval_ms: file.history_interval_ms,
            history_limit: file.history_limit,
            sync_timeout_ms: file.sync_timeout_ms,
            rooms,
        })
    }

    /// Read the access token from the configured environment variable without
    /// ever echoing its value.
    pub fn obtain_token(&self) -> Result<Token, ConfigError> {
        match std::env::var(&self.token_env) {
            Ok(value) if !value.is_empty() => Ok(Token::new(value)),
            Ok(_) => Err(ConfigError::Invalid(format!(
                "environment variable {} is set but empty; export the bot access token there",
                self.token_env
            ))),
            Err(std::env::VarError::NotPresent) => Err(ConfigError::Invalid(format!(
                "environment variable {} is not set; export the bot access token there",
                self.token_env
            ))),
            Err(std::env::VarError::NotUnicode(_)) => Err(ConfigError::Invalid(format!(
                "environment variable {} is not valid UTF-8",
                self.token_env
            ))),
        }
    }

    /// Refuse unsafe storage locations instead of silently widening access.
    ///
    /// The data directory must be a real directory (not a symlink) that is not
    /// group/world accessible. A missing directory is created owner-only. An
    /// existing archive file must be a regular file, never a symlink.
    pub fn ensure_storage(&self) -> Result<(), ConfigError> {
        ensure_private_dir(&self.data_dir)?;
        if let Some(parent) = self.database.parent() {
            if !parent.as_os_str().is_empty() && parent != self.data_dir {
                ensure_private_dir(parent)?;
            }
        }
        match std::fs::symlink_metadata(&self.database) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(ConfigError::Storage(format!(
                        "{} is a symlink; refusing to open an archive through it",
                        self.database.display()
                    )));
                }
                if !meta.is_file() {
                    return Err(ConfigError::Storage(format!(
                        "{} exists but is not a regular file",
                        self.database.display()
                    )));
                }
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(ConfigError::Storage(format!(
                "checking {}: {error}",
                self.database.display()
            ))),
        }
    }
}

/// Validate and normalize the homeserver base URL.
///
/// HTTPS is required except for loopback hosts, where plain HTTP is allowed
/// for local deployments and tests. Credentials, query strings and fragments
/// are rejected so the client can never be pointed at a credential redirect.
pub fn validate_homeserver(input: &str) -> Result<String, ConfigError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(ConfigError::Invalid(
            "homeserver must not be empty".to_owned(),
        ));
    }
    // Error messages deliberately do not echo the input: a rejected URL may
    // carry credentials, a query string or a fragment that must never reach a
    // log, a terminal or an error chain.
    let url = url::Url::parse(trimmed)
        .map_err(|_| ConfigError::Invalid("homeserver is not a valid URL".to_owned()))?;

    match url.scheme() {
        "https" => {}
        "http" => {
            if !host_is_loopback(&url) {
                return Err(ConfigError::Invalid(
                    "homeserver uses plain HTTP; only loopback hosts may use HTTP. \
                     Use https:// for a remote homeserver"
                        .to_owned(),
                ));
            }
        }
        _ => {
            return Err(ConfigError::Invalid(
                "homeserver uses an unsupported scheme; use https (or http on loopback)".to_owned(),
            ));
        }
    }
    if url.host_str().is_none() {
        return Err(ConfigError::Invalid("homeserver has no host".to_owned()));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ConfigError::Invalid(
            "homeserver URL must not carry credentials; put the access token in the environment, \
             never in the URL"
                .to_owned(),
        ));
    }
    if url.query().is_some() {
        return Err(ConfigError::Invalid(
            "homeserver URL must not have a query string".to_owned(),
        ));
    }
    if url.fragment().is_some() {
        return Err(ConfigError::Invalid(
            "homeserver URL must not have a fragment".to_owned(),
        ));
    }
    Ok(url.as_str().trim_end_matches('/').to_owned())
}

fn host_is_loopback(url: &url::Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(name)) => name.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    }
}

fn valid_env_name(name: &str) -> bool {
    !name.is_empty()
        && !name.contains('=')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Exact id or alias used to detect duplicate allowlist entries. Alias
/// localparts are identifiers and are case-sensitive, so they are not
/// normalized: `#Room:hs` and `#room:hs` are distinct aliases.
pub fn selector_key(selector: &RoomSelector) -> String {
    match selector {
        RoomSelector::Id(id) => id.clone(),
        RoomSelector::Alias(alias) => alias.clone(),
    }
}

pub fn selector_display(selector: &RoomSelector) -> &str {
    match selector {
        RoomSelector::Id(id) => id,
        RoomSelector::Alias(alias) => alias,
    }
}

fn line_number(text: &str, byte_offset: usize) -> usize {
    text[..byte_offset.min(text.len())]
        .bytes()
        .filter(|byte| *byte == b'\n')
        .count()
        + 1
}

fn resolve_path(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

fn expand_tilde(path: &Path) -> PathBuf {
    let text = path.to_string_lossy();
    if let Some(rest) = text.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    } else if text == "~" {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home);
        }
    }
    path.to_path_buf()
}

fn ensure_private_dir(dir: &Path) -> Result<(), ConfigError> {
    match std::fs::symlink_metadata(dir) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(ConfigError::Storage(format!(
                    "{} is a symlink; refusing to use it as the data directory",
                    dir.display()
                )));
            }
            if !meta.is_dir() {
                return Err(ConfigError::Storage(format!(
                    "{} exists but is not a directory",
                    dir.display()
                )));
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = meta.permissions().mode() & 0o777;
                if mode & 0o077 != 0 {
                    return Err(ConfigError::Storage(format!(
                        "{} is accessible to group/other (mode {mode:o}); run `chmod 700 {}` \
                         or point data_dir at a private directory",
                        dir.display(),
                        dir.display()
                    )));
                }
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => create_private_dir(dir),
        Err(error) => Err(ConfigError::Storage(format!(
            "checking {}: {error}",
            dir.display()
        ))),
    }
}

fn create_private_dir(dir: &Path) -> Result<(), ConfigError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)
            .map_err(|error| {
                ConfigError::Storage(format!("creating {}: {error}", dir.display()))
            })?;
        // DirBuilder's mode is masked by the umask; apply it exactly.
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).map_err(|error| {
            ConfigError::Storage(format!("setting permissions on {}: {error}", dir.display()))
        })?;
    }
    #[cfg(not(unix))]
    {
        std::fs::create_dir_all(dir).map_err(|error| {
            ConfigError::Storage(format!("creating {}: {error}", dir.display()))
        })?;
    }
    Ok(())
}
