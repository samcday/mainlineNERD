//! Configuration validation, storage safety and token redaction.
//!
//! Everything here is offline: no client is built and no server is contacted.

use std::path::{Path, PathBuf};

use mainlinenerd_ingest::config::{validate_homeserver, Config, Token};

fn write_config(dir: &Path, body: &str) -> PathBuf {
    let path = dir.join("mln.toml");
    std::fs::write(&path, body).unwrap();
    path
}

fn base_config(dir: &Path, extra: &str) -> String {
    format!(
        r#"
homeserver = "https://hs.example.org"
user_id = "@ingest:hs.example.org"
device_id = "MLN"
data_dir = "{}"
database = "archive.sqlite3"
{extra}
[[rooms]]
id = "!room:hs.example.org"
"#,
        dir.display()
    )
}

#[test]
fn example_config_is_valid() {
    let config = Config::load(Path::new("config.example.toml")).expect("example config parses");
    assert_eq!(config.user_id, "@mainlinenerd:example.org");
    assert_eq!(config.rooms.len(), 2);
    assert_eq!(config.token_env, "MLN_ACCESS_TOKEN");
    assert_eq!(config.history_limit, 50);
    assert!(config.database.ends_with("archive.sqlite3"));
}

#[test]
fn homeserver_scheme_rules() {
    assert!(validate_homeserver("https://matrix.example.org").is_ok());
    // Loopback HTTP is the explicit local/test exception.
    for url in [
        "http://127.0.0.1:8008",
        "http://localhost:8008",
        "http://[::1]:8008",
    ] {
        assert!(validate_homeserver(url).is_ok(), "{url} should be allowed");
    }
    // Any other plain HTTP host is refused.
    for url in ["http://matrix.example.org", "http://10.0.0.5:8008"] {
        let error = validate_homeserver(url).unwrap_err();
        assert!(
            error.to_string().contains("loopback"),
            "non-loopback HTTP must be refused: {error}"
        );
    }
}

#[test]
fn homeserver_rejects_credentials_query_fragment_and_schemes() {
    for url in [
        "https://user:pass@matrix.example.org",
        "https://matrix.example.org?next=https://evil.example",
        "https://matrix.example.org#fragment",
        "ftp://matrix.example.org",
        "not a url",
    ] {
        assert!(validate_homeserver(url).is_err(), "{url} must be rejected");
    }
}

#[test]
fn config_rejects_bad_rooms_and_values() {
    let dir = tempfile::tempdir().unwrap();

    let body = r##"
homeserver = "https://hs.example.org"
user_id = "@ingest:hs.example.org"
device_id = "MLN"
data_dir = "."
[[rooms]]
id = "!room:hs.example.org"
alias = "#room:hs.example.org"
"##;
    let path = write_config(dir.path(), body);
    let error = Config::load(&path).unwrap_err();
    assert!(error.to_string().contains("exactly one"), "{error}");

    let body = r#"
homeserver = "https://hs.example.org"
user_id = "@ingest:hs.example.org"
device_id = "MLN"
data_dir = "."
"#;
    let path = write_config(dir.path(), body);
    assert!(Config::load(&path).is_err(), "no rooms must be rejected");

    let duplicate = format!(
        r#"
homeserver = "https://hs.example.org"
user_id = "@ingest:hs.example.org"
device_id = "MLN"
data_dir = "{}"
[[rooms]]
id = "!room:hs.example.org"
[[rooms]]
id = "!room:hs.example.org"
"#,
        dir.path().display()
    );
    let path = write_config(dir.path(), &duplicate);
    let error = Config::load(&path).unwrap_err();
    assert!(error.to_string().contains("duplicate"), "{error}");

    let bad_limit = format!(
        r#"
homeserver = "https://hs.example.org"
user_id = "@ingest:hs.example.org"
device_id = "MLN"
data_dir = "{}"
history_limit = 5000
[[rooms]]
id = "!room:hs.example.org"
"#,
        dir.path().display()
    );
    let path = write_config(dir.path(), &bad_limit);
    let error = Config::load(&path).unwrap_err();
    assert!(error.to_string().contains("history_limit"), "{error}");
}

#[test]
fn relative_database_resolves_under_data_dir() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(dir.path(), &base_config(dir.path(), ""));
    let config = Config::load(&path).unwrap();
    assert_eq!(config.data_dir, dir.path());
    assert_eq!(config.database, dir.path().join("archive.sqlite3"));
}

#[test]
fn storage_rejects_world_accessible_directory() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir(&data_dir).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&data_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let path = write_config(dir.path(), &base_config(&data_dir, ""));
    let config = Config::load(&path).unwrap();
    let error = config.ensure_storage().unwrap_err();
    assert!(
        error.to_string().contains("chmod 700"),
        "must explain the required permissions: {error}"
    );
}

#[test]
fn storage_creates_private_directory() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("fresh").join("nested");
    let path = write_config(dir.path(), &base_config(&data_dir, ""));
    let config = Config::load(&path).unwrap();
    config.ensure_storage().unwrap();
    assert!(data_dir.is_dir());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&data_dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
    }
}

#[cfg(unix)]
#[test]
fn storage_rejects_symlinked_archive() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir(&data_dir).unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&data_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let real = dir.path().join("real.sqlite3");
    std::fs::write(&real, b"x").unwrap();
    std::os::unix::fs::symlink(&real, data_dir.join("archive.sqlite3")).unwrap();
    let path = write_config(dir.path(), &base_config(&data_dir, ""));
    let config = Config::load(&path).unwrap();
    let error = config.ensure_storage().unwrap_err();
    assert!(error.to_string().contains("symlink"), "{error}");
}

#[test]
fn missing_token_env_names_the_variable_without_a_value() {
    let dir = tempfile::tempdir().unwrap();
    let body = base_config(dir.path(), "token_env = \"MLN_TEST_TOKEN_ABSENT\"\n");
    let path = write_config(dir.path(), &body);
    let config = Config::load(&path).unwrap();
    let error = config.obtain_token().unwrap_err();
    assert!(
        error.to_string().contains("MLN_TEST_TOKEN_ABSENT"),
        "must name the variable: {error}"
    );
}

#[test]
fn token_debug_never_reveals_the_secret() {
    let token = Token::new("super-secret-token-value");
    let rendered = format!("{token:?}");
    assert!(!rendered.contains("super-secret-token-value"));
    assert!(rendered.contains("redacted"));
}

#[test]
fn zero_history_interval_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let body = base_config(dir.path(), "history_interval_ms = 0\n");
    let path = write_config(dir.path(), &body);
    let error = Config::load(&path).unwrap_err();
    assert!(error.to_string().contains("history_interval_ms"), "{error}");
}

#[test]
fn alias_identity_is_case_sensitive_and_duplicates_are_exact() {
    let dir = tempfile::tempdir().unwrap();
    let body = format!(
        r##"
homeserver = "https://hs.example.org"
user_id = "@ingest:hs.example.org"
device_id = "MLN"
data_dir = "{}"
[[rooms]]
alias = "#Room:hs.example.org"
[[rooms]]
alias = "#room:hs.example.org"
"##,
        dir.path().display()
    );
    let path = write_config(dir.path(), &body);
    let config = Config::load(&path).unwrap();
    assert_eq!(config.rooms.len(), 2);
    assert_eq!(
        mainlinenerd_ingest::config::selector_key(&config.rooms[0].selector),
        "#Room:hs.example.org"
    );
    assert_eq!(
        mainlinenerd_ingest::config::selector_key(&config.rooms[1].selector),
        "#room:hs.example.org"
    );

    let duplicate = body.replace("#room:hs.example.org", "#Room:hs.example.org");
    let path = write_config(dir.path(), &duplicate);
    let error = Config::load(&path).unwrap_err();
    assert!(error.to_string().contains("duplicate"), "{error}");
}

#[test]
fn parse_and_url_errors_never_echo_source_or_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let secret = "SECRET_MARKER_do_not_print";
    let malformed = format!("homeserver = {secret}\nthis is not toml =\n");
    let path = write_config(dir.path(), &malformed);
    let error = Config::load(&path).unwrap_err().to_string();
    assert!(!error.contains(secret), "TOML source leaked: {error}");

    for url in [
        "https://user:password_marker@hs.example.org",
        "https://hs.example.org?token=query_marker",
        "https://hs.example.org#fragment_marker",
    ] {
        let error = validate_homeserver(url).unwrap_err().to_string();
        for marker in ["password_marker", "query_marker", "fragment_marker"] {
            assert!(!error.contains(marker), "URL leaked in error: {error}");
        }
    }
}

/// Device ids are completely opaque: surrounding whitespace is part of the
/// identifier and must survive a config round trip byte for byte.
#[test]
fn device_id_round_trips_surrounding_whitespace_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let body = format!(
        r#"
homeserver = "https://hs.example.org"
user_id = "@ingest:hs.example.org"
device_id = "  MLN\tDEV  "
data_dir = "{}"
[[rooms]]
id = "!room:hs.example.org"
"#,
        dir.path().display()
    );
    let path = write_config(dir.path(), &body);
    let config = Config::load(&path).unwrap();
    assert_eq!(
        config.device_id, "  MLN\tDEV  ",
        "an opaque device id must be preserved exactly, not trimmed"
    );
}

/// Escaped control characters are legal opaque device-id content and must be
/// decoded and preserved exactly.
#[test]
fn device_id_round_trips_escaped_control_characters_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let body = format!(
        r#"
homeserver = "https://hs.example.org"
user_id = "@ingest:hs.example.org"
device_id = "dev\u0007ice"
data_dir = "{}"
[[rooms]]
id = "!room:hs.example.org"
"#,
        dir.path().display()
    );
    let path = write_config(dir.path(), &body);
    let config = Config::load(&path).unwrap();
    assert_eq!(config.device_id, "dev\u{7}ice");
}

/// An empty value is a missing configuration field; whitespace is still a
/// legitimate opaque identifier and must not be treated as empty.
#[test]
fn device_id_preserves_whitespace_but_rejects_empty_value() {
    let dir = tempfile::tempdir().unwrap();
    for value in ["", "   ", "\t"] {
        let body = format!(
            r#"
homeserver = "https://hs.example.org"
user_id = "@ingest:hs.example.org"
device_id = "{value}"
data_dir = "{}"
[[rooms]]
id = "!room:hs.example.org"
"#,
            dir.path().display()
        );
        let path = write_config(dir.path(), &body);
        if value.is_empty() {
            let error = Config::load(&path).unwrap_err();
            assert_eq!(
                error.to_string(),
                "invalid config: device_id must not be empty"
            );
        } else {
            assert_eq!(Config::load(&path).unwrap().device_id, value);
        }
    }
}
