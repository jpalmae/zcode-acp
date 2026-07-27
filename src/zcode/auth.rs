//! Z.AI OAuth login flow + credential storage.
//!
//! Replicates what `zcode login` does end-to-end so that an ACP host (Buzz) can
//! drive ZCode's OAuth via the `authenticate` method, instead of requiring the
//! user to pre-configure `~/.zcode/cli/config.json`.
//!
//! # Flow (verified against the bundle)
//!
//! The Z.AI CLI flow is **init/poll**, not authorization-code+PKCE:
//!
//! 1. Generate a random 32-byte `pollToken` (hex).
//! 2. `POST {origin}/api/v1/oauth/cli/init` with `Authorization: Bearer
//!    <pollToken>` and body `{"provider":"zai"}`. Returns `{flow_id,
//!    poll_token, authorize_url, expires_at, poll_interval_sec}`.
//! 3. Hand `authorize_url` to the host (it will surface it to the human via
//!    ACP's auth flow / a browser).
//! 4. Poll `GET {origin}/api/v1/oauth/cli/poll/{flow_id}` every
//!    `poll_interval_sec` until `status == "ready"` (which yields `{token,
//!    user, zai:{access_token}}`) or `"failed"`, with an overall 5-min cap.
//! 5. Persist `{access_token, jwt, user}` into the shared credential store at
//!    `~/.zcode/v2/credentials.json` (AES-256-GCM encrypted values, see
//!    [`CredentialStore`]).
//!
//! The host uses the persisted `zai.access_token` to drive model requests via
//! the `zai-coding-plan` provider. There is no client-side refresh_token for
//! the `zai` provider.

use std::path::{Path, PathBuf};

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use super::protocol::RpcError;

/// Default ZCode endpoint origin (production).
pub const DEFAULT_ZCODE_ORIGIN: &str = "https://zcode.z.ai";

/// Overall poll timeout, matching the CLI's 5-minute cap.
const POLL_OVERALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
/// Floor for the poll interval.
const POLL_MIN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// Resolve the ZCode endpoint origin from environment overrides.
pub fn resolve_origin(env: &std::collections::HashMap<String, String>) -> String {
    if let Some(v) = env.get("ZCODE_BASE_URL") {
        return v.clone();
    }
    if let Some(v) = env.get("ZCODE_ENDPOINT_ORIGIN") {
        return v.clone();
    }
    match env.get("ZCODE_ENV").map(|s| s.as_str()) {
        Some("test") => env
            .get("ZCODE_TEST_BASE_URL")
            .cloned()
            .unwrap_or_else(|| DEFAULT_ZCODE_ORIGIN.to_string()),
        Some("production") | None => env
            .get("ZCODE_PRODUCTION_BASE_URL")
            .cloned()
            .unwrap_or_else(|| DEFAULT_ZCODE_ORIGIN.to_string()),
        Some(_) => DEFAULT_ZCODE_ORIGIN.to_string(),
    }
}

// ---------------------------------------------------------------------------
// OAuth flow types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct InitData {
    pub flow_id: String,
    pub poll_token: String,
    pub authorize_url: String,
    pub expires_at: u64,
    pub poll_interval_sec: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PollData {
    pub status: String,
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub user: Option<serde_json::Value>,
    #[serde(default)]
    pub zai: Option<ZaiTokens>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ZaiTokens {
    pub access_token: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UserInfo {
    pub user_id: String,
    #[serde(default)]
    pub avatar: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
}

/// Successful login result.
#[derive(Debug, Clone)]
pub struct LoginOutcome {
    pub zai_access_token: String,
    pub zcode_jwt: String,
    pub user: UserInfo,
}

/// Run the Z.AI CLI OAuth init/poll flow.
///
/// `on_authorize_url` is called once with the URL the human must open; the
/// caller surfaces it through the ACP auth flow. `http` is a configured
/// reqwest client (no auth headers — we add them per call).
pub async fn login<F>(
    http: &reqwest::Client,
    origin: &str,
    on_authorize_url: F,
) -> Result<LoginOutcome, AuthError>
where
    F: FnOnce(&str),
{
    let poll_token = random_hex(32);
    let init_url = format!("{origin}/api/v1/oauth/cli/init");

    tracing::info!(%init_url, "oauth: requesting cli/init");
    let init_resp = http
        .post(&init_url)
        .bearer_auth(&poll_token)
        .header("Content-Type", "application/json")
        .body(json!({ "provider": "zai" }).to_string())
        .send()
        .await
        .map_err(|e| AuthError::Http(init_url.clone(), e.to_string()))?;
    let init_status = init_resp.status();
    let init_text = init_resp.text().await.unwrap_or_default();
    if !init_status.is_success() {
        return Err(AuthError::InitFailed(init_status.as_u16(), init_text));
    }
    let init: InitData = serde_json::from_str(&init_text)
        .map_err(|e| AuthError::BadInit(format!("{e}; body={init_text}")))?;

    on_authorize_url(&init.authorize_url);

    // Poll loop.
    let deadline =
        std::time::Instant::now() + POLL_OVERALL_TIMEOUT.min(std::time::Duration::from_millis(
            init.expires_at.saturating_mul(1000),
        ));
    let interval = POLL_MIN_INTERVAL.max(std::time::Duration::from_secs(init.poll_interval_sec));
    let poll_url = format!("{origin}/api/v1/oauth/cli/poll/{}", init.flow_id);

    loop {
        if std::time::Instant::now() >= deadline {
            return Err(AuthError::Timeout);
        }
        tokio::time::sleep(interval).await;

        tracing::debug!(%poll_url, "oauth: polling");
        let resp = http
            .get(&poll_url)
            .bearer_auth(&poll_token)
            .send()
            .await
            .map_err(|e| AuthError::Http(poll_url.clone(), e.to_string()))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            // Transient failures should not abort the whole flow; log and retry.
            tracing::warn!(code = status.as_u16(), %body, "oauth poll non-2xx; retrying");
            continue;
        }
        let data: PollData = match serde_json::from_str(&body) {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(%body, "oauth poll bad JSON: {e}");
                continue;
            }
        };
        match data.status.as_str() {
            "pending" => continue,
            "failed" => return Err(AuthError::Denied),
            "ready" => {
                let zai = data.zai.ok_or(AuthError::Incomplete("missing zai tokens"))?;
                let token = data
                    .token
                    .ok_or(AuthError::Incomplete("missing zcode jwt"))?;
                let user: UserInfo = match data.user {
                    Some(v) => serde_json::from_value::<UserInfo>(v.clone())
                        .map_err(|e| AuthError::BadUser(e.to_string()))?,
                    None => return Err(AuthError::Incomplete("missing user info")),
                };
                return Ok(LoginOutcome {
                    zai_access_token: zai.access_token,
                    zcode_jwt: token,
                    user,
                });
            }
            other => {
                tracing::warn!(status = other, "oauth poll unknown status; retrying");
                continue;
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("HTTP error calling {0}: {1}")]
    Http(String, String),
    #[error("cli/init failed ({0}): {1}")]
    InitFailed(u16, String),
    #[error("cli/init returned bad data: {0}")]
    BadInit(String),
    #[error("authorization timed out")]
    Timeout,
    #[error("authorization was denied or failed")]
    Denied,
    #[error("ready payload incomplete: {0}")]
    Incomplete(&'static str),
    #[error("bad user info: {0}")]
    BadUser(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("credential store error: {0}")]
    Store(String),
}

/// Map an [`AuthError`] to an ACP-style reason string for `authenticate`.
impl AuthError {
    pub fn reason_code(&self) -> &'static str {
        match self {
            AuthError::Timeout => "auth_timeout",
            AuthError::Denied => "auth_denied",
            AuthError::InitFailed(..) | AuthError::BadInit(_) | AuthError::BadUser(_) => {
                "auth_provider_error"
            }
            AuthError::Http(..) => "auth_network_error",
            AuthError::Incomplete(_) => "auth_incomplete",
            AuthError::Io(_) | AuthError::Store(_) => "auth_internal",
        }
    }
}

// ---------------------------------------------------------------------------
// Credential store: ~/.zcode/v2/credentials.json (AES-256-GCM)
// ---------------------------------------------------------------------------

/// Logical keys in the credential store (the `xv` registry in the bundle).
pub mod cred_keys {
    pub const ACTIVE_PROVIDER: &str = "oauth:active_provider";
    pub const ZAI_ACCESS_TOKEN: &str = "oauth:zai:access_token";
    pub const ZAI_USER_INFO: &str = "oauth:zai:user_info";
    pub const ZCODE_JWT: &str = "zcodejwttoken";
}

/// File-backed credential store. Each value is stored as
/// `enc:v1:<iv>.<authtag>.<ciphertext>` (all base64url).
pub struct CredentialStore {
    path: PathBuf,
    secret: Vec<u8>,
}

impl CredentialStore {
    /// Open the store at `<data_base_dir>/.zcode/v2/credentials.json`, using
    /// `ZCODE_CREDENTIAL_SECRET` or the platform-derived fallback for the key.
    pub fn open(data_base_dir: Option<&Path>) -> Self {
        let home = data_base_dir
            .map(|p| p.to_path_buf())
            .or_else(home_dir)
            .unwrap_or_else(|| PathBuf::from("."));
        let path = home.join(".zcode/v2/credentials.json");

        let secret = match std::env::var("ZCODE_CREDENTIAL_SECRET") {
            Ok(s) if !s.is_empty() => s.into_bytes(),
            _ => fallback_secret(),
        };
        Self { path, secret }
    }

    /// Read the current contents (empty map if missing).
    fn read(&self) -> std::io::Result<serde_json::Map<String, serde_json::Value>> {
        match std::fs::read(&self.path) {
            Ok(bytes) => {
                let v: serde_json::Value =
                    serde_json::from_slice(&bytes).unwrap_or_else(|_| json!({}));
                Ok(v.as_object()
                    .cloned()
                    .unwrap_or_else(serde_json::Map::new))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Ok(serde_json::Map::new())
            }
            Err(e) => Err(e),
        }
    }

    /// Atomically write the map back.
    fn write_atomic(&self, map: &serde_json::Map<String, serde_json::Value>) -> std::io::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(&serde_json::Value::Object(map.clone()))?;
        let tmp = self.path.with_extension(format!(
            "tmp.{}.{}",
            std::process::id(),
            random_hex(4)
        ));
        // Write with 0o600 then rename.
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp)?;
            f.write_all(&bytes)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600));
            }
        }
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }

    /// Encrypt and store a value under `key`.
    pub fn put(&self, key: &str, plaintext: &str) -> Result<(), AuthError> {
        let mut map = self.read()?;
        let enc = encrypt(&self.secret, plaintext.as_bytes())?;
        map.insert(key.to_string(), serde_json::Value::String(enc));
        self.write_atomic(&map).map_err(AuthError::Io)
    }

    /// Read and decrypt a value under `key`, if present and well-formed.
    pub fn get(&self, key: &str) -> Result<Option<String>, AuthError> {
        let map = self.read()?;
        match map.get(key) {
            Some(serde_json::Value::String(s)) => {
                let pt = decrypt(&self.secret, s)?;
                String::from_utf8(pt)
                    .map(Some)
                    .map_err(|e| AuthError::Store(format!("non-utf8 plaintext: {e}")))
            }
            Some(_) => Err(AuthError::Store(format!(
                "credential `{key}` is not a string"
            ))),
            None => Ok(None),
        }
    }

    /// Persist a complete login outcome.
    pub fn save_login(&self, outcome: &LoginOutcome) -> Result<(), AuthError> {
        self.put(cred_keys::ACTIVE_PROVIDER, "zai")?;
        self.put(cred_keys::ZAI_ACCESS_TOKEN, &outcome.zai_access_token)?;
        self.put(cred_keys::ZCODE_JWT, &outcome.zcode_jwt)?;
        let user_json = serde_json::to_string(&outcome.user)
            .map_err(|e| AuthError::Store(format!("encode user: {e}")))?;
        self.put(cred_keys::ZAI_USER_INFO, &user_json)?;
        Ok(())
    }

    /// Drop the Z.AI credentials (best-effort). Used by `logout`.
    pub fn clear(&self) -> Result<(), AuthError> {
        let mut map = self.read()?;
        for k in [
            cred_keys::ACTIVE_PROVIDER,
            cred_keys::ZAI_ACCESS_TOKEN,
            cred_keys::ZAI_USER_INFO,
            cred_keys::ZCODE_JWT,
        ] {
            map.remove(k);
        }
        self.write_atomic(&map).map_err(AuthError::Io)
    }
}

// ---------------------------------------------------------------------------
// AES-256-GCM helpers, matching the bundle's `enc:v1:<iv>.<tag>.<ct>` format
// ---------------------------------------------------------------------------

const ENC_PREFIX: &str = "enc:v1:";
const IV_LEN: usize = 12;
const TAG_LEN: usize = 16;

fn derive_key(secret: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(secret);
    let out = h.finalize();
    let mut k = [0u8; 32];
    k.copy_from_slice(&out);
    k
}

fn fallback_secret() -> Vec<u8> {
    let home = home_dir()
        .and_then(|p| p.to_str().map(str::to_string))
        .unwrap_or_default();
    let user = std::env::var("USER").unwrap_or_default();
    let platform = if cfg!(target_os = "macos") {
        "darwin"
    } else if cfg!(target_os = "windows") {
        "win32"
    } else {
        "linux"
    };
    format!("zcode-credential-fallback:{platform}:{home}:{user}").into_bytes()
}

fn encrypt(secret: &[u8], plaintext: &[u8]) -> Result<String, AuthError> {
    let key_bytes = derive_key(secret);
    let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
    let cipher = Aes256Gcm::new(key);

    let mut iv_bytes = [0u8; IV_LEN];
    fill_random(&mut iv_bytes);
    let nonce = Nonce::from_slice(&iv_bytes);

    let ct = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| AuthError::Store(format!("aes encrypt: {e}")))?;

    // aes-gcm appends the 16-byte tag to the ciphertext.
    let (ct_only, tag) = ct.split_at(ct.len() - TAG_LEN);
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    Ok(format!(
        "{ENC_PREFIX}{}.{}.{}",
        b64.encode(iv_bytes),
        b64.encode(tag),
        b64.encode(ct_only),
    ))
}

fn decrypt(secret: &[u8], literal: &str) -> Result<Vec<u8>, AuthError> {
    let rest = literal
        .strip_prefix(ENC_PREFIX)
        .ok_or_else(|| AuthError::Store("missing enc:v1: prefix".into()))?;
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let mut parts = rest.splitn(3, '.');
    let iv_b64 = parts.next().ok_or_else(|| AuthError::Store("bad literal".into()))?;
    let tag_b64 = parts.next().ok_or_else(|| AuthError::Store("bad literal".into()))?;
    let ct_b64 = parts.next().ok_or_else(|| AuthError::Store("bad literal".into()))?;
    let iv = b64.decode(iv_b64).map_err(|e| AuthError::Store(format!("b64 iv: {e}")))?;
    let tag = b64.decode(tag_b64).map_err(|e| AuthError::Store(format!("b64 tag: {e}")))?;
    let ct = b64.decode(ct_b64).map_err(|e| AuthError::Store(format!("b64 ct: {e}")))?;
    if iv.len() != IV_LEN || tag.len() != TAG_LEN {
        return Err(AuthError::Store("iv/tag length mismatch".into()));
    }
    let mut combined = ct;
    combined.extend_from_slice(&tag);

    let key_bytes = derive_key(secret);
    let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
    let cipher = Aes256Gcm::new(key);
    let nonce = Nonce::from_slice(&iv);
    cipher
        .decrypt(nonce, combined.as_ref())
        .map_err(|e| AuthError::Store(format!("aes decrypt: {e}")))
}

// ---------------------------------------------------------------------------
// Misc helpers
// ---------------------------------------------------------------------------

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from).or_else(|| {
        // Best-effort; the bundle uses os.homedir() which also falls back.
        Some(PathBuf::from("/"))
    })
}

fn fill_random(out: &mut [u8]) {
    // Use /dev/urandom on Unix; this is sufficient for IV/secret generation
    // in a single-process CLI context. Fallback to a simple PRNG if missing.
    #[cfg(unix)]
    {
        use std::io::Read;
        if let Ok(mut f) = std::fs::File::open("/dev/urandom")
            && f.read_exact(out).is_ok() {
                return;
            }
    }
    // Fallback: time-seeded xorshift.
    let mut seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0xC0FFEE);
    for byte in out.iter_mut() {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        *byte = (seed & 0xFF) as u8;
    }
}

fn random_hex(n_bytes: usize) -> String {
    let mut buf = vec![0u8; n_bytes];
    fill_random(&mut buf);
    let mut s = String::with_capacity(n_bytes * 2);
    for b in &buf {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Convert a transport-level [`RpcError`] into a best-effort [`AuthError`].
pub fn from_rpc(err: &RpcError) -> AuthError {
    AuthError::Store(format!("rpc error {}: {}", err.code, err.message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aes_roundtrip_matches_bundle_format() {
        let secret = b"some-secret";
        let pt = b"hello world";
        let enc = encrypt(secret, pt).unwrap();
        assert!(enc.starts_with("enc:v1:"), "got: {enc}");
        // Three dot-separated parts after the prefix.
        let rest = &enc[ENC_PREFIX.len()..];
        assert_eq!(rest.split('.').count(), 3);
        let dec = decrypt(secret, &enc).unwrap();
        assert_eq!(dec, pt);
    }

    #[test]
    fn aes_rejects_tampered_ciphertext() {
        let secret = b"k";
        let enc = encrypt(secret, b"secret").unwrap();
        let mut tampered = enc.clone();
        // Flip a char in the ciphertext portion (last segment).
        let last_byte = tampered.len().checked_sub(1).unwrap();
        let b = tampered.remove(last_byte);
        tampered.insert(last_byte, match b {
            'A' => 'B',
            c => char::from((c as u8) ^ 1),
        });
        assert!(decrypt(secret, &tampered).is_err());
    }
}
