//! Pair configuration: the two replica roots, their IDs and per-replica symlink
//! policy flags (design §2, §4).
//!
//! The config lives at `$XDG_STATE_HOME/fsync/<pair>/config.toml`, outside both
//! replica roots. The functions here take the state home explicitly so tests do
//! not depend on the process environment; [`state_home`] reads it from the
//! environment for the CLI.

use std::ffi::OsString;
use std::fmt;
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// File name of the pair config inside the pair's state directory.
pub const CONFIG_FILE: &str = "config.toml";

/// Identifies one replica of a pair. Random and non-zero; written as 16 hex
/// digits in the config file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ReplicaId(pub u64);

impl ReplicaId {
    /// Generates a random, non-zero ID from the kernel CSPRNG.
    pub fn random() -> Result<Self> {
        loop {
            let mut buf = [0u8; 8];
            match rustix::rand::getrandom(&mut buf, rustix::rand::GetRandomFlags::empty()) {
                Ok(n) if n == buf.len() => {
                    let id = u64::from_le_bytes(buf);
                    if id != 0 {
                        return Ok(ReplicaId(id));
                    }
                }
                Ok(_) | Err(rustix::io::Errno::INTR) => {}
                Err(e) => return Err(Error::io("getrandom", e.into())),
            }
        }
    }
}

impl fmt::Display for ReplicaId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl FromStr for ReplicaId {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        u64::from_str_radix(s, 16).map(ReplicaId)
    }
}

/// How a replica treats symlinks, named after the matching rsync option
/// (design §4.3). [`crate::symlink::classify`] applies it to one link.
///
/// "Unsafe" is rsync's lexical check (absolute, or leaving the root through
/// `..`; [`crate::symlink::is_unsafe`]), applied to the canonical (unmunged)
/// target. `--munge-links` and `-K` are separate per-replica flags on
/// [`ReplicaConfig`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SymlinkPolicy {
    /// No `-l`: every symlink is `Unmanaged(IgnoredLink)`; incoming changes to
    /// the path are skipped.
    Skip,
    /// `-l` (implied by `-a`): symlinks are synced as symlinks, target bytes
    /// verbatim, dangling allowed.
    #[default]
    Links,
    /// `-L`: every symlink is indexed as its referent (File or Dir); a
    /// dangling one is `Unmanaged(Dangling)`.
    CopyLinks,
    /// `--copy-unsafe-links`: unsafe symlinks as with `-L`, safe ones as with `-l`.
    CopyUnsafeLinks,
    /// `--safe-links`: unsafe symlinks are `Unmanaged(IgnoredLink)`, safe ones
    /// are synced as symlinks.
    SafeLinks,
    /// `-k`: symlinks to directories are indexed as the directory; all other
    /// symlinks (dangling ones included) as with `-l`.
    CopyDirlinks,
}

/// One side of a pair.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaConfig {
    #[serde(with = "replica_id_hex")]
    pub id: ReplicaId,
    /// Absolute path of the replica root.
    pub root: PathBuf,
    #[serde(default)]
    pub symlinks: SymlinkPolicy,
    /// rsync `--munge-links` (design §4.4).
    #[serde(default)]
    pub munge_links: bool,
    /// rsync `-K` / `--keep-dirlinks`.
    #[serde(default)]
    pub keep_dirlinks: bool,
}

impl ReplicaConfig {
    /// A replica with a fresh random ID and default symlink settings.
    pub fn new(root: PathBuf) -> Result<Self> {
        Ok(ReplicaConfig {
            id: ReplicaId::random()?,
            root,
            symlinks: SymlinkPolicy::default(),
            munge_links: false,
            keep_dirlinks: false,
        })
    }
}

/// A sync pair: two replicas kept in sync with each other.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PairConfig {
    pub name: String,
    pub replicas: [ReplicaConfig; 2],
}

impl PairConfig {
    /// A new pair over two absolute roots, with distinct random replica IDs.
    pub fn new(name: &str, roots: [PathBuf; 2]) -> Result<Self> {
        validate_pair_name(name)?;
        let [a, b] = roots;
        let a = ReplicaConfig::new(a)?;
        let mut b = ReplicaConfig::new(b)?;
        while b.id == a.id {
            b.id = ReplicaId::random()?;
        }
        let cfg = PairConfig {
            name: name.to_owned(),
            replicas: [a, b],
        };
        cfg.check().map_err(|reason| Error::InvalidRoot {
            path: cfg.replicas[0].root.clone(),
            reason,
        })?;
        Ok(cfg)
    }

    /// Loads the config of `pair` from its state directory.
    pub fn load(state_home: &Path, pair: &str) -> Result<Self> {
        let path = config_path(state_home, pair)?;
        let cfg = Self::load_from(&path)?;
        if cfg.name != pair {
            return Err(Error::InvalidConfig {
                path,
                reason: format!("name {:?} does not match pair {pair:?}", cfg.name),
            });
        }
        Ok(cfg)
    }

    /// Loads and validates a config file.
    pub fn load_from(path: &Path) -> Result<Self> {
        let text = match fs::read_to_string(path) {
            Ok(t) => t,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Err(Error::ConfigNotFound {
                    path: path.to_owned(),
                });
            }
            Err(e) => return Err(Error::io(format!("read {}", path.display()), e)),
        };
        let cfg: PairConfig = toml::from_str(&text).map_err(|source| Error::ConfigParse {
            path: path.to_owned(),
            source,
        })?;
        validate_pair_name(&cfg.name)
            .map_err(|e| e.to_string())
            .and_then(|()| cfg.check())
            .map_err(|reason| Error::InvalidConfig {
                path: path.to_owned(),
                reason,
            })?;
        Ok(cfg)
    }

    /// Writes the config for a new pair. Fails with [`Error::ConfigExists`]
    /// instead of overwriting. Returns the config file path.
    pub fn create(&self, state_home: &Path) -> Result<PathBuf> {
        let path = config_path(state_home, &self.name)?;
        write_atomic(&path, self.to_toml()?.as_bytes(), false)?;
        Ok(path)
    }

    /// Writes the config, atomically replacing any existing file. Returns the
    /// config file path.
    pub fn save(&self, state_home: &Path) -> Result<PathBuf> {
        let path = config_path(state_home, &self.name)?;
        write_atomic(&path, self.to_toml()?.as_bytes(), true)?;
        Ok(path)
    }

    fn to_toml(&self) -> Result<String> {
        Ok(toml::to_string_pretty(self)?)
    }

    /// Checks the invariants that do not depend on the filesystem.
    fn check(&self) -> Result<(), String> {
        let [a, b] = &self.replicas;
        for r in [a, b] {
            if r.id.0 == 0 {
                return Err("replica id must be non-zero".into());
            }
            if !r.root.is_absolute() {
                return Err(format!("root {} is not absolute", r.root.display()));
            }
        }
        if a.id == b.id {
            return Err(format!("both replicas have id {}", a.id));
        }
        if a.root.starts_with(&b.root) || b.root.starts_with(&a.root) {
            return Err(format!(
                "roots {} and {} overlap",
                a.root.display(),
                b.root.display()
            ));
        }
        Ok(())
    }
}

/// Creates and saves a new pair: canonicalises both roots, checks they are
/// directories that neither overlap each other nor contain the state
/// directory, and generates replica IDs. Returns the config and its path.
pub fn init(state_home: &Path, pair: &str, a: &Path, b: &Path) -> Result<(PairConfig, PathBuf)> {
    validate_pair_name(pair)?;
    let roots = [canonical_root(a)?, canonical_root(b)?];
    // Compare against the canonical state directory so a symlinked state home
    // cannot hide that it lies inside a root.
    let state_home = fs::canonicalize(state_home).unwrap_or_else(|_| state_home.to_owned());
    let state_dir = pair_dir(&state_home, pair)?;
    for root in &roots {
        if state_dir.starts_with(root) {
            return Err(Error::InvalidRoot {
                path: root.clone(),
                reason: format!("contains the state directory {}", state_dir.display()),
            });
        }
    }
    let cfg = PairConfig::new(pair, roots)?;
    let path = cfg.create(&state_home)?;
    Ok((cfg, path))
}

fn canonical_root(path: &Path) -> Result<PathBuf> {
    let invalid = |reason: String| Error::InvalidRoot {
        path: path.to_owned(),
        reason,
    };
    let root = fs::canonicalize(path).map_err(|e| invalid(e.to_string()))?;
    let meta = fs::metadata(&root).map_err(|e| invalid(e.to_string()))?;
    if !meta.is_dir() {
        return Err(invalid("not a directory".into()));
    }
    if root.to_str().is_none() {
        return Err(invalid(
            "path is not valid UTF-8, which the config file cannot store yet".into(),
        ));
    }
    Ok(root)
}

/// The base state directory: `$XDG_STATE_HOME`, else `$HOME/.local/state`.
pub fn state_home() -> Result<PathBuf> {
    state_home_from(std::env::var_os("XDG_STATE_HOME"), std::env::var_os("HOME"))
}

/// [`state_home`] with explicit environment values. Relative values are
/// ignored, as the XDG Base Directory spec requires.
pub fn state_home_from(
    xdg_state_home: Option<OsString>,
    home: Option<OsString>,
) -> Result<PathBuf> {
    let absolute = |v: Option<OsString>| v.map(PathBuf::from).filter(|p| p.is_absolute());
    if let Some(dir) = absolute(xdg_state_home) {
        return Ok(dir);
    }
    absolute(home)
        .map(|h| h.join(".local/state"))
        .ok_or(Error::NoStateHome)
}

/// `<state_home>/fsync/<pair>`: the pair's index, journal and config.
pub fn pair_dir(state_home: &Path, pair: &str) -> Result<PathBuf> {
    validate_pair_name(pair)?;
    Ok(state_home.join("fsync").join(pair))
}

/// `<state_home>/fsync/<pair>/config.toml`.
pub fn config_path(state_home: &Path, pair: &str) -> Result<PathBuf> {
    Ok(pair_dir(state_home, pair)?.join(CONFIG_FILE))
}

/// Pair names become a directory name, so they are restricted to
/// `[A-Za-z0-9._-]`, at most 64 bytes, not starting with `.`.
pub fn validate_pair_name(name: &str) -> Result<()> {
    let err = |reason| {
        Err(Error::InvalidPairName {
            name: name.to_owned(),
            reason,
        })
    };
    if name.is_empty() {
        return err("must not be empty");
    }
    if name.len() > 64 {
        return err("must be at most 64 bytes");
    }
    if name.starts_with('.') {
        return err("must not start with '.'");
    }
    if !name
        .bytes()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-'))
    {
        return err("may only contain ASCII letters, digits, '.', '_' and '-'");
    }
    Ok(())
}

/// Writes `bytes` to `path` via a fsynced temp file in the same directory.
/// With `replace`, an existing file is atomically replaced; without it, an
/// existing file makes the call fail with [`Error::ConfigExists`].
fn write_atomic(path: &Path, bytes: &[u8], replace: bool) -> Result<()> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let dir = path.parent().expect("config path has a parent");
    DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .map_err(|e| Error::io(format!("create {}", dir.display()), e))?;

    let tmp = dir.join(format!(
        ".{CONFIG_FILE}.tmp.{}.{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| {
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)
            .map_err(|e| Error::io(format!("create {}", tmp.display()), e))?;
        f.write_all(bytes)
            .and_then(|()| f.sync_all())
            .map_err(|e| Error::io(format!("write {}", tmp.display()), e))?;
        if replace {
            fs::rename(&tmp, path)
        } else {
            // link(2) refuses to replace an existing name, unlike rename(2).
            fs::hard_link(&tmp, path)
        }
        .map_err(|e| match e.kind() {
            io::ErrorKind::AlreadyExists => Error::ConfigExists {
                path: path.to_owned(),
            },
            _ => Error::io(format!("install {}", path.display()), e),
        })
    })();
    // After a rename the temp name is already gone; ignore that error.
    let _ = fs::remove_file(&tmp);
    result?;
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| Error::io(format!("fsync {}", dir.display()), e))
}

/// Serde helper: a [`ReplicaId`] as a 16-digit hex string. TOML integers are
/// signed 64-bit, so a raw `u64` would not round-trip.
mod replica_id_hex {
    use super::ReplicaId;
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    pub fn serialize<S: Serializer>(id: &ReplicaId, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(id)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<ReplicaId, D::Error> {
        let s = String::deserialize(d)?;
        if s.len() != 16 {
            return Err(D::Error::custom(format!(
                "replica id {s:?} must be 16 hex digits"
            )));
        }
        s.parse()
            .map_err(|e| D::Error::custom(format!("replica id {s:?}: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tempdir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn state_home_resolution() {
        let p = |s: &str| Some(OsString::from(s));
        assert_eq!(
            state_home_from(p("/x/state"), p("/home/u")).unwrap(),
            PathBuf::from("/x/state")
        );
        assert_eq!(
            state_home_from(None, p("/home/u")).unwrap(),
            PathBuf::from("/home/u/.local/state")
        );
        assert_eq!(
            state_home_from(p("rel"), p("/home/u")).unwrap(),
            PathBuf::from("/home/u/.local/state")
        );
        assert!(matches!(
            state_home_from(None, None),
            Err(Error::NoStateHome)
        ));
        assert!(matches!(
            state_home_from(p("rel"), p("rel")),
            Err(Error::NoStateHome)
        ));
    }

    #[test]
    fn pair_names() {
        for ok in ["docs", "a.b-c_d", "X1", &"a".repeat(64)] {
            validate_pair_name(ok).unwrap();
        }
        for bad in ["", ".", "..", ".hidden", "a/b", "a b", "ä", &"a".repeat(65)] {
            assert!(validate_pair_name(bad).is_err(), "{bad:?} accepted");
        }
    }

    #[test]
    fn replica_id_hex_format() {
        let id = ReplicaId(0x00ab_cdef_0123_4567);
        assert_eq!(id.to_string(), "00abcdef01234567");
        assert_eq!("00abcdef01234567".parse::<ReplicaId>().unwrap(), id);
        assert_eq!(ReplicaId(u64::MAX).to_string(), "ffffffffffffffff");
    }

    #[test]
    fn random_ids_are_nonzero_and_vary() {
        let a = ReplicaId::random().unwrap();
        let b = ReplicaId::random().unwrap();
        assert_ne!(a.0, 0);
        assert_ne!(a, b);
    }

    #[test]
    fn init_round_trips() {
        let (state, ra, rb) = (tempdir(), tempdir(), tempdir());
        let (cfg, path) = init(state.path(), "docs", ra.path(), rb.path()).unwrap();
        assert!(path.ends_with("fsync/docs/config.toml"));
        assert_ne!(cfg.replicas[0].id, cfg.replicas[1].id);
        assert_eq!(cfg.replicas[0].root, fs::canonicalize(ra.path()).unwrap());
        assert_eq!(cfg.replicas[0].symlinks, SymlinkPolicy::Links);

        let loaded = PairConfig::load(state.path(), "docs").unwrap();
        assert_eq!(loaded, cfg);
        assert_eq!(PairConfig::load_from(&path).unwrap(), cfg);

        // A u64 with the top bit set must survive the round trip.
        let mut big = cfg.clone();
        big.replicas[0].id = ReplicaId(u64::MAX);
        big.replicas[1].symlinks = SymlinkPolicy::CopyUnsafeLinks;
        big.replicas[1].munge_links = true;
        big.save(state.path()).unwrap();
        assert_eq!(PairConfig::load(state.path(), "docs").unwrap(), big);

        // No temp files are left behind.
        let names: Vec<_> = fs::read_dir(path.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, [OsString::from(CONFIG_FILE)]);
    }

    #[test]
    fn init_refuses_existing_pair() {
        let (state, ra, rb) = (tempdir(), tempdir(), tempdir());
        let (cfg, _) = init(state.path(), "p", ra.path(), rb.path()).unwrap();
        let err = init(state.path(), "p", ra.path(), rb.path()).unwrap_err();
        assert!(matches!(err, Error::ConfigExists { .. }), "{err}");
        assert_eq!(PairConfig::load(state.path(), "p").unwrap(), cfg);
    }

    #[test]
    fn init_rejects_bad_roots() {
        let (state, ra) = (tempdir(), tempdir());
        let nested = ra.path().join("sub");
        fs::create_dir(&nested).unwrap();
        let file = ra.path().join("file");
        fs::write(&file, b"x").unwrap();

        let cases: [(&Path, &Path); 4] = [
            (ra.path(), ra.path()),
            (ra.path(), &nested),
            (ra.path(), &file),
            (ra.path(), Path::new("/nonexistent/fsync-test")),
        ];
        for (a, b) in cases {
            let err = init(state.path(), "p", a, b).unwrap_err();
            assert!(
                matches!(err, Error::InvalidRoot { .. }),
                "{a:?} {b:?}: {err}"
            );
        }
        // State directory inside a replica root.
        let rb = tempdir();
        let err = init(ra.path(), "p", rb.path(), ra.path()).unwrap_err();
        assert!(matches!(err, Error::InvalidRoot { .. }), "{err}");
        assert!(!ra.path().join("fsync").exists());
    }

    #[test]
    fn load_rejects_invalid_configs() {
        let state = tempdir();
        let path = config_path(state.path(), "p").unwrap();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let replica =
            |id: &str, root: &str| format!("[[replicas]]\nid = \"{id}\"\nroot = \"{root}\"\n");
        let cases = [
            // Same ID twice.
            format!(
                "name = \"p\"\n{}{}",
                replica("0000000000000001", "/a"),
                replica("0000000000000001", "/b")
            ),
            // Zero ID.
            format!(
                "name = \"p\"\n{}{}",
                replica("0000000000000000", "/a"),
                replica("0000000000000001", "/b")
            ),
            // Relative root.
            format!(
                "name = \"p\"\n{}{}",
                replica("0000000000000001", "a"),
                replica("0000000000000002", "/b")
            ),
            // Nested roots.
            format!(
                "name = \"p\"\n{}{}",
                replica("0000000000000001", "/a"),
                replica("0000000000000002", "/a/b")
            ),
            // Bad pair name.
            format!(
                "name = \"../p\"\n{}{}",
                replica("0000000000000001", "/a"),
                replica("0000000000000002", "/b")
            ),
        ];
        for text in &cases {
            fs::write(&path, text).unwrap();
            let err = PairConfig::load_from(&path).unwrap_err();
            assert!(matches!(err, Error::InvalidConfig { .. }), "{text}\n{err}");
        }
        let parse_errors = [
            // Only one replica.
            format!("name = \"p\"\n{}", replica("0000000000000001", "/a")),
            // Short ID.
            format!(
                "name = \"p\"\n{}{}",
                replica("1", "/a"),
                replica("0000000000000002", "/b")
            ),
            // Unknown field.
            format!(
                "name = \"p\"\nextra = 1\n{}{}",
                replica("0000000000000001", "/a"),
                replica("0000000000000002", "/b")
            ),
            // Unknown policy.
            format!(
                "name = \"p\"\n{}symlinks = \"follow\"\n{}",
                replica("0000000000000001", "/a"),
                replica("0000000000000002", "/b")
            ),
        ];
        for text in &parse_errors {
            fs::write(&path, text).unwrap();
            let err = PairConfig::load_from(&path).unwrap_err();
            assert!(matches!(err, Error::ConfigParse { .. }), "{text}\n{err}");
        }
        // Name that does not match the directory.
        fs::write(
            &path,
            format!(
                "name = \"q\"\n{}{}",
                replica("0000000000000001", "/a"),
                replica("0000000000000002", "/b")
            ),
        )
        .unwrap();
        assert!(matches!(
            PairConfig::load(state.path(), "p"),
            Err(Error::InvalidConfig { .. })
        ));
        // Missing pair.
        assert!(matches!(
            PairConfig::load(state.path(), "none"),
            Err(Error::ConfigNotFound { .. })
        ));
    }
}
