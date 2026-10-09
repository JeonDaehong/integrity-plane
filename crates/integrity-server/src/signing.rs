//! The Plane's certificate signing key (RFC 0005): a 32-byte Ed25519 seed in a file.

use std::io::Write as _;
use std::path::Path;

use integrity_core::CertSigner;

/// Loads the seed at `path`, or creates it from the operating system's random source (readable by
/// the owner only, on Unix).
pub fn load_or_create(path: &Path) -> Result<CertSigner, String> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let seed: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
                format!(
                    "{}: a signing key file holds exactly 32 bytes, found {}",
                    path.display(),
                    bytes.len()
                )
            })?;
            Ok(CertSigner::from_seed(&seed))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let mut seed = [0u8; 32];
            getrandom::fill(&mut seed).map_err(|e| format!("no OS randomness: {e}"))?;
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
            }
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt as _;
                options.mode(0o600);
            }
            let mut file = options
                .open(path)
                .map_err(|e| format!("{}: {e}", path.display()))?;
            file.write_all(&seed)
                .and_then(|()| file.sync_all())
                .map_err(|e| format!("{}: {e}", path.display()))?;
            Ok(CertSigner::from_seed(&seed))
        }
        Err(e) => Err(format!("{}: {e}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_once_then_loads_the_same_key() {
        let dir = std::env::temp_dir().join(format!(
            "oip-signing-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos())
        ));
        let path = dir.join("keys").join("signing.key");
        let first = load_or_create(&path).unwrap();
        let again = load_or_create(&path).unwrap();
        assert_eq!(first.public_key(), again.public_key());
        std::fs::write(&path, b"short").unwrap();
        assert!(load_or_create(&path).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
