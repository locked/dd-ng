use anyhow::{anyhow, Context, Result};
use std::io::{BufRead, Write};

pub fn write_line<W: Write>(w: &mut W, s: &str) -> Result<()> {
    w.write_all(s.as_bytes()).context("write_line: write body")?;
    w.write_all(b"\n").context("write_line: write nl")?;
    w.flush().context("write_line: flush")?;
    Ok(())
}

pub fn read_line<R: BufRead>(r: &mut R) -> Result<String> {
    let mut s = String::new();
    let n = r.read_line(&mut s).context("read_line")?;
    if n == 0 {
        return Err(anyhow!("unexpected EOF"));
    }
    if s.ends_with('\n') {
        s.pop();
        if s.ends_with('\r') {
            s.pop();
        }
    }
    Ok(s)
}

pub fn rendezvous_socket_path(token: &str) -> std::path::PathBuf {
    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("/tmp"));
    base.join(format!("dd-ng.{token}.sock"))
}

pub fn compute_ranges(total: u64, n: u32) -> Vec<crate::proto::Range> {
    let n = n as u64;
    let base = total / n;
    let rem = total % n;
    let mut out = Vec::with_capacity(n as usize);
    let mut off = 0u64;
    for i in 0..n {
        let len = base + if i < rem { 1 } else { 0 };
        out.push(crate::proto::Range {
            offset: off,
            length: len,
        });
        off += len;
    }
    out
}

pub fn parse_remote(spec: &str) -> Result<(String, String)> {
    // user@host:/path  OR  host:/path
    let (host, path) = spec
        .split_once(':')
        .ok_or_else(|| anyhow!("remote spec must be [user@]host:/path"))?;
    if host.is_empty() || path.is_empty() {
        return Err(anyhow!("bad remote spec"));
    }
    Ok((host.to_string(), path.to_string()))
}

/// Try to enlarge the pipe capacity for `fd` to `size` bytes via
/// fcntl(F_SETPIPE_SZ). Silently returns Ok(actual_size) on success or if the
/// kernel clamps the value; returns Err on hard failure (e.g. fd is not a pipe).
///
/// On Linux, /proc/sys/fs/pipe-max-size caps the value unless the process is
/// privileged; we ignore EPERM and treat it as a soft failure (Ok(0)).
pub fn try_set_pipe_size(fd: std::os::unix::io::RawFd, size: i32) -> Result<i32> {
    // fcntl(fd, F_SETPIPE_SZ, size) on Linux.
    const F_SETPIPE_SZ: i32 = 1031;
    extern "C" {
        fn fcntl(fd: i32, cmd: i32, arg: i32) -> i32;
    }
    let r = unsafe { fcntl(fd, F_SETPIPE_SZ, size) };
    if r < 0 {
        let e = std::io::Error::last_os_error();
        // EPERM (privileged) or EINVAL (not a pipe): treat as best-effort.
        if matches!(e.raw_os_error(), Some(1) | Some(22)) {
            return Ok(0);
        }
        return Err(anyhow!("fcntl(F_SETPIPE_SZ, {size}) failed: {e}"));
    }
    Ok(r)
}

/// Parse a size string with optional unit suffix.
/// Accepts: plain integer bytes, or a number (integer or decimal) followed by
/// a unit. Units are case-insensitive; both SI (K=1000) and IEC (Ki=1024) forms
/// are accepted, plus the common short-form (K/M/G/T = 1024-based, matching
/// `dd`, `fio`, and typical sysadmin usage).
///
/// Examples: "4194304", "4M", "16M", "1.5G", "512K", "2GiB", "1000KB"
pub fn parse_size(s: &str) -> Result<u64> {
    let s = s.trim();
    if s.is_empty() {
        return Err(anyhow!("empty size"));
    }
    // Split into numeric prefix and unit suffix.
    let split_at = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '_'))
        .unwrap_or(s.len());
    let (num_str, unit_str) = s.split_at(split_at);
    let num_str = num_str.replace('_', "");
    if num_str.is_empty() {
        return Err(anyhow!("size {s:?} has no numeric part"));
    }
    let num: f64 = num_str
        .parse()
        .map_err(|_| anyhow!("bad number in size {s:?}"))?;
    if !num.is_finite() || num < 0.0 {
        return Err(anyhow!("size must be non-negative and finite"));
    }
    let unit = unit_str.trim().to_ascii_lowercase();
    // Strip an optional trailing 'b' (bytes) so "KB", "KiB", "MB" all work.
    let unit = unit.strip_suffix('b').unwrap_or(&unit).to_string();
    let mult: u64 = match unit.as_str() {
        "" => 1,
        "k" | "ki" => 1u64 << 10,
        "m" | "mi" => 1u64 << 20,
        "g" | "gi" => 1u64 << 30,
        "t" | "ti" => 1u64 << 40,
        "p" | "pi" => 1u64 << 50,
        other => return Err(anyhow!("unknown size unit {other:?} in {s:?}")),
    };
    let bytes = num * (mult as f64);
    if bytes > (u64::MAX as f64) {
        return Err(anyhow!("size {s:?} too large"));
    }
    Ok(bytes as u64)
}
