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
