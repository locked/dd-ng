use crate::proto::{
    encode_frame_hdr, CtrlMsg, Manifest, Mode, Range, MAGIC,
};
use crate::util::{compute_ranges, parse_remote, read_line, write_line};
use anyhow::{anyhow, bail, Context, Result};
use rand::RngCore;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

pub struct SendOpts {
    pub input: String, // "-" for stdin (Mode B)
    pub remote: String,
    pub streams: u32,
    pub block_size: u64,
    pub remote_bin: String,
    pub ssh_bin: String,
    pub extra_ssh: Vec<String>,
    pub stream_delay_ms: u64,
    pub no_ssh_defaults: bool,
    /// If 0, no live progress. Otherwise interval in ms between updates.
    pub progress_ms: u64,
    pub sync: bool,
    pub verbose: u8,
    pub direct: bool,
}

#[derive(Clone)]
struct SshCtx {
    ssh_bin: String,
    remote_bin: String,
    host: String,
    extra: Vec<String>,
    no_defaults: bool,
    verbose: u8,
}

fn default_ssh_opts() -> &'static [&'static str] {
    &[
        "-o", "ControlMaster=no",
        "-o", "ControlPath=none",
        "-o", "Compression=no",
        "-o", "ServerAliveInterval=30",
        "-o", "ServerAliveCountMax=6",
    ]
}

impl SshCtx {
    fn cmd(&self, remote_args: &[&str]) -> Command {
        let mut c = Command::new(&self.ssh_bin);
        if !self.no_defaults {
            for a in default_ssh_opts() {
                c.arg(a);
            }
        }
        for a in &self.extra {
            c.arg(a);
        }
        c.arg(&self.host).arg(&self.remote_bin);
        for a in remote_args {
            c.arg(a);
        }
        if self.verbose >= 1 {
            eprintln!("[send] exec: {}", format_cmd(&c));
        }
        c
    }
}

fn format_cmd(c: &Command) -> String {
    let mut parts: Vec<String> = Vec::new();
    parts.push(shell_quote(&c.get_program().to_string_lossy()));
    for a in c.get_args() {
        parts.push(shell_quote(&a.to_string_lossy()));
    }
    parts.join(" ")
}

fn shell_quote(s: &str) -> String {
    if s.is_empty() {
        return "''".to_string();
    }
    let safe = s
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"@%+=:,./-_".contains(&b));
    if safe {
        s.to_string()
    } else {
        format!("'{}'", s.replace('\'', "'\\''"))
    }
}

// --------- live progress ---------

struct Progress {
    bytes: AtomicU64,
    total: u64, // 0 if unknown
    done: AtomicBool,
    start: std::time::Instant,
}

impl Progress {
    fn new(total: u64) -> Arc<Self> {
        Arc::new(Self {
            bytes: AtomicU64::new(0),
            total,
            done: AtomicBool::new(false),
            start: std::time::Instant::now(),
        })
    }
    fn add(&self, n: u64) {
        self.bytes.fetch_add(n, Ordering::Relaxed);
    }
    fn stop(&self) {
        self.done.store(true, Ordering::Relaxed);
    }
}

fn fmt_bytes(mut n: f64) -> String {
    const U: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut i = 0;
    while n >= 1024.0 && i < U.len() - 1 {
        n /= 1024.0;
        i += 1;
    }
    format!("{:.2} {}", n, U[i])
}

fn spawn_progress(p: Arc<Progress>, interval_ms: u64) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let interval = std::time::Duration::from_millis(interval_ms);
        let mut last_bytes = 0u64;
        let mut last_t = std::time::Instant::now();
        let is_tty = unsafe { libc_isatty_stderr() };
        while !p.done.load(Ordering::Relaxed) {
            std::thread::sleep(interval);
            let now = std::time::Instant::now();
            let cur = p.bytes.load(Ordering::Relaxed);
            let dt = now.duration_since(last_t).as_secs_f64().max(1e-9);
            let inst = (cur - last_bytes) as f64 / dt;
            let avg = cur as f64 / now.duration_since(p.start).as_secs_f64().max(1e-9);
            let msg = if p.total > 0 {
                let pct = (cur as f64) * 100.0 / (p.total as f64);
                let eta = if inst > 0.0 {
                    ((p.total - cur.min(p.total)) as f64 / inst) as u64
                } else {
                    0
                };
                format!(
                    "[send] {}/{} ({:.1}%)  cur {}/s  avg {}/s  eta {}s",
                    fmt_bytes(cur as f64),
                    fmt_bytes(p.total as f64),
                    pct,
                    fmt_bytes(inst),
                    fmt_bytes(avg),
                    eta
                )
            } else {
                format!(
                    "[send] {} sent  cur {}/s  avg {}/s",
                    fmt_bytes(cur as f64),
                    fmt_bytes(inst),
                    fmt_bytes(avg)
                )
            };
            if is_tty {
                eprint!("\r\x1b[2K{}", msg);
                use std::io::Write as _;
                let _ = std::io::stderr().flush();
            } else {
                eprintln!("{}", msg);
            }
            last_bytes = cur;
            last_t = now;
        }
        if is_tty {
            eprintln!();
        }
    })
}

// Tiny direct isatty(2) via libc symbol lookup, avoiding a new dep.
unsafe fn libc_isatty_stderr() -> bool {
    extern "C" {
        fn isatty(fd: i32) -> i32;
    }
    isatty(2) == 1
}

pub fn run(opts: SendOpts) -> Result<()> {
    let (host, remote_path) = parse_remote(&opts.remote)?;

    let (mode, total_size, ranges) = if opts.input == "-" {
        if opts.direct {
            bail!("--direct requires a seekable input (regular file or block device), not stdin");
        }
        (Mode::Framed, 0u64, Vec::new())
    } else {
        let meta = std::fs::metadata(&opts.input)
            .with_context(|| format!("stat {}", opts.input))?;
        use std::os::unix::fs::FileTypeExt;
        let ft = meta.file_type();
        let total = if ft.is_file() {
            meta.len()
        } else if ft.is_block_device() {
            // Block devices report size 0 via stat(); use lseek(SEEK_END).
            let f = File::open(&opts.input)
                .with_context(|| format!("open {}", opts.input))?;
            use std::io::Seek;
            (&f).seek(std::io::SeekFrom::End(0))
                .with_context(|| format!("seek end {}", opts.input))?
        } else {
            bail!("input must be a regular file, block device, or '-' for stdin");
        };
        if opts.direct {
            const A: u64 = 4096;
            if total % A != 0 {
                bail!(
                    "--direct requires total size ({total}) to be a multiple of {A}"
                );
            }
            if opts.block_size % A != 0 {
                bail!("--direct requires --block-size to be a multiple of {A}");
            }
            (Mode::Range, total, compute_ranges_aligned(total, opts.streams, A))
        } else {
            (Mode::Range, total, compute_ranges(total, opts.streams))
        }
    };

    let mut tok = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut tok);
    let token = tok.iter().map(|b| format!("{b:02x}")).collect::<String>();

    let manifest = Manifest {
        magic: MAGIC.to_string(),
        token: token.clone(),
        mode,
        n_streams: opts.streams,
        total_size,
        block_size: opts.block_size,
        output_path: remote_path.clone(),
        sync: opts.sync,
        direct: opts.direct,
        ranges: ranges.clone(),
    };

    let ssh = SshCtx {
        ssh_bin: opts.ssh_bin.clone(),
        remote_bin: opts.remote_bin.clone(),
        host: host.clone(),
        extra: opts.extra_ssh.clone(),
        no_defaults: opts.no_ssh_defaults,
        verbose: opts.verbose,
    };

    eprintln!(
        "[send] mode={:?} {} -> {}  size={}  streams={}",
        mode,
        if opts.input == "-" { "<stdin>" } else { &opts.input },
        opts.remote,
        if mode == Mode::Range {
            total_size.to_string()
        } else {
            "?".into()
        },
        opts.streams
    );

    // --- ctrl SSH ---
    let mut ctrl = ssh
        .cmd(&["recv-ctrl"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .context("spawn ssh ctrl")?;

    write_line(
        ctrl.stdin.as_mut().unwrap(),
        &serde_json::to_string(&manifest)?,
    )
    .context("sending manifest to remote ctrl")?;
    if opts.verbose >= 2 {
        eprintln!("[ctrl>] Manifest {}", serde_json::to_string(&manifest)?);
    }

    let mut ctrl_out = BufReader::new(ctrl.stdout.take().unwrap());
    let ready_line = read_line(&mut ctrl_out).context("reading Ready from remote ctrl")?;
    if opts.verbose >= 2 {
        eprintln!("[ctrl<] {}", ready_line);
    }
    match serde_json::from_str::<CtrlMsg>(&ready_line)? {
        CtrlMsg::Ready => {}
        CtrlMsg::Abort { reason } => bail!("remote aborted: {reason}"),
        other => bail!("unexpected ctrl msg: {other:?}"),
    }
    eprintln!("[send] remote ready");

    let start = std::time::Instant::now();
    let progress = Progress::new(total_size);
    let prog_handle = if opts.progress_ms > 0 {
        Some(spawn_progress(progress.clone(), opts.progress_ms))
    } else {
        None
    };

    let (total_sent, stream_crcs) = match mode {
        Mode::Range => run_mode_range(&opts, &ssh, &token, &ranges, &progress)?,
        Mode::Framed => run_mode_framed(&opts, &ssh, &token, &progress)?,
    };

    progress.stop();
    if let Some(h) = prog_handle {
        let _ = h.join();
    }

    if opts.sync {
        eprintln!("[send] all bytes sent; waiting for remote fsync...");
    } else {
        eprintln!("[send] all bytes sent; waiting for remote finalize...");
    }

    let report = CtrlMsg::SenderReport {
        stream_crcs,
        total_bytes: total_sent,
    };
    let report_json = serde_json::to_string(&report)?;
    let write_res = write_line(ctrl.stdin.as_mut().unwrap(), &report_json)
        .context("sending SenderReport to remote ctrl");
    if opts.verbose >= 2 {
        eprintln!("[ctrl>] {}", report_json);
    }

    // Whether the write succeeded or not, try to read the remote's verdict.
    // If the remote already aborted (and closed its stdin, causing EPIPE),
    // ctrl_out will still contain the Abort message.
    let final_line = read_line(&mut ctrl_out).context("reading final ctrl msg");
    if opts.verbose >= 2 {
        if let Ok(l) = &final_line {
            eprintln!("[ctrl<] {}", l);
        }
    }

    if let Err(e) = &write_res {
        // If we can read a proper Abort, surface that instead of raw EPIPE.
        if let Ok(line) = &final_line {
            if let Ok(msg) = serde_json::from_str::<CtrlMsg>(line) {
                match msg {
                    CtrlMsg::Abort { reason } => bail!("remote aborted: {reason}"),
                    CtrlMsg::Done { bytes } => {
                        // Benign race: local ssh closed our stdin fd after the
                        // remote had already read SenderReport and exited. As
                        // long as the byte count matches, the transfer is fine.
                        if mode == Mode::Range && bytes != total_size {
                            bail!(
                                "size mismatch after ctrl write error ({e}): \
                                 ack {bytes}, expected {total_size}"
                            );
                        }
                        let el = start.elapsed().as_secs_f64();
                        eprintln!(
                            "[send] done: {} bytes in {:.2}s ({:.1} MB/s)",
                            bytes,
                            el,
                            (bytes as f64) / 1e6 / el.max(1e-9)
                        );
                        let _ = ctrl.wait();
                        return Ok(());
                    }
                    other => bail!("unexpected ctrl msg after write failure: {other:?}"),
                }
            }
        }
        // No useful message from remote; surface the write error with context.
        return Err(anyhow!("{e}"));
    }

    let final_line = final_line?;
    match serde_json::from_str::<CtrlMsg>(&final_line)? {
        CtrlMsg::Done { bytes } => {
            let el = start.elapsed().as_secs_f64();
            eprintln!(
                "[send] done: {} bytes in {:.2}s ({:.1} MB/s)",
                bytes,
                el,
                (bytes as f64) / 1e6 / el.max(1e-9)
            );
            if mode == Mode::Range && bytes != total_size {
                bail!("size mismatch: ack {bytes}, expected {total_size}");
            }
        }
        CtrlMsg::Abort { reason } => bail!("remote aborted: {reason}"),
        other => bail!("unexpected ctrl msg: {other:?}"),
    }
    let status = ctrl.wait()?;
    if !status.success() {
        bail!("ctrl exited {status}");
    }
    Ok(())
}

// ------------------ MODE A: fixed ranges, per-stream CRC32C ------------------

fn run_mode_range(
    opts: &SendOpts,
    ssh: &SshCtx,
    token: &str,
    ranges: &[Range],
    progress: &Arc<Progress>,
) -> Result<(u64, Vec<u32>)> {
    let input_path = PathBuf::from(&opts.input);
    let mut handles = Vec::with_capacity(ranges.len());

    for (i, r) in ranges.iter().enumerate() {
        if i > 0 && opts.stream_delay_ms > 0 {
            thread::sleep(std::time::Duration::from_millis(opts.stream_delay_ms));
        }
        let ssh = ssh.clone();
        let token = token.to_string();
        let range = r.clone();
        let bs = opts.block_size as usize;
        let id = i as u32;
        let input_path = input_path.clone();
        let progress = progress.clone();

        let h = thread::spawn(move || -> Result<(u64, u32)> {
            let id_str = id.to_string();
            let mut child = ssh
                .cmd(&["recv-data", "--token", &token, "--id", &id_str])
                .stdin(Stdio::piped())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .spawn()
                .context("spawn ssh data")?;

            let f = File::open(&input_path)
                .with_context(|| format!("open {}", input_path.display()))?;
            let mut buf = vec![0u8; bs];
            let mut written = 0u64;
            let mut crc = 0u32;
            let mut sink = child.stdin.take().unwrap();

            while written < range.length {
                let want =
                    std::cmp::min(buf.len() as u64, range.length - written) as usize;
                let n = f
                    .read_at(&mut buf[..want], range.offset + written)
                    .with_context(|| format!("pread @{}", range.offset + written))?;
                if n == 0 {
                    bail!("short read stream {id}");
                }
                crc = crc32c::crc32c_append(crc, &buf[..n]);
                sink.write_all(&buf[..n]).map_err(|e| ssh_write_err(id, e))?;
                written += n as u64;
                progress.add(n as u64);
            }
            drop(sink);
            let status = child.wait()?;
            if !status.success() {
                bail!("ssh data stream {id} exited {status}");
            }
            Ok((written, crc))
        });
        handles.push(h);
    }

    let mut total = 0u64;
    let mut crcs = vec![0u32; ranges.len()];
    for (i, h) in handles.into_iter().enumerate() {
        let (b, c) = h.join().map_err(|_| anyhow!("stream {i} panicked"))??;
        total += b;
        crcs[i] = c;
    }
    Ok((total, crcs))
}

// ------------------ MODE B: framed, work-stealing dispatch ------------------

struct Chunk {
    offset: u64,
    data: Vec<u8>,
}

struct BoundedQueue {
    inner: Mutex<QState>,
    not_empty: Condvar,
    not_full: Condvar,
}
struct QState {
    buf: VecDeque<Chunk>,
    cap: usize,
    closed: bool,
}
impl BoundedQueue {
    fn new(cap: usize) -> Self {
        Self {
            inner: Mutex::new(QState {
                buf: VecDeque::with_capacity(cap),
                cap,
                closed: false,
            }),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
        }
    }
    fn push(&self, c: Chunk) {
        let mut g = self.inner.lock().unwrap();
        while g.buf.len() >= g.cap && !g.closed {
            g = self.not_full.wait(g).unwrap();
        }
        g.buf.push_back(c);
        self.not_empty.notify_one();
    }
    fn pop(&self) -> Option<Chunk> {
        let mut g = self.inner.lock().unwrap();
        loop {
            if let Some(c) = g.buf.pop_front() {
                self.not_full.notify_one();
                return Some(c);
            }
            if g.closed {
                return None;
            }
            g = self.not_empty.wait(g).unwrap();
        }
    }
    fn close(&self) {
        let mut g = self.inner.lock().unwrap();
        g.closed = true;
        self.not_empty.notify_all();
        self.not_full.notify_all();
    }
}

fn run_mode_framed(
    opts: &SendOpts,
    ssh: &SshCtx,
    token: &str,
    progress: &Arc<Progress>,
) -> Result<(u64, Vec<u32>)> {
    let n = opts.streams as usize;
    let q = Arc::new(BoundedQueue::new(std::cmp::max(4, n * 2)));

    let mut handles = Vec::with_capacity(n);
    for i in 0..n {
        if i > 0 && opts.stream_delay_ms > 0 {
            thread::sleep(std::time::Duration::from_millis(opts.stream_delay_ms));
        }
        let ssh = ssh.clone();
        let token = token.to_string();
        let q = q.clone();
        let id = i as u32;
        let progress = progress.clone();

        let h = thread::spawn(move || -> Result<u64> {
            let id_str = id.to_string();
            let mut child = ssh
                .cmd(&["recv-data", "--token", &token, "--id", &id_str])
                .stdin(Stdio::piped())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .spawn()
                .context("spawn ssh data")?;
            let mut sink = child.stdin.take().unwrap();
            let mut sent = 0u64;

            while let Some(chunk) = q.pop() {
                let crc = crc32c::crc32c(&chunk.data);
                let hdr = encode_frame_hdr(chunk.offset, chunk.data.len() as u32, crc);
                sink.write_all(&hdr).map_err(|e| ssh_write_err(id, e))?;
                sink.write_all(&chunk.data)
                    .map_err(|e| ssh_write_err(id, e))?;
                sent += chunk.data.len() as u64;
                progress.add(chunk.data.len() as u64);
            }
            drop(sink);
            let status = child.wait()?;
            if !status.success() {
                bail!("ssh data stream {id} exited {status}");
            }
            Ok(sent)
        });
        handles.push(h);
    }

    // Producer reads stdin in full block_size chunks.
    let bs = opts.block_size as usize;
    let stdin = std::io::stdin();
    let mut lock = stdin.lock();
    let mut offset = 0u64;
    let mut total = 0u64;
    loop {
        let mut buf = vec![0u8; bs];
        let mut filled = 0;
        while filled < bs {
            match lock.read(&mut buf[filled..])? {
                0 => break,
                n => filled += n,
            }
        }
        if filled == 0 {
            break;
        }
        buf.truncate(filled);
        q.push(Chunk { offset, data: buf });
        offset += filled as u64;
        total += filled as u64;
    }
    q.close();

    for (i, h) in handles.into_iter().enumerate() {
        let _ = h.join().map_err(|_| anyhow!("worker {i} panicked"))??;
    }
    Ok((total, Vec::new()))
}

fn ssh_write_err(id: u32, e: std::io::Error) -> anyhow::Error {
    anyhow!(
        "stream {id}: ssh write failed ({e}). \
         sshd may be throttling (MaxStartups / fail2ban); \
         try --stream-delay-ms 300 or lower -n."
    )
}

/// Like `compute_ranges` but each range offset+length is a multiple of `align`,
/// except the last which absorbs the (already-aligned) remainder. Requires
/// `total % align == 0`.
fn compute_ranges_aligned(total: u64, n: u32, align: u64) -> Vec<crate::proto::Range> {
    assert!(align > 0 && total % align == 0);
    let n = n as u64;
    let base_blocks = (total / align) / n;
    let rem_blocks = (total / align) % n;
    let mut out = Vec::with_capacity(n as usize);
    let mut off = 0u64;
    for i in 0..n {
        let blocks = base_blocks + if i < rem_blocks { 1 } else { 0 };
        let len = blocks * align;
        out.push(crate::proto::Range { offset: off, length: len });
        off += len;
    }
    out
}
