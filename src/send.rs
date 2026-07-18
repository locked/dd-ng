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
    pub sync: bool,
    pub verbose: u8,
    pub direct: bool,
    pub stats: bool,
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
        if self.verbose >= 2 {
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

// --------- stage stats ---------

/// Cumulative wait-time & byte counters across all sender threads.
/// Times are in nanoseconds; all fields are Relaxed atomic.
pub struct StageStats {
    pub src_read_ns: AtomicU64,
    pub src_read_bytes: AtomicU64,
    pub net_send_ns: AtomicU64,
    pub net_send_bytes: AtomicU64,
    /// Framed-mode only: sampled by progress thread.
    pub q_depth_sum: AtomicU64,
    pub q_depth_samples: AtomicU64,
    pub q_cap: AtomicU64,
    /// Per-stream (index = id), populated at stream end.
    pub per_stream: Mutex<Vec<PerStreamSendStats>>,
}

#[derive(Debug, Clone, Default)]
pub struct PerStreamSendStats {
    pub id: u32,
    pub bytes: u64,
    pub src_read_ns: u64,
    pub net_send_ns: u64,
    pub wall_ns: u64,
}

impl StageStats {
    pub fn new(n: u32) -> Arc<Self> {
        Arc::new(Self {
            src_read_ns: AtomicU64::new(0),
            src_read_bytes: AtomicU64::new(0),
            net_send_ns: AtomicU64::new(0),
            net_send_bytes: AtomicU64::new(0),
            q_depth_sum: AtomicU64::new(0),
            q_depth_samples: AtomicU64::new(0),
            q_cap: AtomicU64::new(0),
            per_stream: Mutex::new(vec![PerStreamSendStats::default(); n as usize]),
        })
    }
}

/// Time `f`, adding elapsed ns to `ns_ctr` and returning f's result.
#[allow(dead_code)]
#[inline]
fn timed<T>(ns_ctr: &AtomicU64, f: impl FnOnce() -> T) -> T {
    let t0 = std::time::Instant::now();
    let r = f();
    ns_ctr.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    r
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

fn spawn_progress(
    p: Arc<Progress>,
    interval_ms: u64,
    stats: Option<Arc<StageStats>>,
    n_streams: u32,
    q_for_sampling: Option<Arc<BoundedQueue>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let interval = std::time::Duration::from_millis(interval_ms);
        let mut last_bytes = 0u64;
        let mut last_t = std::time::Instant::now();
        let mut last_src_ns = 0u64;
        let mut last_net_ns = 0u64;
        let is_tty = unsafe { libc_isatty_stderr() };
        while !p.done.load(Ordering::Relaxed) {
            std::thread::sleep(interval);
            // Sample queue depth if applicable.
            if let (Some(st), Some(q)) = (stats.as_ref(), q_for_sampling.as_ref()) {
                let (d, cap) = q.snapshot();
                st.q_depth_sum.fetch_add(d as u64, Ordering::Relaxed);
                st.q_depth_samples.fetch_add(1, Ordering::Relaxed);
                st.q_cap.store(cap as u64, Ordering::Relaxed);
            }
            let now = std::time::Instant::now();
            let cur = p.bytes.load(Ordering::Relaxed);
            let dt = now.duration_since(last_t).as_secs_f64().max(1e-9);
            let inst = (cur - last_bytes) as f64 / dt;
            let avg = cur as f64 / now.duration_since(p.start).as_secs_f64().max(1e-9);
            let mut msg = if p.total > 0 {
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

            if let Some(st) = stats.as_ref() {
                // Busy fractions across all threads over the last interval.
                // Denominator: N_threads * dt (in ns).
                let src_ns = st.src_read_ns.load(Ordering::Relaxed);
                let net_ns = st.net_send_ns.load(Ordering::Relaxed);
                let d_src = src_ns.saturating_sub(last_src_ns) as f64;
                let d_net = net_ns.saturating_sub(last_net_ns) as f64;
                last_src_ns = src_ns;
                last_net_ns = net_ns;
                // src read is single-threaded in framed mode, N-threaded in range mode.
                let src_threads = if q_for_sampling.is_some() { 1.0 } else { n_streams as f64 };
                let denom_src = src_threads * dt * 1e9;
                let denom_net = (n_streams as f64) * dt * 1e9;
                let src_busy = (d_src / denom_src.max(1.0)).min(1.0) * 100.0;
                let net_busy = (d_net / denom_net.max(1.0)).min(1.0) * 100.0;
                msg.push_str(&format!("  src {:>4.0}% net {:>4.0}%", src_busy, net_busy));
                if q_for_sampling.is_some() {
                    let cap = st.q_cap.load(Ordering::Relaxed).max(1);
                    let samples = st.q_depth_samples.load(Ordering::Relaxed).max(1);
                    let avg_d = st.q_depth_sum.load(Ordering::Relaxed) as f64 / samples as f64;
                    msg.push_str(&format!("  q {:.1}/{}", avg_d, cap));
                }
            }

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
        if ft.is_fifo() {
            if opts.direct {
                bail!("--direct requires a seekable input (regular file or block device), not a FIFO");
            }
            // Route FIFO through Framed mode by redirecting stdin from the FIFO.
            redirect_stdin_from(&opts.input)?;
            (Mode::Framed, 0u64, Vec::new())
        } else {
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
                bail!("input must be a regular file, block device, FIFO, or '-' for stdin");
            };
            if opts.direct {
                const A: u64 = 4096;
                if opts.block_size % A != 0 {
                    bail!("--direct requires --block-size to be a multiple of {A}");
                }
                (Mode::Range, total, compute_ranges_aligned(total, opts.streams, A))
            } else {
                (Mode::Range, total, compute_ranges(total, opts.streams))
            }
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

    if opts.verbose >= 1 {
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
    }

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
    if opts.verbose >= 3 {
        eprintln!("[ctrl>] Manifest {}", serde_json::to_string(&manifest)?);
    }

    let mut ctrl_out = BufReader::new(ctrl.stdout.take().unwrap());
    let ready_line = read_line(&mut ctrl_out).context("reading Ready from remote ctrl")?;
    if opts.verbose >= 3 {
        eprintln!("[ctrl<] {}", ready_line);
    }
    match serde_json::from_str::<CtrlMsg>(&ready_line)? {
        CtrlMsg::Ready => {}
        CtrlMsg::Abort { reason } => bail!("remote aborted: {reason}"),
        other => bail!("unexpected ctrl msg: {other:?}"),
    }
    if opts.verbose >= 1 {
        eprintln!("[send] remote ready");
    }

    let start = std::time::Instant::now();
    let progress = Progress::new(total_size);
    let stats = if opts.stats { Some(StageStats::new(opts.streams)) } else { None };

    // For framed mode we sample the queue depth; the queue itself is created
    // inside run_mode_framed. To keep spawn_progress simple, framed sampling
    // is enabled by giving run_mode_framed the stats handle and letting the
    // producer register the queue via a shared slot. We pass an Option<Arc<>>
    // via a Mutex placeholder.
    let framed_queue_slot: Arc<Mutex<Option<Arc<BoundedQueue>>>> = Arc::new(Mutex::new(None));

    const PROGRESS_INTERVAL_MS: u64 = 500;
    let prog_handle = if opts.verbose >= 1 {
        Some(spawn_progress(
            progress.clone(),
            PROGRESS_INTERVAL_MS,
            stats.clone(),
            opts.streams,
            None,
        ))
    } else {
        None
    };

    // Framed queue-depth sampler (only if stats + framed + progress enabled).
    let mut q_sampler_handle: Option<thread::JoinHandle<()>> = None;
    if opts.stats && mode == Mode::Framed && opts.verbose >= 1 {
        let slot = framed_queue_slot.clone();
        let st = stats.clone().unwrap();
        let done_flag = progress.clone();
        let interval = std::time::Duration::from_millis(PROGRESS_INTERVAL_MS);
        q_sampler_handle = Some(thread::spawn(move || {
            while !done_flag.done.load(Ordering::Relaxed) {
                std::thread::sleep(interval);
                if let Some(q) = slot.lock().unwrap().as_ref() {
                    let (d, cap) = q.snapshot();
                    st.q_depth_sum.fetch_add(d as u64, Ordering::Relaxed);
                    st.q_depth_samples.fetch_add(1, Ordering::Relaxed);
                    st.q_cap.store(cap as u64, Ordering::Relaxed);
                }
            }
        }));
    }

    let (total_sent, stream_crcs) = match mode {
        Mode::Range => run_mode_range(&opts, &ssh, &token, &ranges, &progress, stats.as_ref())?,
        Mode::Framed => run_mode_framed(&opts, &ssh, &token, &progress, stats.as_ref(), &framed_queue_slot)?,
    };

    progress.stop();
    if let Some(h) = prog_handle {
        let _ = h.join();
    }
    if let Some(h) = q_sampler_handle {
        let _ = h.join();
    }

    if opts.verbose >= 1 {
        if opts.sync {
            eprintln!("[send] all bytes sent; waiting for remote fsync...");
        } else {
            eprintln!("[send] all bytes sent; waiting for remote finalize...");
        }
    }

    let report = CtrlMsg::SenderReport {
        stream_crcs,
        total_bytes: total_sent,
    };
    let report_json = serde_json::to_string(&report)?;
    let write_res = write_line(ctrl.stdin.as_mut().unwrap(), &report_json)
        .context("sending SenderReport to remote ctrl");
    if opts.verbose >= 3 {
        eprintln!("[ctrl>] {}", report_json);
    }

    // Whether the write succeeded or not, try to read the remote's verdict.
    // If the remote already aborted (and closed its stdin, causing EPIPE),
    // ctrl_out will still contain the Abort message.
    // We may receive an optional CtrlMsg::RecvStats before the final Done.
    let mut remote_stats: Option<RemoteStats> = None;
    let final_line_res = loop {
        match read_line(&mut ctrl_out).context("reading final ctrl msg") {
            Ok(line) => {
                if opts.verbose >= 3 {
                    eprintln!("[ctrl<] {}", line);
                }
                match serde_json::from_str::<CtrlMsg>(&line) {
                    Ok(CtrlMsg::RecvStats {
                        net_recv_ns, dst_write_ns, wall_ns, per_stream,
                        read_starved_ns, read_calls, read_short_calls,
                    }) => {
                        remote_stats = Some(RemoteStats {
                            net_recv_ns, dst_write_ns, wall_ns, per_stream,
                            read_starved_ns, read_calls, read_short_calls,
                        });
                        continue;
                    }
                    _ => break Ok(line),
                }
            }
            Err(e) => break Err(e),
        }
    };
    let final_line = final_line_res;

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
                        if opts.verbose >= 1 {
                            eprintln!(
                                "[send] done: {} bytes in {:.2}s ({:.1} MB/s)",
                                bytes,
                                el,
                                (bytes as f64) / 1e6 / el.max(1e-9)
                            );
                        }
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
            if opts.verbose >= 1 {
                eprintln!(
                    "[send] done: {} bytes in {:.2}s ({:.1} MB/s)",
                    bytes,
                    el,
                    (bytes as f64) / 1e6 / el.max(1e-9)
                );
            }
            if mode == Mode::Range && bytes != total_size {
                bail!("size mismatch: ack {bytes}, expected {total_size}");
            }
        }
        CtrlMsg::Abort { reason } => bail!("remote aborted: {reason}"),
        other => bail!("unexpected ctrl msg: {other:?}"),
    }
    if opts.stats {
        print_stats_table(
            stats.as_ref(),
            remote_stats.as_ref(),
            start.elapsed(),
            mode,
            opts.streams,
        );
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
    stats: Option<&Arc<StageStats>>,
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
        let stats = stats.cloned();

        let h = thread::spawn(move || -> Result<(u64, u32)> {
            let wall_t0 = std::time::Instant::now();
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

            let mut local_read_ns = 0u64;
            let mut local_write_ns = 0u64;

            while written < range.length {
                let want =
                    std::cmp::min(buf.len() as u64, range.length - written) as usize;
                let t0 = std::time::Instant::now();
                let n = f
                    .read_at(&mut buf[..want], range.offset + written)
                    .with_context(|| format!("pread @{}", range.offset + written))?;
                let dt_read = t0.elapsed().as_nanos() as u64;
                local_read_ns += dt_read;
                if let Some(st) = &stats {
                    st.src_read_ns.fetch_add(dt_read, Ordering::Relaxed);
                    st.src_read_bytes.fetch_add(n as u64, Ordering::Relaxed);
                }
                if n == 0 {
                    bail!("short read stream {id}");
                }
                crc = crc32c::crc32c_append(crc, &buf[..n]);
                let t1 = std::time::Instant::now();
                sink.write_all(&buf[..n]).map_err(|e| ssh_write_err(id, e))?;
                let dt_write = t1.elapsed().as_nanos() as u64;
                local_write_ns += dt_write;
                if let Some(st) = &stats {
                    st.net_send_ns.fetch_add(dt_write, Ordering::Relaxed);
                    st.net_send_bytes.fetch_add(n as u64, Ordering::Relaxed);
                }
                written += n as u64;
                progress.add(n as u64);
            }
            drop(sink);
            let status = child.wait()?;
            if !status.success() {
                bail!("ssh data stream {id} exited {status}");
            }
            if let Some(st) = &stats {
                let mut per = st.per_stream.lock().unwrap();
                per[id as usize] = PerStreamSendStats {
                    id,
                    bytes: written,
                    src_read_ns: local_read_ns,
                    net_send_ns: local_write_ns,
                    wall_ns: wall_t0.elapsed().as_nanos() as u64,
                };
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
    fn snapshot(&self) -> (usize, usize) {
        let g = self.inner.lock().unwrap();
        (g.buf.len(), g.cap)
    }
}

fn run_mode_framed(
    opts: &SendOpts,
    ssh: &SshCtx,
    token: &str,
    progress: &Arc<Progress>,
    stats: Option<&Arc<StageStats>>,
    q_slot: &Arc<Mutex<Option<Arc<BoundedQueue>>>>,
) -> Result<(u64, Vec<u32>)> {
    let n = opts.streams as usize;
    let q = Arc::new(BoundedQueue::new(std::cmp::max(4, n * 2)));
    *q_slot.lock().unwrap() = Some(q.clone());

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
        let stats = stats.cloned();

        let h = thread::spawn(move || -> Result<u64> {
            let wall_t0 = std::time::Instant::now();
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
            let mut local_write_ns = 0u64;

            while let Some(chunk) = q.pop() {
                let crc = crc32c::crc32c(&chunk.data);
                let hdr = encode_frame_hdr(chunk.offset, chunk.data.len() as u32, crc);
                let t0 = std::time::Instant::now();
                sink.write_all(&hdr).map_err(|e| ssh_write_err(id, e))?;
                sink.write_all(&chunk.data)
                    .map_err(|e| ssh_write_err(id, e))?;
                let dt = t0.elapsed().as_nanos() as u64;
                local_write_ns += dt;
                if let Some(st) = &stats {
                    st.net_send_ns.fetch_add(dt, Ordering::Relaxed);
                    st.net_send_bytes.fetch_add(chunk.data.len() as u64, Ordering::Relaxed);
                }
                sent += chunk.data.len() as u64;
                progress.add(chunk.data.len() as u64);
            }
            drop(sink);
            let status = child.wait()?;
            if !status.success() {
                bail!("ssh data stream {id} exited {status}");
            }
            if let Some(st) = &stats {
                let mut per = st.per_stream.lock().unwrap();
                per[id as usize] = PerStreamSendStats {
                    id,
                    bytes: sent,
                    src_read_ns: 0, // src read is shared by the producer; see aggregate
                    net_send_ns: local_write_ns,
                    wall_ns: wall_t0.elapsed().as_nanos() as u64,
                };
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
            let t0 = std::time::Instant::now();
            let r = lock.read(&mut buf[filled..])?;
            let dt = t0.elapsed().as_nanos() as u64;
            if let Some(st) = stats {
                st.src_read_ns.fetch_add(dt, Ordering::Relaxed);
                st.src_read_bytes.fetch_add(r as u64, Ordering::Relaxed);
            }
            match r {
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

/// Open `path` (a FIFO or other readable file) and dup its fd onto stdin (fd 0)
/// so the Framed producer, which reads from `std::io::stdin()`, consumes from it.
/// Blocks until a writer opens the FIFO (standard open(2) semantics for O_RDONLY).
fn redirect_stdin_from(path: &str) -> Result<()> {
    use std::os::unix::io::AsRawFd;
    extern "C" {
        fn dup2(oldfd: i32, newfd: i32) -> i32;
    }
    let f = File::open(path).with_context(|| format!("open {path}"))?;
    let rc = unsafe { dup2(f.as_raw_fd(), 0) };
    if rc < 0 {
        return Err(anyhow!(
            "dup2({}, 0) failed: {}",
            f.as_raw_fd(),
            std::io::Error::last_os_error()
        ));
    }
    // `f` drops here, closing the original fd; fd 0 keeps the open file description.
    Ok(())
}

fn ssh_write_err(id: u32, e: std::io::Error) -> anyhow::Error {
    anyhow!(
        "stream {id}: ssh write failed ({e}). \
         sshd may be throttling (MaxStartups / fail2ban); \
         try --stream-delay-ms 300 or lower -n."
    )
}

fn fmt_ns_rate(bytes: u64, ns: u64) -> String {
    if ns == 0 {
        return "  -  ".to_string();
    }
    let bps = (bytes as f64) * 1e9 / (ns as f64);
    format!("{}/s", fmt_bytes(bps))
}

struct RemoteStats {
    net_recv_ns: u64,
    dst_write_ns: u64,
    wall_ns: u64,
    per_stream: Vec<crate::proto::RecvStreamStats>,
    read_starved_ns: u64,
    read_calls: u64,
    read_short_calls: u64,
}

fn print_stats_table(
    stats: Option<&Arc<StageStats>>,
    remote: Option<&RemoteStats>,
    wall: std::time::Duration,
    mode: Mode,
    n_streams: u32,
) {
    let wall_ns = wall.as_nanos() as u64;
    eprintln!();
    eprintln!("=== dd-ng stats ===");
    eprintln!(
        "wall {:.3}s  streams {}  mode {:?}",
        wall.as_secs_f64(),
        n_streams,
        mode
    );
    if let Some(st) = stats {
        let src_ns = st.src_read_ns.load(Ordering::Relaxed);
        let src_bytes = st.src_read_bytes.load(Ordering::Relaxed);
        let net_ns = st.net_send_ns.load(Ordering::Relaxed);
        let net_bytes = st.net_send_bytes.load(Ordering::Relaxed);
        let src_threads = if mode == Mode::Framed { 1 } else { n_streams as u64 };
        let src_busy = if wall_ns > 0 {
            100.0 * (src_ns as f64) / ((src_threads as f64) * (wall_ns as f64))
        } else { 0.0 };
        let net_busy = if wall_ns > 0 {
            100.0 * (net_ns as f64) / ((n_streams as f64) * (wall_ns as f64))
        } else { 0.0 };
        eprintln!("{:<12} {:>12} {:>8} {:>14}", "stage", "bytes", "busy%", "eff rate");
        eprintln!(
            "{:<12} {:>12} {:>7.1}% {:>14}",
            "src read", fmt_bytes(src_bytes as f64), src_busy.min(100.0), fmt_ns_rate(src_bytes, src_ns)
        );
        eprintln!(
            "{:<12} {:>12} {:>7.1}% {:>14}",
            "net send", fmt_bytes(net_bytes as f64), net_busy.min(100.0), fmt_ns_rate(net_bytes, net_ns)
        );
        if mode == Mode::Framed {
            let samples = st.q_depth_samples.load(Ordering::Relaxed);
            if samples > 0 {
                let cap = st.q_cap.load(Ordering::Relaxed);
                let avg = st.q_depth_sum.load(Ordering::Relaxed) as f64 / samples as f64;
                eprintln!("queue depth avg {:.2}/{}  ({} samples)", avg, cap, samples);
            }
        }
    }
    if let Some(r) = remote {
        let total_bytes: u64 = r.per_stream.iter().map(|s| s.bytes).sum();
        let denom = (n_streams as f64) * (wall_ns as f64).max(1.0);
        let r_net_busy = 100.0 * (r.net_recv_ns as f64) / denom;
        let r_dst_busy = 100.0 * (r.dst_write_ns as f64) / denom;
        let r_starved  = 100.0 * (r.read_starved_ns as f64) / denom;
        eprintln!(
            "{:<12} {:>12} {:>7.1}% {:>14}   [remote]",
            "net recv", fmt_bytes(total_bytes as f64), r_net_busy.min(100.0),
            fmt_ns_rate(total_bytes, r.net_recv_ns)
        );
        eprintln!(
            "{:<12} {:>12} {:>7.1}% {:>14}   [remote]",
            "dst write", fmt_bytes(total_bytes as f64), r_dst_busy.min(100.0),
            fmt_ns_rate(total_bytes, r.dst_write_ns)
        );
        // Starvation summary.
        let short_frac = if r.read_calls > 0 {
            100.0 * (r.read_short_calls as f64) / (r.read_calls as f64)
        } else { 0.0 };
        eprintln!(
            "read starved  {:>7.1}% of wall    ({} short of {} reads, {:.1}%)",
            r_starved.min(100.0),
            r.read_short_calls, r.read_calls, short_frac
        );
        eprintln!();
        eprintln!(
            "{:<3} {:>12} {:>10} {:>10} {:>10} {:>10}   [remote per-stream]",
            "id", "bytes", "net_recv", "dst_write", "starved", "wall"
        );
        let mut sorted = r.per_stream.clone();
        sorted.sort_by_key(|s| s.stream_id);
        for s in &sorted {
            eprintln!(
                "{:<3} {:>12} {:>9.3}s {:>9.3}s {:>9.3}s {:>9.3}s",
                s.stream_id,
                fmt_bytes(s.bytes as f64),
                (s.net_recv_ns as f64) / 1e9,
                (s.dst_write_ns as f64) / 1e9,
                (s.read_starved_ns as f64) / 1e9,
                (s.wall_ns as f64) / 1e9,
            );
        }
    } else {
        eprintln!("(remote stats unavailable; receiver may be an older version)");
    }

    if let Some(st) = stats {
        let per = st.per_stream.lock().unwrap();
        eprintln!();
        eprintln!(
            "{:<3} {:>12} {:>10} {:>10} {:>10}   [sender per-stream]",
            "id", "bytes", "src_read", "net_send", "wall"
        );
        for s in per.iter() {
            eprintln!(
                "{:<3} {:>12} {:>9.3}s {:>9.3}s {:>9.3}s",
                s.id,
                fmt_bytes(s.bytes as f64),
                (s.src_read_ns as f64) / 1e9,
                (s.net_send_ns as f64) / 1e9,
                (s.wall_ns as f64) / 1e9,
            );
        }
    }

    // Refined bottleneck heuristic using read_starved_ns.
    if let (Some(st), Some(r)) = (stats, remote) {
        let src_ns = st.src_read_ns.load(Ordering::Relaxed);
        let s_net_ns = st.net_send_ns.load(Ordering::Relaxed);
        let src_threads = if mode == Mode::Framed { 1 } else { n_streams as u64 };
        let src_busy = (src_ns as f64) / ((src_threads as f64) * (wall_ns as f64).max(1.0));
        let net_send_busy = (s_net_ns as f64) / ((n_streams as f64) * (wall_ns as f64).max(1.0));
        let dst_busy = (r.dst_write_ns as f64) / ((n_streams as f64) * (wall_ns as f64).max(1.0));
        let starved  = (r.read_starved_ns as f64) / ((n_streams as f64) * (wall_ns as f64).max(1.0));

        // Interpretation:
        //   - If sender's net_send is high AND receiver is starved on reads
        //     -> network is slow (sender pushes hard, receiver waits).
        //   - If sender's net_send is high AND receiver is NOT starved but
        //     dst_write is high -> destination disk is the bottleneck (back-pressure).
        //   - If src_busy is high -> source is the bottleneck.
        let (label, val) = if src_busy > 0.7 {
            ("source read", src_busy)
        } else if net_send_busy > 0.5 && starved > 0.3 {
            ("network", starved.max(net_send_busy))
        } else if net_send_busy > 0.5 && dst_busy > 0.4 {
            ("destination write", dst_busy)
        } else if dst_busy > src_busy && dst_busy > starved {
            ("destination write", dst_busy)
        } else if starved > src_busy && starved > dst_busy {
            ("network", starved)
        } else {
            let m = src_busy.max(net_send_busy).max(dst_busy).max(starved);
            ("(unclear)", m)
        };
        if val > 0.3 {
            eprintln!("\nlikely bottleneck: {} ({:.0}% of wall)", label, val * 100.0);
        } else {
            eprintln!("\nno stage dominates (all < 30% of wall); likely CPU/other or near line-rate");
        }
    }
}

/// Like `compute_ranges` but each range offset+length is a multiple of `align`,
/// except the last which absorbs the (possibly unaligned) remainder. Offsets
/// are always aligned; only the final range's length may be unaligned.
fn compute_ranges_aligned(total: u64, n: u32, align: u64) -> Vec<crate::proto::Range> {
    assert!(align > 0);
    let n = n as u64;
    let aligned_total = (total / align) * align;
    let base_blocks = (aligned_total / align) / n;
    let rem_blocks = (aligned_total / align) % n;
    let mut out = Vec::with_capacity(n as usize);
    let mut off = 0u64;
    for i in 0..n {
        let blocks = base_blocks + if i < rem_blocks { 1 } else { 0 };
        let len = blocks * align;
        out.push(crate::proto::Range { offset: off, length: len });
        off += len;
    }
    // Any sub-`align` tail goes onto the last range.
    if let Some(last) = out.last_mut() {
        last.length += total - aligned_total;
    }
    out
}
