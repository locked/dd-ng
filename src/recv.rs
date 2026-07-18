use crate::proto::{
    decode_frame_hdr, CtrlMsg, Manifest, Mode, Range, RecvStreamStats, RvMsg, FRAME_HDR_LEN, MAGIC,
    STARVE_THRESHOLD,
};
use crate::util::{read_line, rendezvous_socket_path, write_line};
use anyhow::{anyhow, bail, Context, Result};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Read};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Default, Debug, Clone, Copy)]
struct RecvIoStats {
    net_recv_ns: u64,
    dst_write_ns: u64,
    read_starved_ns: u64,
    read_calls: u64,
    read_short_calls: u64,
}
impl RecvIoStats {
    /// Record a completed read() call.
    #[inline]
    fn record_read(&mut self, dur_ns: u64, returned: usize) {
        self.net_recv_ns += dur_ns;
        self.read_calls += 1;
        if returned > 0 && returned < STARVE_THRESHOLD {
            self.read_starved_ns += dur_ns;
            self.read_short_calls += 1;
        }
    }
    #[inline]
    fn record_write(&mut self, dur_ns: u64) {
        self.dst_write_ns += dur_ns;
    }
}

// ---------------- CTRL role ----------------

pub fn run_ctrl() -> Result<()> {
    let stdin = std::io::stdin();
    let mut stdin_lock = stdin.lock();
    let mut line = String::new();
    stdin_lock
        .read_line(&mut line)
        .context("read manifest line")?;
    if line.is_empty() {
        bail!("no manifest received");
    }
    let manifest: Manifest =
        serde_json::from_str(line.trim_end()).context("parse manifest")?;
    if manifest.magic != MAGIC {
        bail!("bad magic: {}", manifest.magic);
    }

    let sock_path = rendezvous_socket_path(&manifest.token);
    if let Some(parent) = sock_path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let _ = std::fs::remove_file(&sock_path);
    let listener = UnixListener::bind(&sock_path)
        .with_context(|| format!("bind {}", sock_path.display()))?;
    let _guard = Remover(Some(sock_path.clone()));

    // Prepare output. Detect whether target is an existing block device;
    // if so, don't truncate, don't preallocate, and don't set_len later.
    let out_path = PathBuf::from(&manifest.output_path);
    if let Some(parent) = out_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).ok();
        }
    }
    let is_block_dev = match std::fs::metadata(&out_path) {
        Ok(m) => {
            use std::os::unix::fs::FileTypeExt;
            m.file_type().is_block_device() || m.file_type().is_char_device()
        }
        Err(_) => false,
    };
    let mut oo = OpenOptions::new();
    oo.create(true).write(true).mode(0o644);
    if !is_block_dev {
        oo.truncate(true);
    }
    let f = oo
        .open(&out_path)
        .with_context(|| format!("open output {}", out_path.display()))?;
    if manifest.mode == Mode::Range && !is_block_dev {
        // Real preallocation (extents allocated) so parallel pwrites don't
        // contend on block allocation. Falls back to ftruncate if unsupported.
        if manifest.total_size > 0 {
            preallocate(&f, manifest.total_size)?;
        }
    }
    if is_block_dev && manifest.mode == Mode::Range && manifest.total_size > 0 {
        // Verify the block device is large enough.
        use std::io::Seek;
        let dev_size = (&f)
            .seek(std::io::SeekFrom::End(0))
            .with_context(|| format!("seek end {}", out_path.display()))?;
        if dev_size < manifest.total_size {
            bail!(
                "target block device {} is {} bytes; need {}",
                out_path.display(),
                dev_size,
                manifest.total_size
            );
        }
    }
    drop(f);

    // READY
    let mut stdout = std::io::stdout().lock();
    write_line(&mut stdout, &serde_json::to_string(&CtrlMsg::Ready)?)?;

    let mut assignments: HashMap<u32, Range> = HashMap::new();
    if manifest.mode == Mode::Range {
        for (i, r) in manifest.ranges.iter().enumerate() {
            assignments.insert(i as u32, r.clone());
        }
    }
    let n = manifest.n_streams;

    // Accept N Hello and hand out assignments.
    let mut peers: Vec<UnixStream> = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let (mut stream, _) = listener.accept().context("accept rv")?;
        let mut rd = BufReader::new(stream.try_clone()?);
        let msg: RvMsg = serde_json::from_str(&read_line(&mut rd)?)?;
        let stream_id = match msg {
            RvMsg::Hello { stream_id } => stream_id,
            other => bail!("expected Hello, got {other:?}"),
        };
        let assign = match manifest.mode {
            Mode::Range => {
                let r = assignments
                    .get(&stream_id)
                    .ok_or_else(|| anyhow!("no assignment for stream {stream_id}"))?
                    .clone();
                RvMsg::AssignRange {
                    output_path: manifest.output_path.clone(),
                    offset: r.offset,
                    length: r.length,
                    direct: manifest.direct,
                    no_crc: manifest.no_crc,
                }
            }
            Mode::Framed => RvMsg::AssignFramed {
                output_path: manifest.output_path.clone(),
                no_crc: manifest.no_crc,
            },
        };
        write_line(&mut stream, &serde_json::to_string(&assign)?)?;
        peers.push(stream);
    }

    // Phase 2: collect Done/Failed.
    let mut recv_crcs: HashMap<u32, u32> = HashMap::new();
    let mut recv_bytes: HashMap<u32, u64> = HashMap::new();
    let mut total_bytes = 0u64;
    let mut per_stream_stats: Vec<RecvStreamStats> = Vec::with_capacity(n as usize);
    let mut sum_net_recv_ns: u64 = 0;
    let mut sum_dst_write_ns: u64 = 0;
    let mut sum_read_starved_ns: u64 = 0;
    let mut sum_read_calls: u64 = 0;
    let mut sum_read_short_calls: u64 = 0;
    let mut max_wall_ns: u64 = 0;

    for peer in peers.into_iter() {
        let mut rd = BufReader::new(peer.try_clone()?);
        let msg: RvMsg = serde_json::from_str(&read_line(&mut rd)?)?;
        match msg {
            RvMsg::Done {
                stream_id, bytes, crc,
                net_recv_ns, dst_write_ns, wall_ns,
                read_starved_ns, read_calls, read_short_calls,
            } => {
                total_bytes += bytes;
                recv_bytes.insert(stream_id, bytes);
                recv_crcs.insert(stream_id, crc);
                sum_net_recv_ns += net_recv_ns;
                sum_dst_write_ns += dst_write_ns;
                sum_read_starved_ns += read_starved_ns;
                sum_read_calls += read_calls;
                sum_read_short_calls += read_short_calls;
                if wall_ns > max_wall_ns { max_wall_ns = wall_ns; }
                per_stream_stats.push(RecvStreamStats {
                    stream_id,
                    bytes,
                    net_recv_ns,
                    dst_write_ns,
                    wall_ns,
                    read_starved_ns,
                    read_calls,
                    read_short_calls,
                });
                if manifest.mode == Mode::Range {
                    let expected = assignments.get(&stream_id).unwrap().length;
                    if bytes != expected {
                        return abort(
                            &mut stdout,
                            format!(
                                "stream {stream_id}: wrote {bytes}, expected {expected}"
                            ),
                        );
                    }
                }
            }
            RvMsg::Failed { stream_id, reason } => {
                return abort(&mut stdout, format!("stream {stream_id} failed: {reason}"));
            }
            other => bail!("unexpected rv msg: {other:?}"),
        }
    }

    // Wait for SenderReport on ctrl stdin.
    let mut line = String::new();
    stdin_lock
        .read_line(&mut line)
        .context("read SenderReport")?;
    if line.is_empty() {
        bail!("no SenderReport received");
    }
    let sr: CtrlMsg = serde_json::from_str(line.trim_end())?;
    let (sender_crcs, sender_total) = match sr {
        CtrlMsg::SenderReport {
            stream_crcs,
            total_bytes,
        } => (stream_crcs, total_bytes),
        CtrlMsg::Abort { reason } => bail!("sender aborted: {reason}"),
        other => bail!("unexpected ctrl msg: {other:?}"),
    };

    // Verify.
    if manifest.mode == Mode::Range {
        if sender_crcs.len() != n as usize {
            return abort(
                &mut stdout,
                format!("sender crc count {} != {}", sender_crcs.len(), n),
            );
        }
        for i in 0..n {
            let s = sender_crcs[i as usize];
            let r = *recv_crcs.get(&i).unwrap_or(&0xdead_beef);
            if s != r {
                return abort(
                    &mut stdout,
                    format!("crc mismatch on stream {i}: sender=0x{s:08x} recv=0x{r:08x}"),
                );
            }
        }
        if sender_total != manifest.total_size {
            return abort(
                &mut stdout,
                format!(
                    "sender total {} != manifest total {}",
                    sender_total, manifest.total_size
                ),
            );
        }
    } else {
        // Framed: per-frame CRCs already verified. Sizes must match.
        if sender_total != total_bytes {
            return abort(
                &mut stdout,
                format!(
                    "framed size mismatch: sender {}, received {}",
                    sender_total, total_bytes
                ),
            );
        }
    }

    // fsync (only if sender requested it)
    let f = OpenOptions::new()
        .write(true)
        .open(&out_path)
        .context("reopen for finalize")?;
    if manifest.mode == Mode::Framed && !is_block_dev {
        // Framed writes may have grown the file arbitrarily; set final length.
        f.set_len(total_bytes).ok();
    }
    if manifest.sync {
        f.sync_all().context("fsync output")?;
    }
    drop(f);

    write_line(
        &mut stdout,
        &serde_json::to_string(&CtrlMsg::RecvStats {
            net_recv_ns: sum_net_recv_ns,
            dst_write_ns: sum_dst_write_ns,
            read_starved_ns: sum_read_starved_ns,
            read_calls: sum_read_calls,
            read_short_calls: sum_read_short_calls,
            wall_ns: max_wall_ns,
            per_stream: per_stream_stats,
        })?,
    )?;
    write_line(
        &mut stdout,
        &serde_json::to_string(&CtrlMsg::Done { bytes: total_bytes })?,
    )?;
    Ok(())
}

fn abort<W: std::io::Write>(w: &mut W, reason: String) -> Result<()> {
    let _ = write_line(w, &serde_json::to_string(&CtrlMsg::Abort { reason: reason.clone() })?);
    bail!(reason);
}

// ---------------- DATA role ----------------

pub fn run_data(token: &str, id: u32) -> Result<()> {
    // Best-effort: enlarge stdin pipe (ssh -> dd-ng recv-data) to reduce
    // syscall count and prevent short reads on high-latency links.
    let _ = crate::util::try_set_pipe_size(0, 1 << 20);

    let sock_path = rendezvous_socket_path(token);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut stream = loop {
        match UnixStream::connect(&sock_path) {
            Ok(s) => break s,
            Err(e) => {
                if std::time::Instant::now() >= deadline {
                    return Err(anyhow::Error::new(e)
                        .context(format!("connect {}", sock_path.display())));
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    };

    write_line(
        &mut stream,
        &serde_json::to_string(&RvMsg::Hello { stream_id: id })?,
    )?;
    let mut rd = BufReader::new(stream.try_clone()?);
    let assign: RvMsg = serde_json::from_str(&read_line(&mut rd)?)?;

    let wall_t0 = std::time::Instant::now();
    let mut io = RecvIoStats::default();

    let (written, crc) = match assign {
        RvMsg::AssignRange {
            output_path,
            offset,
            length,
            direct,
            no_crc,
        } => run_data_range(
            &mut stream,
            id,
            &output_path,
            offset,
            length,
            direct,
            no_crc,
            &mut io,
        )?,
        RvMsg::AssignFramed { output_path, no_crc } => {
            run_data_framed(&mut stream, id, &output_path, no_crc, &mut io)?
        }
        other => bail!("expected Assign*, got {other:?}"),
    };

    write_line(
        &mut stream,
        &serde_json::to_string(&RvMsg::Done {
            stream_id: id,
            bytes: written,
            crc,
            net_recv_ns: io.net_recv_ns,
            dst_write_ns: io.dst_write_ns,
            wall_ns: wall_t0.elapsed().as_nanos() as u64,
            read_starved_ns: io.read_starved_ns,
            read_calls: io.read_calls,
            read_short_calls: io.read_short_calls,
        })?,
    )?;
    Ok(())
}

fn run_data_range(
    stream: &mut UnixStream,
    id: u32,
    path: &str,
    offset: u64,
    length: u64,
    direct: bool,
    no_crc: bool,
    io: &mut RecvIoStats,
) -> Result<(u64, u32)> {
    const ALIGN: usize = 4096;
    let mut oo = OpenOptions::new();
    oo.write(true);
    if direct {
        // O_DIRECT = 0o40000 on Linux.
        oo.custom_flags(0o40000);
    }
    let out = oo
        .open(path)
        .with_context(|| format!("open output {path}"))?;
    let stdin = std::io::stdin();
    let mut lock = stdin.lock();
    // With O_DIRECT the buffer, offset, and length of each pwrite must be
    // aligned to the underlying block size (we use 4 KiB, which covers all
    // sane setups). Use 4 MiB chunks by default.
    let bufsize: usize = if direct { 4 * 1024 * 1024 } else { 1 << 20 };
    let mut buf = if direct {
        AlignedBuf::new(bufsize, ALIGN)
    } else {
        AlignedBuf::heap(bufsize)
    };
    // Under O_DIRECT, only write the aligned prefix here; the unaligned tail
    // (if any) is handled below via a second, buffered fd.
    let aligned_len = if direct {
        length - (length % ALIGN as u64)
    } else {
        length
    };
    let tail_len = length - aligned_len;
    let mut written = 0u64;
    let mut crc = 0u32;
    while written < aligned_len {
        let want = std::cmp::min(buf.len() as u64, aligned_len - written) as usize;
        // In O_DIRECT mode we must read a *full* aligned chunk from stdin
        // before pwriting; short reads would leave us with an unaligned length.
        let target = if direct {
            std::cmp::min(want, buf.len())
        } else {
            want
        };
        let mut filled = 0;
        while filled < target {
            let t0 = std::time::Instant::now();
            let r = lock.read(&mut buf.as_mut_slice()[filled..target]);
            let dt = t0.elapsed().as_nanos() as u64;
            match r {
                Ok(0) => {
                    io.record_read(dt, 0);
                    break;
                }
                Ok(n) => {
                    io.record_read(dt, n);
                    filled += n;
                }
                Err(e) => {
                    let _ = report_failed(stream, id, format!("stdin read: {e}"));
                    return Err(e.into());
                }
            }
        }
        if filled == 0 {
            break;
        }
        if direct && filled != target {
            // Only tolerable if this is the final chunk and length is aligned.
            // In practice sender guarantees full aligned ranges when direct is on.
            let reason = format!(
                "short read under O_DIRECT: got {filled}, wanted {target} \
                 (stream {id})"
            );
            let _ = report_failed(stream, id, reason.clone());
            bail!(reason);
        }
        let tw = std::time::Instant::now();
        out.write_all_at(&buf.as_slice()[..filled], offset + written)
            .with_context(|| format!("pwrite @{}", offset + written))?;
        io.record_write(tw.elapsed().as_nanos() as u64);
        if !no_crc {
            crc = crc32c::crc32c_append(crc, &buf.as_slice()[..filled]);
        }
        written += filled as u64;
    }
    if written != aligned_len {
        let reason = format!("short input: got {written}, expected {aligned_len}");
        let _ = report_failed(stream, id, reason.clone());
        bail!(reason);
    }
    drop(out);

    // Handle unaligned tail under O_DIRECT by reopening without O_DIRECT.
    if direct && tail_len > 0 {
        let tail_out = OpenOptions::new()
            .write(true)
            .open(path)
            .with_context(|| format!("reopen output (buffered tail) {path}"))?;
        let mut tail_buf = vec![0u8; tail_len as usize];
        let mut filled = 0usize;
        while filled < tail_buf.len() {
            let t0 = std::time::Instant::now();
            let r = lock.read(&mut tail_buf[filled..]);
            let dt = t0.elapsed().as_nanos() as u64;
            match r {
                Ok(0) => {
                    io.record_read(dt, 0);
                    break;
                }
                Ok(n) => {
                    io.record_read(dt, n);
                    filled += n;
                }
                Err(e) => {
                    let _ = report_failed(stream, id, format!("stdin read (tail): {e}"));
                    return Err(e.into());
                }
            }
        }
        if filled != tail_buf.len() {
            let reason = format!(
                "short input on tail: got {filled}, expected {} (stream {id})",
                tail_buf.len()
            );
            let _ = report_failed(stream, id, reason.clone());
            bail!(reason);
        }
        let tw = std::time::Instant::now();
        tail_out
            .write_all_at(&tail_buf, offset + written)
            .with_context(|| format!("pwrite tail @{}", offset + written))?;
        io.record_write(tw.elapsed().as_nanos() as u64);
        if !no_crc {
            crc = crc32c::crc32c_append(crc, &tail_buf);
        }
        written += tail_len;
    }

    if written != length {
        let reason = format!("short input: got {written}, expected {length}");
        let _ = report_failed(stream, id, reason.clone());
        bail!(reason);
    }
    Ok((written, crc))
}

/// Heap buffer, optionally aligned via `posix_memalign` for O_DIRECT.
struct AlignedBuf {
    ptr: *mut u8,
    len: usize,
    aligned: bool,
    fallback: Vec<u8>,
}
// Safe: we don't share the pointer across threads.
unsafe impl Send for AlignedBuf {}
impl AlignedBuf {
    fn new(len: usize, align: usize) -> Self {
        extern "C" {
            fn posix_memalign(memptr: *mut *mut libc_void, align: usize, size: usize) -> i32;
        }
        let mut p: *mut libc_void = std::ptr::null_mut();
        let rc = unsafe { posix_memalign(&mut p, align, len) };
        if rc != 0 || p.is_null() {
            // Fall back to Vec (won't satisfy O_DIRECT, but caller decides).
            return Self::heap(len);
        }
        Self { ptr: p as *mut u8, len, aligned: true, fallback: Vec::new() }
    }
    fn heap(len: usize) -> Self {
        Self { ptr: std::ptr::null_mut(), len, aligned: false, fallback: vec![0u8; len] }
    }
    fn len(&self) -> usize { self.len }
    fn as_slice(&self) -> &[u8] {
        if self.aligned {
            unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
        } else {
            &self.fallback
        }
    }
    fn as_mut_slice(&mut self) -> &mut [u8] {
        if self.aligned {
            unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
        } else {
            &mut self.fallback
        }
    }
}
impl Drop for AlignedBuf {
    fn drop(&mut self) {
        if self.aligned && !self.ptr.is_null() {
            extern "C" { fn free(p: *mut libc_void); }
            unsafe { free(self.ptr as *mut libc_void); }
        }
    }
}
#[allow(non_camel_case_types)]
type libc_void = std::ffi::c_void;

fn run_data_framed(
    stream: &mut UnixStream,
    id: u32,
    path: &str,
    no_crc: bool,
    io: &mut RecvIoStats,
) -> Result<(u64, u32)> {
    let out = OpenOptions::new()
        .write(true)
        .open(path)
        .with_context(|| format!("open output {path}"))?;
    let stdin = std::io::stdin();
    let mut lock = stdin.lock();
    let mut hdr = [0u8; FRAME_HDR_LEN];
    let mut written = 0u64;
    // Framed mode returns crc=0 (per-frame CRCs already verified inline).
    loop {
        // Try to read a header; EOF at start of a frame => done.
        let mut got = 0;
        while got < FRAME_HDR_LEN {
            let t0 = std::time::Instant::now();
            let r = lock.read(&mut hdr[got..])?;
            let dt = t0.elapsed().as_nanos() as u64;
            io.record_read(dt, r);
            match r {
                0 => break,
                n => got += n,
            }
        }
        if got == 0 {
            break;
        }
        if got < FRAME_HDR_LEN {
            let reason = format!("truncated frame header ({got} bytes)");
            let _ = report_failed(stream, id, reason.clone());
            bail!(reason);
        }
        let (offset, length, want_crc) = decode_frame_hdr(&hdr);
        let mut buf = vec![0u8; length as usize];
        // Read payload in a loop so we can classify each read().
        let mut filled = 0usize;
        while filled < buf.len() {
            let t0 = std::time::Instant::now();
            let r = lock.read(&mut buf[filled..])?;
            let dt = t0.elapsed().as_nanos() as u64;
            io.record_read(dt, r);
            if r == 0 {
                bail!("short frame payload: got {filled}/{}", buf.len());
            }
            filled += r;
        }
        let got_crc = if no_crc { want_crc } else { crc32c::crc32c(&buf) };
        if got_crc != want_crc {
            let reason = format!(
                "crc mismatch stream {id} @off {offset} len {length}: \
                 got 0x{got_crc:08x} want 0x{want_crc:08x}"
            );
            let _ = report_failed(stream, id, reason.clone());
            bail!(reason);
        }
        let tw = std::time::Instant::now();
        out.write_all_at(&buf, offset)
            .with_context(|| format!("pwrite @{offset}"))?;
        io.record_write(tw.elapsed().as_nanos() as u64);
        written += length as u64;
    }
    Ok((written, 0))
}

fn report_failed(stream: &mut UnixStream, id: u32, reason: String) -> Result<()> {
    write_line(
        stream,
        &serde_json::to_string(&RvMsg::Failed {
            stream_id: id,
            reason,
        })?,
    )
}

struct Remover(Option<PathBuf>);
impl Drop for Remover {
    fn drop(&mut self) {
        if let Some(p) = self.0.take() {
            let _ = std::fs::remove_file(p);
        }
    }
}

/// Real preallocation (extents actually allocated). Falls back to ftruncate.
fn preallocate(f: &std::fs::File, size: u64) -> Result<()> {
    use std::os::unix::io::AsRawFd;
    extern "C" {
        fn posix_fallocate(fd: i32, offset: i64, len: i64) -> i32;
    }
    let rc = unsafe { posix_fallocate(f.as_raw_fd(), 0, size as i64) };
    if rc == 0 {
        Ok(())
    } else {
        // Not fatal: fall back to sparse (ftruncate). Some filesystems (tmpfs,
        // network mounts) don't support fallocate.
        f.set_len(size).context("ftruncate fallback")
    }
}
