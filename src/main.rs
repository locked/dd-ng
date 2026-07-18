mod proto;
mod recv;
mod send;
mod util;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "dd-ng", version, about = "Parallel dd over multiple SSH TCP flows")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Send a local file (or stdin with `-`) to a remote host over N parallel SSH flows.
    Send {
        /// Local input file, or "-" for stdin (framed mode).
        input: String,
        /// Destination: [user@]host:/remote/path
        remote: String,
        /// Number of parallel data streams.
        #[arg(short = 'n', long, default_value_t = 8)]
        streams: u32,
        /// I/O block size. Accepts unit suffixes: K, M, G, T (1024-based),
        /// or explicit KiB/MiB/GiB. Default 4M.
        #[arg(short = 'b', long, default_value = "4M", value_parser = parse_size_arg)]
        block_size: u64,
        /// Path to dd-ng on the remote host.
        #[arg(long, default_value = "dd-ng")]
        remote_bin: String,
        /// Local ssh binary.
        #[arg(long, default_value = "ssh")]
        ssh_bin: String,
        /// Extra args passed to ssh (repeatable), e.g. --ssh-opt=-p --ssh-opt=2222
        #[arg(long = "ssh-opt")]
        ssh_opt: Vec<String>,
        /// Delay in ms between spawning successive data SSH connections.
        #[arg(long, default_value_t = 20)]
        stream_delay_ms: u64,
        /// Do NOT inject the default safe ssh options
        /// (ControlMaster=no, ControlPath=none, Compression=no, keepalives).
        #[arg(long)]
        no_ssh_defaults: bool,
        /// Verbose: default is silent; -v shows progress + summary lines,
        /// -vv also shows ssh commands, -vvv also shows control-channel messages.
        #[arg(short = 'v', long, action = clap::ArgAction::Count)]
        verbose: u8,
        /// fsync the output on the receiver before ack. Slower but durable.
        #[arg(long)]
        sync: bool,
        /// Open the output with O_DIRECT on the receiver (bypass page cache).
        /// Range mode only; requires 4 KiB alignment. Eliminates the final
        /// fsync drain on large writes to block devices.
        #[arg(long)]
        direct: bool,
        /// Skip CRC32C verification (per-stream in Range mode, per-frame in
        /// Framed mode). TCP still protects individual segments; use this to
        /// isolate whether CRC computation is the bottleneck.
        #[arg(long)]
        no_crc: bool,
        /// Collect and print per-stage telemetry (src read / net / dst write).
        /// Adds a busy% + queue-depth summary to progress, plus a final table.
        #[arg(long)]
        stats: bool,
    },
    /// Receiver control role (invoked over SSH by the sender).
    RecvCtrl,
    /// Receiver data role (invoked over SSH by the sender).
    RecvData {
        #[arg(long)]
        token: String,
        #[arg(long)]
        id: u32,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Send {
            input,
            remote,
            streams,
            block_size,
            remote_bin,
            ssh_bin,
            ssh_opt,
            stream_delay_ms,
            no_ssh_defaults,
            verbose,
            sync,
            direct,
            no_crc,
            stats,
        } => send::run(send::SendOpts {
            input,
            remote,
            streams,
            block_size,
            remote_bin,
            ssh_bin,
            extra_ssh: ssh_opt,
            stream_delay_ms,
            no_ssh_defaults,
            sync,
            verbose,
            direct,
            no_crc,
            stats,
        }),
        Cmd::RecvCtrl => recv::run_ctrl(),
        Cmd::RecvData { token, id } => recv::run_data(&token, id),
    }
}

fn parse_size_arg(s: &str) -> Result<u64, String> {
    util::parse_size(s).map_err(|e| e.to_string())
}
