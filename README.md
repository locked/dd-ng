# dd-ng

A `dd`-over-SSH replacement that splits a transfer across N parallel SSH TCP
flows to overcome single-flow bandwidth-delay-product (BDP) limits.

One SSH connection carries the control channel (a small JSON protocol); N more
carry the actual data. On the receiver, streams rendezvous over a local Unix
socket and pwrite into the output file (or block device) in parallel.

## Benchmark

30 GiB copy between two hosts across a WAN link, LV to LV
(`/dev/vg/test` → `/dev/vg/test`).

### Plain `dd | ssh` — **47 MB/s**

```
$ dd if=/dev/vg/test status=progress | ssh target-host 'dd of=/dev/vg/test'
32182538240 bytes (32 GB, 30 GiB) copied, 681 s, 47.3 MB/s
62914560+0 records in
62914560+0 records out
32212254720 bytes (32 GB, 30 GiB) copied, 683.581 s, 47.1 MB/s
62914560+0 records in
62914560+0 records out
32212254720 bytes (32 GB, 30 GiB) copied, 687.964 s, 46.8 MB/s
```

### `dd-ng -n 8 --direct` — **427 MB/s (~9× faster)**

```
$ dd-ng send --direct -n 8 /dev/vg/test target-host:/dev/vg/test
[send] mode=Range /dev/vg/test -> target-host:/dev/vg/test  size=32212254720  streams=8
[send] remote ready
[send] 30.00 GiB/30.00 GiB (100.0%)  cur 0.00 B/s  avg 406.81 MiB/s  eta 0s
[send] all bytes sent; waiting for remote finalize...
[send] done: 32212254720 bytes in 75.52s (426.6 MB/s)
```

## Usage

`dd-ng` must be installed at the same path on both hosts (or use
`--remote-bin`).

```
# Seekable source (regular file or block device): parallel byte-range mode.
dd-ng send -n 8 SRC user@host:DST

# stdin -> remote file: framed mode (per-frame CRC, work-stealing dispatch).
some-cmd | dd-ng send -n 4 - user@host:DST
```

Useful flags:

| flag                  | meaning                                                            |
|-----------------------|--------------------------------------------------------------------|
| `-n N`                | number of parallel data streams (default 4)                        |
| `-b SIZE`             | block size per pwrite (default 4 MiB)                              |
| `--direct`            | receiver opens output with `O_DIRECT` (bypass page cache)          |
| `--sync`              | fsync output before ack (durability; adds fsync-drain tail)        |
| `--stream-delay-ms`   | stagger data connections to avoid sshd `MaxStartups` / fail2ban    |
| `--ssh-opt '-o KEY=V'`| extra ssh options (repeatable)                                     |
| `-v`, `-vv`           | verbose: print ssh commands; `-vv` also traces control messages    |
| `-q`                  | suppress live progress                                             |

## When it helps

A single TCP connection can only have a limited amount of data "in flight"
(unacknowledged) at any moment — capped by the TCP window (Linux typically
auto-tunes up to a few MiB). Its maximum throughput is roughly:

```
single_flow_throughput ≈ TCP_window / round_trip_time
```

On a low-latency LAN that number is huge, and one flow easily saturates the
link. But on a long-distance path the round-trip time dominates: with a 4 MiB
window and 80 ms RTT, a single flow tops out around 50 MB/s no matter how fat
the underlying pipe is. The link sits idle waiting for ACKs to come back.

Opening N parallel connections gives you N independent windows, so aggregate
throughput scales roughly linearly until you actually hit the link's capacity.
That's the trick `dd-ng` uses, and it's why the WAN benchmark above jumps from
47 MB/s (one flow) to 427 MB/s (eight flows).

Quick check for any given path:

```
iperf3 -c HOST -P 1        # one flow
iperf3 -c HOST -P 8        # eight flows
```

If `-P 8` is substantially faster than `-P 1`, `dd-ng` will help. If they're
similar, plain `dd | ssh` is already close to optimal and parallelism just
adds overhead.

## Notes on `--direct`

Writing to large block devices without `--direct` looks fast at first (bytes
land in the receiver's page cache at RAM speed) but stalls near the end as
dirty pages drain to the physical device. `--direct` sends writes straight to
the device: sender-side "100%" actually means "done", and there is no
`fsync` tail.

Requires the total size, block size, and range offsets to be multiples of
4 KiB (satisfied automatically for block devices). Range mode only.

## Build

```
cargo build --release
```
