#!/usr/bin/env python3
"""
i2vm - migrate InfluxDB 2.x data to VictoriaMetrics and verify it, series by series.

  export   influxd inspect export-lp, one gzip file per time chunk (reads the TSM/WAL files directly)
  import   POST the exported line protocol to VictoriaMetrics /write (batched, gzip, retries)
  compare  per chunk and per series: point count, value sum, first/last timestamp (export vs. VM)
  migrate  export + import + compare in one go (default command of the Docker image)

Every option can also be given as an environment variable (shown in --help).
Progress is kept in <export-dir>/state.json, so an interrupted run resumes where it stopped and
chunks are never imported twice (unless --force).

SPDX-License-Identifier: MIT
"""
import argparse
import base64
import csv
import gzip
import json
import math
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

VERSION = "0.1.0"


def log(msg):
    print(f"[i2vm] {msg}", file=sys.stderr, flush=True)


# ============================================================================
# Time handling and chunking
# ============================================================================

def parse_time(s):
    if s is None or s == "":
        return None
    if s.lower() == "now":
        return datetime.now(timezone.utc).replace(microsecond=0)
    dt = datetime.fromisoformat(s.strip().replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def rfc3339(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def rfc3339_nano(dt, minus_ns=0):
    """RFC3339 with nanoseconds; minus_ns lets us build an inclusive end just before a boundary."""
    ns = int(dt.timestamp()) * 1_000_000_000 + dt.microsecond * 1000 - minus_ns
    sec, frac = divmod(ns, 1_000_000_000)
    base = datetime.fromtimestamp(sec, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{base}.{frac:09d}Z"


def ms(dt):
    return int(dt.timestamp() * 1000)


def _next_boundary(dt, chunk):
    if chunk == "year":
        return dt.replace(year=dt.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    if chunk == "month":
        y, m = (dt.year + 1, 1) if dt.month == 12 else (dt.year, dt.month + 1)
        return dt.replace(year=y, month=m, day=1, hour=0, minute=0, second=0, microsecond=0)
    if chunk == "week":
        d = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return d + timedelta(days=7 - d.weekday())  # next Monday
    if chunk == "day":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    raise ValueError(f"unknown chunk size {chunk}")


def _label(dt, chunk):
    return dt.strftime({"year": "%Y", "month": "%Y-%m", "week": "%Y-%m-%d", "day": "%Y-%m-%d"}[chunk])


def chunks(start, end, chunk):
    """Yield (label, chunk_start, chunk_end_exclusive) covering [start, end)."""
    cur = start
    while cur < end:
        nxt = min(_next_boundary(cur, chunk), end)
        yield _label(cur, chunk), cur, nxt
        cur = nxt


# ============================================================================
# State (resume support)
# ============================================================================

class State:
    def __init__(self, export_dir):
        self.path = os.path.join(export_dir, "state.json")
        self.data = {"version": VERSION, "chunks": {}}
        if os.path.exists(self.path):
            with open(self.path) as fh:
                self.data = json.load(fh)
            self.data.setdefault("chunks", {})

    def chunk(self, label):
        return self.data["chunks"].setdefault(label, {})

    def save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(self.data, fh, indent=2, sort_keys=True)
        os.replace(tmp, self.path)


def chunk_file(export_dir, label):
    return os.path.join(export_dir, f"{label}.lp.gz")


# ============================================================================
# HTTP helpers
# ============================================================================

def http(method, url, data=None, headers=None, user=None, password=None, timeout=600, retries=5):
    headers = dict(headers or {})
    if user:
        tok = base64.b64encode(f"{user}:{password or ''}".encode()).decode()
        headers["Authorization"] = "Basic " + tok
    delay = 2
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, data=data, method=method, headers=headers)
            return urllib.request.urlopen(req, timeout=timeout)
        except urllib.error.HTTPError as e:
            body = e.read()[:500].decode("utf-8", "replace")
            if 400 <= e.code < 500 and e.code != 429:
                raise RuntimeError(f"{method} {url} -> HTTP {e.code}: {body}") from None
            err = f"HTTP {e.code}: {body}"
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            err = str(e)
        if attempt == retries:
            raise RuntimeError(f"{method} {url} failed after {retries} attempts: {err}")
        log(f"  {method} failed ({err}), retry {attempt}/{retries - 1} in {delay}s")
        time.sleep(delay)
        delay = min(delay * 2, 60)


def vm_url(args, path, query=None):
    url = args.vm_url.rstrip("/") + path
    if query:
        url += "?" + urllib.parse.urlencode(query)
    return url


# ============================================================================
# InfluxDB API helpers (optional: only to resolve a bucket name / find the oldest point)
# ============================================================================

def influx_api(args, path, query=None, body=None, accept="application/json"):
    if not (args.influx_url and args.influx_token):
        raise RuntimeError("INFLUX_URL and INFLUX_TOKEN are needed for this")
    url = args.influx_url.rstrip("/") + path
    if query:
        url += "?" + urllib.parse.urlencode(query)
    headers = {"Authorization": f"Token {args.influx_token}", "Accept": accept}
    data = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode()
    with http("POST" if data else "GET", url, data=data, headers=headers, retries=3) as r:
        return r.read().decode()


def resolve_bucket(args):
    if args.bucket_id:
        return args.bucket_id
    if not args.bucket:
        data_dir = os.path.join(args.engine_path, "data")
        found = sorted(os.listdir(data_dir)) if os.path.isdir(data_dir) else []
        raise RuntimeError(
            "set BUCKET_ID, or BUCKET (name) together with INFLUX_URL and INFLUX_TOKEN. "
            f"Bucket IDs found in {data_dir}: {', '.join(found) or 'none'}")
    q = {"name": args.bucket}
    if args.influx_org:
        q["org"] = args.influx_org
    res = json.loads(influx_api(args, "/api/v2/buckets", q))
    buckets = res.get("buckets", [])
    if len(buckets) != 1:
        raise RuntimeError(f"bucket '{args.bucket}' not found (or ambiguous, set INFLUX_ORG)")
    args.bucket_id = buckets[0]["id"]
    log(f"bucket '{args.bucket}' -> id {args.bucket_id}")
    return args.bucket_id


def find_oldest(args):
    """Ask InfluxDB (Flux) for the oldest point in the bucket."""
    if not args.bucket:
        raise RuntimeError("START is not set; set it, or set BUCKET + INFLUX_URL + INFLUX_TOKEN to detect it")
    # keep only _time before merging the tables: _value differs in type (float/bool/string)
    flux = (f'from(bucket: "{args.bucket}") |> range(start: 0) |> first() '
            f'|> keep(columns: ["_time"]) |> group() |> min(column: "_time")')
    body = {"query": flux, "type": "flux", "dialect": {"header": True, "annotations": []}}
    q = {"org": args.influx_org} if args.influx_org else None
    out = influx_api(args, "/api/v2/query", q, body=body, accept="application/csv")
    rows = [r for r in csv.reader(out.splitlines()) if r]
    if len(rows) < 2:
        raise RuntimeError("bucket seems to be empty")
    idx = rows[0].index("_time")
    oldest = parse_time(rows[1][idx])
    log(f"oldest point in bucket: {rfc3339_nano(oldest)}")
    return oldest.replace(hour=0, minute=0, second=0, microsecond=0)


def window(args):
    start = parse_time(args.start)
    if start is None:
        start = find_oldest(args)
    end = parse_time(args.end) or datetime.now(timezone.utc).replace(microsecond=0)
    if end <= start:
        raise RuntimeError(f"END ({rfc3339(end)}) must be after START ({rfc3339(start)})")
    return start, end


# ============================================================================
# export
# ============================================================================

def cmd_export(args, state=None):
    state = state or State(args.export_dir)
    bucket_id = resolve_bucket(args)
    start, end = window(args)
    influxd = shutil.which("influxd")
    if not influxd:
        raise RuntimeError("influxd not found in PATH (use the Docker image, or install InfluxDB 2.x)")
    log(f"export bucket {bucket_id} from {args.engine_path}, {rfc3339(start)} .. {rfc3339(end)} per {args.chunk}")
    for label, cs, ce in chunks(start, end, args.chunk):
        st = state.chunk(label)
        path = chunk_file(args.export_dir, label)
        if st.get("exported") and os.path.exists(path) and not args.force:
            log(f"{label}: already exported, skipping")
            continue
        tmp = path + ".part"
        cmd = [influxd, "inspect", "export-lp",
               "--bucket-id", bucket_id,
               "--engine-path", args.engine_path,
               "--start", rfc3339_nano(cs),
               "--end", rfc3339_nano(ce, minus_ns=1),   # inclusive end, just before the next chunk
               "--compress",
               "--output-path", tmp]
        t0 = time.time()
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            raise RuntimeError(f"export-lp failed for {label}:\n{res.stderr.strip()}")
        os.replace(tmp, path)
        lines = count_lines(path)
        st.update({"exported": True, "imported": False, "start": rfc3339(cs), "end": rfc3339(ce),
                   "lines": lines, "bytes": os.path.getsize(path)})
        state.save()
        log(f"{label}: {lines} lines, {os.path.getsize(path) / 1e6:.1f} MB gz ({time.time() - t0:.1f}s)")


def count_lines(path):
    n = 0
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if line.strip() and not line.startswith("#"):
                n += 1
    return n


# ============================================================================
# import
# ============================================================================

def cmd_import(args, state=None):
    state = state or State(args.export_dir)
    labels = sorted(k for k, v in state.data["chunks"].items() if v.get("exported"))
    if not labels:
        raise RuntimeError(f"nothing exported in {args.export_dir} yet")
    q = {"db": args.vm_db} if args.vm_db else {}
    url = vm_url(args, "/write", q)
    log(f"import {len(labels)} chunks -> {url}")
    for label in labels:
        st = state.chunk(label)
        if st.get("imported") and not args.force:
            log(f"{label}: already imported, skipping")
            continue
        path = chunk_file(args.export_dir, label)
        t0, sent, batch, size = time.time(), 0, [], 0

        def flush():
            nonlocal batch, size, sent
            if not batch:
                return
            body = gzip.compress("".join(batch).encode("utf-8"), compresslevel=1)
            with http("POST", url, data=body, user=args.vm_user, password=args.vm_password,
                      headers={"Content-Encoding": "gzip", "Content-Type": "text/plain"}) as r:
                if r.status not in (200, 204):
                    raise RuntimeError(f"unexpected HTTP {r.status} from VictoriaMetrics")
            sent += len(batch)
            batch, size = [], 0

        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if not line.strip() or line.startswith("#"):
                    continue
                if not line.endswith("\n"):
                    line += "\n"
                batch.append(line)
                size += len(line)
                if size >= args.batch_bytes:
                    flush()
            flush()
        st["imported"] = True
        st["imported_lines"] = sent
        state.save()
        log(f"{label}: {sent} lines imported ({time.time() - t0:.1f}s)")
    vm_flush(args)


def vm_flush(args):
    """Make freshly written data searchable and drop cached query results (backfill!)."""
    for path in ("/internal/force_flush", "/internal/resetRollupResultCache"):
        try:
            with http("GET", vm_url(args, path), user=args.vm_user, password=args.vm_password, retries=2):
                pass
        except RuntimeError as e:
            log(f"note: {path} not available ({e}); with vmcluster/vmauth call it on vmselect/vminsert yourself")


# ============================================================================
# Line protocol parsing (for compare)
# ============================================================================

def _split_unescaped(s, sep, respect_quotes=False):
    out, cur, i, n, in_q = [], [], 0, len(s), False
    while i < n:
        c = s[i]
        if c == "\\" and i + 1 < n:
            cur.append(c)
            cur.append(s[i + 1])
            i += 2
            continue
        if respect_quotes and c == '"':
            in_q = not in_q
        elif c == sep and not in_q:
            out.append("".join(cur))
            cur = []
            i += 1
            continue
        cur.append(c)
        i += 1
    out.append("".join(cur))
    return out


def _unescape(s):
    if "\\" not in s:
        return s
    out, i = [], 0
    while i < len(s):
        if s[i] == "\\" and i + 1 < len(s) and s[i + 1] in ', ="\\':
            out.append(s[i + 1])
            i += 2
        else:
            out.append(s[i])
            i += 1
    return "".join(out)


STRING = object()  # marker for string field values


def _field_value(v):
    """Float for numeric/bool fields, STRING for string fields."""
    if not v:
        return None
    if v[0] == '"':
        return STRING
    if v[-1] in "iu":
        return float(v[:-1])
    lv = v.lower()
    if lv in ("t", "true"):
        return 1.0
    if lv in ("f", "false"):
        return 0.0
    return float(v)


def parse_line(line):
    """Return (measurement, [(tag, value)], [(field, float_or_None)], ts_ns) or None."""
    if "\\" not in line and '"' not in line:
        parts = line.split(" ")
    else:
        parts = _split_unescaped(line, " ", respect_quotes=True)
    parts = [p for p in parts if p != ""]
    if len(parts) < 3:
        return None  # export-lp always writes a timestamp
    series, fields, ts = parts[0], parts[1], int(parts[2])

    sp = _split_unescaped(series, ",") if "\\" in series else series.split(",")
    meas = _unescape(sp[0])
    tags = []
    for t in sp[1:]:
        kv = _split_unescaped(t, "=")
        if len(kv) >= 2:
            k, v = _unescape(kv[0]), _unescape("=".join(kv[1:]))
            if v != "":
                tags.append((k, v))

    fl = (_split_unescaped(fields, ",", respect_quotes=True)
          if ("\\" in fields or '"' in fields) else fields.split(","))
    out = []
    for f in fl:
        kv = _split_unescaped(f, "=", respect_quotes=True)
        if len(kv) < 2:
            continue
        try:
            val = _field_value("=".join(kv[1:]))
        except ValueError:
            val = None
        out.append((_unescape(kv[0]), val))
    return meas, tags, out, ts


# ============================================================================
# compare
# ============================================================================

class Agg:
    __slots__ = ("count", "sum", "first", "last")

    def __init__(self):
        self.count, self.sum, self.first, self.last = 0, 0.0, None, None

    def add(self, v, ts_ms):
        self.count += 1
        if v is not None and math.isfinite(v):
            self.sum += v
        if self.first is None or ts_ms < self.first:
            self.first = ts_ms
        if self.last is None or ts_ms > self.last:
            self.last = ts_ms


def series_key(name, labels):
    return name + "{" + ",".join(f'{k}="{v}"' for k, v in sorted(labels)) + "}"


def fmt_ms(v):
    if v is None:
        return ""
    return datetime.fromtimestamp(v / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def metric_name(args, meas, field, nfields):
    """Mirror VictoriaMetrics' InfluxDB naming (-influxSkipMeasurement / -influxSkipSingleField / separator)."""
    if args.skip_measurement:
        return field
    if args.skip_single_field and nfields == 1:
        return meas
    return meas + args.separator + field


def aggregate_lp(args, path, lo_ms, hi_ms):
    aggs, bad = {}, 0
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
        for raw in fh:
            line = raw.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            try:
                p = parse_line(line)
            except Exception:
                p = None
            if p is None:
                bad += 1
                if bad <= 5:
                    log(f"  WARN unparsable line: {line[:200]}")
                continue
            meas, tags, fields, ts_ns = p
            ts_ms = ts_ns // 1_000_000  # VictoriaMetrics stores milliseconds
            if ts_ms < lo_ms or ts_ms > hi_ms:
                continue
            labels = list(tags)
            if args.vm_db and not any(k == args.db_label for k, _ in labels):
                labels.append((args.db_label, args.vm_db))
            for fk, val in fields:
                if val is STRING:
                    # VictoriaMetrics stores string fields as a series with value 0
                    if args.string_fields == "skip":
                        continue
                    val = 0.0
                elif val is None:
                    continue
                k = series_key(metric_name(args, meas, fk, len(fields)), labels)
                a = aggs.get(k)
                if a is None:
                    a = aggs[k] = Agg()
                a.add(val, ts_ms)
    return aggs, bad


def aggregate_vm(args, lo_ms, hi_ms):
    match = args.match or (f'{{{args.db_label}="{args.vm_db}"}}' if args.vm_db else '{__name__=~".+"}')
    q = {"match[]": match, "start": f"{lo_ms / 1000:.3f}", "end": f"{hi_ms / 1000:.3f}"}
    aggs = {}
    with http("GET", vm_url(args, "/api/v1/export", q), user=args.vm_user, password=args.vm_password,
              timeout=args.timeout) as resp:
        for raw in resp:
            if not raw.strip():
                continue
            obj = json.loads(raw)
            m = dict(obj["metric"])
            name = m.pop("__name__", "")
            k = series_key(name, m.items())
            a = aggs.get(k)
            if a is None:
                a = aggs[k] = Agg()
            for v, ts in zip(obj["values"], obj["timestamps"]):
                if lo_ms <= int(ts) <= hi_ms:
                    a.add(None if v is None else float(v), int(ts))
    return aggs


def compare_chunk(args, label, cs, ce, writer):
    lo, hi = ms(cs), ms(ce) - 1
    lp, bad = aggregate_lp(args, chunk_file(args.export_dir, label), lo, hi)
    vm = aggregate_vm(args, lo, hi)
    stats = {}
    for k in sorted(set(lp) | set(vm)):
        a, b = lp.get(k), vm.get(k)
        if a and not b:
            status = "MISSING_IN_VM"
        elif b and not a:
            status = "EXTRA_IN_VM"
        else:
            probs = []
            if a.count != b.count:
                probs.append("COUNT")
            if not math.isclose(a.sum, b.sum, rel_tol=args.rel_tol, abs_tol=1e-9):
                probs.append("SUM")
            if a.first != b.first or a.last != b.last:
                probs.append("RANGE")
            status = "OK" if not probs else "DIFF_" + "+".join(probs)
        stats[status] = stats.get(status, 0) + 1
        writer.writerow([
            label, status, k,
            a.count if a else "", b.count if b else "",
            (b.count - a.count) if (a and b) else "",
            repr(a.sum) if a else "", repr(b.sum) if b else "",
            fmt_ms(a.first) if a else "", fmt_ms(b.first) if b else "",
            fmt_ms(a.last) if a else "", fmt_ms(b.last) if b else "",
        ])
    pts_lp = sum(a.count for a in lp.values())
    pts_vm = sum(b.count for b in vm.values())
    return stats, pts_lp, pts_vm, bad


def cmd_compare(args, state=None):
    state = state or State(args.export_dir)
    labels = sorted(k for k, v in state.data["chunks"].items() if v.get("exported"))
    if not labels:
        raise RuntimeError(f"nothing exported in {args.export_dir} yet")
    report = args.report or os.path.join(args.export_dir, "compare_report.csv")
    total, tot_lp, tot_vm, failed = {}, 0, 0, []
    with open(report, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["chunk", "status", "series", "export_count", "vm_count", "count_delta",
                    "export_sum", "vm_sum", "export_first", "vm_first", "export_last", "vm_last"])
        for label in labels:
            st = state.chunk(label)
            cs, ce = parse_time(st["start"]), parse_time(st["end"])
            stats, p_lp, p_vm, bad = compare_chunk(args, label, cs, ce, w)
            ok = stats.get("OK", 0)
            n = sum(stats.values())
            flag = "OK  " if ok == n else "DIFF"
            log(f"{flag} {label}: {ok}/{n} series ok, points export {p_lp} / VM {p_vm}"
                + (f", {bad} unparsable lines" if bad else ""))
            if ok != n:
                failed.append(label)
            st["compare"] = stats
            for s, c in stats.items():
                total[s] = total.get(s, 0) + c
            tot_lp += p_lp
            tot_vm += p_vm
    state.save()
    log("")
    log(f"points: export {tot_lp}, VM {tot_vm}, delta {tot_vm - tot_lp:+d}")
    for s in sorted(total, key=lambda s: (s != "OK", s)):
        log(f"  {s:<24} {total[s]}")
    log(f"report: {report}")
    if failed:
        log(f"chunks with differences: {', '.join(failed)}")
        return 1
    log("all series match")
    return 0


# ============================================================================
# migrate
# ============================================================================

def cmd_migrate(args):
    state = State(args.export_dir)
    cmd_export(args, state)
    cmd_import(args, state)
    if args.no_compare:
        return 0
    return cmd_compare(args, state)


# ============================================================================
# CLI
# ============================================================================

def env(name, default=None):
    v = os.environ.get(name)
    return v if v not in (None, "") else default


def env_bool(name):
    return env(name, "false").lower() in ("1", "true", "yes", "on")


def build_parser():
    p = argparse.ArgumentParser(prog="i2vm", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--version", action="version", version=f"i2vm {VERSION}")
    sub = p.add_subparsers(dest="command")

    common = argparse.ArgumentParser(add_help=False)
    g = common.add_argument_group("paths")
    g.add_argument("--export-dir", default=env("EXPORT_DIR", "/export"),
                   help="where chunk files, state.json and the report go [EXPORT_DIR, /export]")

    src = argparse.ArgumentParser(add_help=False)
    g = src.add_argument_group("InfluxDB 2.x source")
    g.add_argument("--engine-path", default=env("ENGINE_PATH", "/var/lib/influxdb2/engine"),
                   help="InfluxDB engine dir (mount it read-only) [ENGINE_PATH, /var/lib/influxdb2/engine]")
    g.add_argument("--bucket-id", default=env("BUCKET_ID"), help="bucket ID [BUCKET_ID]")
    g.add_argument("--bucket", default=env("BUCKET"),
                   help="bucket name, resolved to an ID via the API [BUCKET]")
    g.add_argument("--influx-url", default=env("INFLUX_URL"),
                   help="only needed for --bucket / auto START, e.g. http://influxdb:8086 [INFLUX_URL]")
    g.add_argument("--influx-token", default=env("INFLUX_TOKEN"), help="[INFLUX_TOKEN]")
    g.add_argument("--influx-org", default=env("INFLUX_ORG"), help="[INFLUX_ORG]")

    win = argparse.ArgumentParser(add_help=False)
    g = win.add_argument_group("time window")
    g.add_argument("--start", default=env("START"),
                   help="RFC3339; default: oldest point (needs BUCKET + INFLUX_URL + INFLUX_TOKEN) [START]")
    g.add_argument("--end", default=env("END"),
                   help="RFC3339 or 'now', exclusive. Set it to the moment your writers switched to VM, "
                        "so the backfill does not overlap live data [END, now]")
    g.add_argument("--chunk", default=env("CHUNK", "month"), choices=["year", "month", "week", "day"],
                   help="size of one export file / compare unit [CHUNK, month]")
    g.add_argument("--force", action="store_true", default=env_bool("FORCE"),
                   help="redo chunks that state.json marks as done [FORCE]")

    vm = argparse.ArgumentParser(add_help=False)
    g = vm.add_argument_group("VictoriaMetrics")
    g.add_argument("--vm-url", default=env("VM_URL", "http://victoriametrics:8428"),
                   help="single-node URL (or vminsert/vmselect behind vmauth) [VM_URL, http://victoriametrics:8428]")
    g.add_argument("--vm-db", default=env("VM_DB"),
                   help="value for /write?db=... - must match the db label of your live data, empty = none [VM_DB]")
    g.add_argument("--vm-user", default=env("VM_USER"), help="basic auth [VM_USER]")
    g.add_argument("--vm-password", default=env("VM_PASSWORD"), help="[VM_PASSWORD]")
    g.add_argument("--batch-bytes", type=int, default=int(env("BATCH_BYTES", str(8 * 1024 * 1024))),
                   help="uncompressed bytes per /write request [BATCH_BYTES, 8 MiB]")

    cmp_ = argparse.ArgumentParser(add_help=False)
    g = cmp_.add_argument_group("compare (mirror your VictoriaMetrics flags)")
    g.add_argument("--db-label", default=env("DB_LABEL", "db"), help="VM -influxDBLabel [DB_LABEL, db]")
    g.add_argument("--separator", default=env("SEPARATOR", "_"),
                   help="VM -influxMeasurementFieldSeparator [SEPARATOR, _]")
    g.add_argument("--skip-measurement", action="store_true", default=env_bool("SKIP_MEASUREMENT"),
                   help="VM runs with -influxSkipMeasurement [SKIP_MEASUREMENT]")
    g.add_argument("--skip-single-field", action="store_true", default=env_bool("SKIP_SINGLE_FIELD"),
                   help="VM runs with -influxSkipSingleField [SKIP_SINGLE_FIELD]")
    g.add_argument("--match", default=env("MATCH"),
                   help='series selector for the VM side, default {db="<VM_DB>"} [MATCH]')
    g.add_argument("--string-fields", default=env("STRING_FIELDS", "zero"), choices=["zero", "skip"],
                   help="VictoriaMetrics stores string fields (e.g. HA's friendly_name_str) as series with "
                        "value 0: 'zero' expects them, 'skip' if you drop them via relabeling [STRING_FIELDS, zero]")
    g.add_argument("--rel-tol", type=float, default=float(env("REL_TOL", "1e-9")),
                   help="relative tolerance for sums [REL_TOL, 1e-9]")
    g.add_argument("--report", default=env("REPORT"), help="CSV path [REPORT, <export-dir>/compare_report.csv]")
    g.add_argument("--timeout", type=int, default=int(env("TIMEOUT", "3600")), help="[TIMEOUT, 3600]")

    sub.add_parser("export", parents=[common, src, win], help="export chunks with influxd inspect export-lp")
    sub.add_parser("import", parents=[common, vm, win], help="import exported chunks into VictoriaMetrics")
    sub.add_parser("compare", parents=[common, vm, cmp_, win], help="compare exported chunks with VictoriaMetrics")
    m = sub.add_parser("migrate", parents=[common, src, win, vm, cmp_], help="export + import + compare")
    m.add_argument("--no-compare", action="store_true", default=env_bool("NO_COMPARE"), help="[NO_COMPARE]")
    return p


def main(argv=None):
    p = build_parser()
    args = p.parse_args(argv)
    if not args.command:
        p.print_help()
        return 2
    os.makedirs(args.export_dir, exist_ok=True)
    log(f"i2vm {VERSION}: {args.command}")
    try:
        if args.command == "export":
            cmd_export(args)
            return 0
        if args.command == "import":
            cmd_import(args)
            return 0
        if args.command == "compare":
            return cmd_compare(args)
        return cmd_migrate(args)
    except RuntimeError as e:
        log(f"ERROR: {e}")
        return 1
    except KeyboardInterrupt:
        log("interrupted - run again to resume")
        return 130


if __name__ == "__main__":
    sys.exit(main())
