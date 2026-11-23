# influx_2_to_victoriametrics

Migrate historical data from **InfluxDB 2.x** to **VictoriaMetrics**, and **verify it series by series** afterwards.

`vmctl` only supports InfluxDB 1.x, and the existing InfluxDB 2.x scripts query through Flux and rebuild the line protocol themselves, which goes wrong with string fields, escaping and series that have different tag sets. This tool skips all of that:

1. **export**: `influxd inspect export-lp` reads the TSM/WAL files directly and writes exact line protocol, one gzip file per month (or year/week/day).
2. **import**: the files are POSTed to VictoriaMetrics' `/write`, which is the same endpoint your live writers use, so backfilled and live series get identical names and labels. Batched, gzip, with retries.
3. **compare**: for every chunk and every series, it compares the **point count, value sum, first and last timestamp** of the export against what VictoriaMetrics returns from `/api/v1/export`. The result is a CSV report, and the exit code is 1 if anything differs.

Progress is kept in `state.json`: an interrupted run resumes where it stopped, and a chunk is never imported twice.

Everything runs in one Docker image based on the official `influxdb:2-alpine` image. The tool itself (`i2vm.py`) is a single Python file with no dependencies outside the standard library.

## Quick start

```bash
docker run --rm --network <your-docker-network> \
  -v influxdb2-data:/var/lib/influxdb2:ro \
  -v "$PWD/export:/export" \
  -e BUCKET=home_assistant \
  -e INFLUX_URL=http://influxdb:8086 \
  -e INFLUX_TOKEN=<token> \
  -e END=2026-11-01T00:00:00Z \
  -e VM_URL=http://victoriametrics:8428 \
  -e VM_DB=home_assistant \
  ghcr.io/isarrider/influx_2_to_victoriametrics:latest
```

* `influxdb2-data` is InfluxDB's data volume (or a host path such as `/srv/influxdb2:/var/lib/influxdb2:ro`). Always mount it **read-only**.
* The default command is `migrate` (export + import + compare). To run a single step, append `export`, `import` or `compare`.
* When it finishes, `./export` holds `YYYY-MM.lp.gz` files, `state.json` and `compare_report.csv`.

A compose example is in [`docker-compose.example.yml`](docker-compose.example.yml).

## Before you start

| Check | Why | Risk if skipped |
|---|---|---|
| VictoriaMetrics `-retentionPeriod` covers your oldest data (e.g. `100y`) | VM silently drops samples older than the retention period | 🔴 Data lost without an error, but compare reports `MISSING_IN_VM` |
| `END` = the moment your writers switched to VictoriaMetrics | Otherwise the backfill overlaps data VM already has, so you get duplicates | 🟡 Compare reports `DIFF_COUNT` / `EXTRA_IN_VM` |
| `VM_DB` gives the same `db` label as your live data | VM adds `db=<VM_DB>` from `/write?db=…`; a different value means separate series | 🟡 History and live data show up as two series in Grafana |
| Your VM InfluxDB flags are mirrored (see [Configuration](#configuration)) | Compare has to predict VM's metric names | 🟢 Only affects the compare |
| InfluxDB may keep running | Export reads the files read-only. Once your writers point to VM, the data no longer changes anyway | 🟢 – |

To check which `db` label your live series have:

```bash
curl -s 'http://<vm>:8428/api/v1/series?match[]={__name__=~".+"}&limit=5'
```

## Configuration

Every option is an environment variable (or a CLI flag, see `i2vm migrate --help`).

| Variable | Default | Meaning |
|---|---|---|
| **Source** | | |
| `ENGINE_PATH` | `/var/lib/influxdb2/engine` | InfluxDB engine directory |
| `BUCKET_ID` | – | Bucket ID. Either this… |
| `BUCKET` | – | …or the bucket name, resolved via the API (needs `INFLUX_URL` + `INFLUX_TOKEN`) |
| `INFLUX_URL` / `INFLUX_TOKEN` / `INFLUX_ORG` | – | Only needed for `BUCKET` and START auto-detection |
| **Time window** | | |
| `START` | oldest point | RFC3339. Empty = ask InfluxDB for the oldest point (needs the API settings) |
| `END` | `now` | RFC3339, exclusive. Set it to your cut-over time |
| `CHUNK` | `month` | `year`, `month`, `week` or `day`: size of one export file / compare unit |
| `FORCE` | `false` | Redo chunks that `state.json` marks as done (re-importing creates duplicates, see below) |
| **Destination** | | |
| `VM_URL` | `http://victoriametrics:8428` | VictoriaMetrics single-node URL (or vmauth in front of a cluster) |
| `VM_DB` | – | Sent as `/write?db=…` and becomes the `db` label. Empty = no label |
| `VM_USER` / `VM_PASSWORD` | – | Basic auth |
| `BATCH_BYTES` | `8388608` | Uncompressed bytes per `/write` request |
| **Compare** | | |
| `DB_LABEL` | `db` | VM `-influxDBLabel` |
| `SEPARATOR` | `_` | VM `-influxMeasurementFieldSeparator` |
| `SKIP_MEASUREMENT` | `false` | VM runs with `-influxSkipMeasurement` |
| `SKIP_SINGLE_FIELD` | `false` | VM runs with `-influxSkipSingleField` |
| `STRING_FIELDS` | `zero` | `zero`: expect string fields as series with value 0 (VM default). `skip`: you drop them via relabeling |
| `MATCH` | `{db="<VM_DB>"}` | Series selector for the VM side |
| `REL_TOL` | `1e-9` | Relative tolerance for comparing sums |
| `NO_COMPARE` | `false` | `migrate` without the compare step |
| `EXPORT_DIR` | `/export` | Where chunk files, `state.json` and the report go |

## Reading the compare report

`compare_report.csv` has one row per chunk and series: `chunk, status, series, export_count, vm_count, count_delta, export_sum, vm_sum, export_first, vm_first, export_last, vm_last`.

| Status | Meaning | Usual cause | Fix |
|---|---|---|---|
| 🟢 `OK` | Count, sum, first and last timestamp identical | – | – |
| 🔴 `MISSING_IN_VM` | Series exists in the export, not in VM | Retention too short, chunk not imported, series dropped by relabeling | Check `-retentionPeriod`, run `import` again (state shows which chunks are done) |
| 🟡 `EXTRA_IN_VM` | Series in VM but not in the export | Live data in the window (`END` too late), naming flags differ, string fields with `STRING_FIELDS=skip` | Fix `END` / naming flags |
| 🟡 `DIFF_COUNT` (VM has more) | Duplicates | A chunk was imported twice (`FORCE`), or the window overlaps live data | Run VM with `-dedup.minScrapeInterval=1ms`, which drops samples with identical timestamps |
| 🟡 `DIFF_COUNT` (VM has fewer) | Points merged | Several points within the same millisecond (VM stores ms, InfluxDB ns) together with dedup | Usually harmless |
| 🔴 `DIFF_SUM` with equal count | Values changed | Should not happen, so look at the series in vmui | – |
| 🟡 `DIFF_RANGE` | First/last timestamp differ | Follows from one of the above | – |

## What VictoriaMetrics does with InfluxDB data

This was verified end-to-end against real InfluxDB 2.7 / 2.x and VictoriaMetrics in CI ([`tests/integration.sh`](tests/integration.sh)):

| Influx line protocol | Stored in VictoriaMetrics |
|---|---|
| `°C,entity_id=x value=21.5` | `°C_value{db="…",entity_id="x"} 21.5`, i.e. `<measurement>_<field>`, tags become labels |
| Escaped spaces/commas (`entity_id=pv\ power,zone=a\,b`), spaces in measurement names | Unescaped label values / names, kept 1:1 |
| `on=true` / `on=false` | `state_on` with 1 / 0 |
| `starts=5i` | `state_starts` with 5 |
| **String fields** (`friendly_name_str="Wohnzimmer"`) | ⚠️ **A series with value 0 for every point**, e.g. `°C_friendly_name_str` |
| Nanosecond timestamps | Truncated to milliseconds |
| Series with different tag sets | Each keeps exactly its own tags |

### Home Assistant: string attributes

Home Assistant's InfluxDB integration writes every attribute as a `*_str` field. In VictoriaMetrics these turn into series of zeros, for the backfill **and** for your live data. To get rid of them, drop them with VM relabeling (`-relabelConfig=/etc/vm/relabel.yml`):

```yaml
- action: drop
  if: '{__name__=~".+_str"}'
```

Then run the compare with `STRING_FIELDS=skip`. Alternatively, limit what HA writes with the integration's `ignore_attributes` option.

### Grafana

VictoriaMetrics doesn't speak InfluxQL or Flux. Dashboards need a Prometheus-type datasource pointing at VM and MetricsQL queries, e.g. `mean("value") … GROUP BY time($__interval)` becomes `avg_over_time(°C_value{entity_id="wohnzimmer_temp"}[$__interval])`. Metric names with special characters such as `°C_value` must be written as `{__name__="°C_value", …}`.

Graphs can look different from InfluxDB even with identical data (see the VictoriaMetrics [migration guide](https://docs.victoriametrics.com/guides/migrate-from-influx/)):

| Difference | InfluxDB | VictoriaMetrics | What to do |
|---|---|---|---|
| Gaps in the data | Shown as gaps (unless `fill(previous)`) | Filled by default (series are treated as continuous) | Start VM with `-search.setLookbackToStep` to fill at most one step |
| `GROUP BY time(…)` | In the query | The `step` / `$__interval` of the range query | `avg_over_time(m[$__interval])` |
| Extra points | – | Appear if the step is smaller than the data resolution | Set the panel's *Min interval* to the sensor's write interval |
| Real last value | `last()` | `last_over_time(m[…])` / `tlast_over_time` | Use it for stat panels (e.g. meter readings) |

## Re-running and resuming

* Running the same command again skips every chunk that `state.json` marks as exported / imported, and then compares again. That's safe at any time.
* `compare` on its own can be repeated as often as you like.
* `FORCE=true` redoes the export **and the import**. Without `-dedup.minScrapeInterval` in VM this doubles the points of those chunks, and compare will show it.
* To redo a single chunk, delete its entry from `state.json` (and the `.lp.gz` file) and run again.
* After importing, the tool calls `/internal/force_flush` and `/internal/resetRollupResultCache`, so the backfilled data is visible immediately and Grafana doesn't show cached gaps.

## Without Docker

```bash
# needs influxd (InfluxDB 2.x) in PATH for the export step, Python 3.9+
./i2vm.py migrate --engine-path ~/.influxdbv2/engine --bucket-id <id> \
  --start 2023-01-01T00:00:00Z --end 2026-11-01T00:00:00Z \
  --vm-url http://localhost:8428 --vm-db home_assistant --export-dir ./export
```

`import` and `compare` don't need `influxd`, so they can run anywhere the export files are.

## Image

* `ghcr.io/isarrider/influx_2_to_victoriametrics:latest` is built from `main` for `linux/amd64` and `linux/arm64` (Raspberry Pi 4/5, Rock 5).
* Version tags `vX.Y.Z` publish `X.Y.Z` and `X.Y`.
* Build it yourself, optionally matching your InfluxDB version (the export tool should be the same version as, or newer than, the InfluxDB that wrote the data):

  ```bash
  docker build --build-arg INFLUXDB_TAG=2.7-alpine -t i2vm .
  ```

## Tests

On every push, [`tests/integration.sh`](tests/integration.sh) runs against InfluxDB `2.7-alpine` and `2-alpine`:

* it starts InfluxDB 2 and VictoriaMetrics, and writes three months of Home-Assistant-like data (floats, ints, bools, string fields, escaped tags, a measurement with a space, a series with a different tag set, sub-millisecond timestamps)
* `migrate` with the engine volume mounted read-only **while InfluxDB is running**, START auto-detected → every series must be `OK`
* a second run must skip all chunks (resume)
* negative test: after one series is deleted in VM, `compare` must fail and report `MISSING_IN_VM`

The image is published only when the tests pass.

## License

MIT, see [LICENSE](LICENSE).
