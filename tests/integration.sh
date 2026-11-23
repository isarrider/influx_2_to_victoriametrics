#!/usr/bin/env bash
# End-to-end test: real InfluxDB 2 + VictoriaMetrics containers, run the i2vm image against them.
#   tests/integration.sh <i2vm-image> [influxdb-image] [victoriametrics-image]
set -euo pipefail

IMAGE=${1:?usage: $0 <i2vm-image> [influxdb-image] [vm-image]}
INFLUX_IMAGE=${2:-influxdb:2-alpine}
VM_IMAGE=${3:-victoriametrics/victoria-metrics:latest}
NET=i2vm-test
TOKEN=i2vm-test-token
HERE=$(cd "$(dirname "$0")" && pwd)
WORK=$(mktemp -d)

cleanup() {
  docker rm -f i2vm-influx i2vm-vm >/dev/null 2>&1 || true
  docker volume rm i2vm-influx-data >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

docker network create "$NET" >/dev/null
docker volume create i2vm-influx-data >/dev/null

echo "== start InfluxDB ($INFLUX_IMAGE) and VictoriaMetrics ($VM_IMAGE)"
docker run -d --name i2vm-influx --network "$NET" --network-alias influxdb \
  -v i2vm-influx-data:/var/lib/influxdb2 \
  -e DOCKER_INFLUXDB_INIT_MODE=setup \
  -e DOCKER_INFLUXDB_INIT_USERNAME=admin \
  -e DOCKER_INFLUXDB_INIT_PASSWORD=adminadmin123 \
  -e DOCKER_INFLUXDB_INIT_ORG=test \
  -e DOCKER_INFLUXDB_INIT_BUCKET=home_assistant \
  -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN="$TOKEN" \
  "$INFLUX_IMAGE" >/dev/null
docker run -d --name i2vm-vm --network "$NET" --network-alias victoriametrics -p 127.0.0.1:18428:8428 \
  "$VM_IMAGE" -retentionPeriod=100y >/dev/null

for _ in $(seq 60); do
  docker exec i2vm-influx influx ping >/dev/null 2>&1 && \
  docker exec i2vm-influx influx bucket list --token "$TOKEN" --org test 2>/dev/null | grep -q home_assistant && break
  sleep 2
done

echo "== write test data"
python3 "$HERE/gen_data.py" > "$WORK/data.lp"
wc -l < "$WORK/data.lp"
docker exec -i i2vm-influx influx write --bucket home_assistant --org test --token "$TOKEN" \
  --precision ns < "$WORK/data.lp"

run_i2vm() {
  docker run --rm --network "$NET" \
    -v i2vm-influx-data:/var/lib/influxdb2:ro \
    -v "$WORK/export:/export" \
    -e BUCKET=home_assistant -e INFLUX_URL=http://influxdb:8086 -e INFLUX_TOKEN="$TOKEN" -e INFLUX_ORG=test \
    -e END=2024-04-01T00:00:00Z \
    -e VM_URL=http://victoriametrics:8428 -e VM_DB=home_assistant \
    "$IMAGE" "$@"
}

echo "== migrate (InfluxDB still running, engine mounted read-only, START auto-detected)"
run_i2vm migrate

echo "== state + report"
cat "$WORK/export/state.json"
echo "series in report: $(($(wc -l < "$WORK/export/compare_report.csv") - 1))"
if grep -qv ',OK,' <(tail -n +2 "$WORK/export/compare_report.csv"); then
  echo "FAIL: report contains non-OK rows"; grep -v ',OK,' "$WORK/export/compare_report.csv"; exit 1
fi

echo "== second run must resume (skip everything) and still match"
run_i2vm migrate 2>&1 | tee "$WORK/rerun.log"
[ "$(grep -c 'already imported' "$WORK/rerun.log")" -eq 3 ] || { echo "FAIL: expected 3 skipped chunks"; exit 1; }

echo "== negative test: delete one series in VM, compare must fail"
curl -fsS -X POST http://127.0.0.1:18428/api/v1/admin/tsdb/delete_series \
  --data-urlencode 'match[]={__name__="state_starts"}'
if run_i2vm compare; then
  echo "FAIL: compare did not detect the deleted series"; exit 1
fi
grep -q 'MISSING_IN_VM,"state_starts' "$WORK/export/compare_report.csv" || { echo "FAIL: MISSING_IN_VM not reported"; exit 1; }

echo "== all integration tests passed"
