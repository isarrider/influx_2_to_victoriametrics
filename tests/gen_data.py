#!/usr/bin/env python3
"""Generate Home-Assistant-like line protocol for the integration test (Jan-Mar 2024, every 15 min)."""
import sys
from datetime import datetime, timezone

start = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp())
end = int(datetime(2024, 4, 1, tzinfo=timezone.utc).timestamp())
out = sys.stdout
i = 0
for t in range(start, end, 900):
    ns = t * 1_000_000_000 + 123_456  # sub-millisecond part, VM keeps milliseconds
    # HA style: unit as measurement, numeric "value" + string attribute fields
    out.write(f'°C,domain=sensor,entity_id=wohnzimmer_temp value={20 + (i % 50) / 10},'
              f'friendly_name_str="Wohnzimmer Temperatur",device_class_str="temperature" {ns}\n')
    out.write(f'kWh,domain=sensor,entity_id=house_meter value={1000 + i * 0.25} {ns}\n')
    # escaping: space and comma in tag values, space in measurement name
    out.write(f'W,domain=sensor,entity_id=pv\\ power,zone=a\\,b value={(i * 37) % 5000}.5 {ns}\n')
    out.write(f'my\\ measurement,entity_id=x value={i % 3} {ns}\n')
    # booleans and integers, several fields in one line
    out.write(f'state,entity_id=pump on={"true" if i % 2 else "false"},starts={i // 10}i {ns}\n')
    # series with a different tag set (no "domain" tag)
    if i % 4 == 0:
        out.write(f'%,entity_id=humidity value={40 + i % 30} {ns}\n')
    i += 1
