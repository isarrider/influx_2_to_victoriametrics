# i2vm - InfluxDB 2.x -> VictoriaMetrics migration with verification.
#
# Based on the official InfluxDB 2 image, so `influxd inspect export-lp` is the real thing.
# Pick the tag matching (or newer than) the InfluxDB version that wrote your data:
#   docker build --build-arg INFLUXDB_TAG=2.7-alpine -t i2vm .
ARG INFLUXDB_TAG=2-alpine
FROM influxdb:${INFLUXDB_TAG}

RUN apk add --no-cache python3

COPY i2vm.py /usr/local/bin/i2vm
RUN chmod 0755 /usr/local/bin/i2vm \
 && mkdir -p /export \
 && i2vm --version

ENV EXPORT_DIR=/export \
    ENGINE_PATH=/var/lib/influxdb2/engine \
    PYTHONUNBUFFERED=1

LABEL org.opencontainers.image.title="influx_2_to_victoriametrics" \
      org.opencontainers.image.description="Migrate InfluxDB 2.x data to VictoriaMetrics (export-lp, import, per-series compare)" \
      org.opencontainers.image.source="https://github.com/isarrider/influx_2_to_victoriametrics" \
      org.opencontainers.image.licenses="MIT"

VOLUME ["/export"]
WORKDIR /export

# Replace the InfluxDB server entrypoint: this image only runs the migration tool.
ENTRYPOINT ["i2vm"]
CMD ["migrate"]
