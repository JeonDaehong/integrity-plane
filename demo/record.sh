#!/usr/bin/env bash
# Records demo/demo.gif: the compose stack, demo/demo.py through the Plane, rendered with agg.
# Needs docker compose, asciinema, agg, a Python with pyiceberg[pyarrow,s3fs] ($PYTHON) and the
# integrity CLI ($CLI). Run from the repository root.
set -euo pipefail

docker compose -f deploy/docker-compose.yml down -v >/dev/null 2>&1 || true
docker compose -f deploy/docker-compose.yml up -d --build
for i in $(seq 1 120); do
  curl -sf http://127.0.0.1:8181/v1/integrity/status >/dev/null && break
  sleep 2
done
asciinema rec --overwrite --cols 112 --rows 30 -c "${PYTHON:-python} demo/demo.py ${CLI:-integrity}" demo/demo.cast
agg --font-size 15 --speed 1.0 --last-frame-duration 6 --theme monokai demo/demo.cast demo/demo.gif
docker compose -f deploy/docker-compose.yml down -v >/dev/null 2>&1
