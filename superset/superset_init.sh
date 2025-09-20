#!/bin/bash
set -e

superset db upgrade
superset fab create-admin \
  --username $SUPERSET_USER \
  --firstname $SUPERSET_FIRSTNAME \
  --lastname $SUPERSET_LASTNAME \
  --email $SUPERSET_EMAIL \
  --password $SUPERSET_PASSWORD || true
superset init

exec /usr/bin/run-server.sh