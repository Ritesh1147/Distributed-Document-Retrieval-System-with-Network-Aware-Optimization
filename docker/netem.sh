#!/bin/sh
# Kernel-level network emulation inside a container (needs cap_add: NET_ADMIN).
# Complements the application-level simulator in ddr/netsim.py.
#
#   docker compose exec dist-s0r0 netem.sh 20ms 5ms 1%     # delay jitter loss
#   docker compose exec dist-s0r0 netem.sh off
set -e
DEV=${DEV:-eth0}
if [ "$1" = "off" ]; then
  tc qdisc del dev "$DEV" root 2>/dev/null || true
  echo "netem removed from $DEV"
  exit 0
fi
DELAY=${1:-10ms}; JITTER=${2:-2ms}; LOSS=${3:-0%}
tc qdisc replace dev "$DEV" root netem delay "$DELAY" "$JITTER" distribution normal loss "$LOSS"
tc qdisc show dev "$DEV"
