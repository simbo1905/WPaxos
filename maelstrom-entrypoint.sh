#!/usr/bin/env bash
#
# Runs a Maelstrom test against WPaxos.
#
# Shape mirrors the reference invocation for uvrr-core's maelstrom-lin-kv:
#
#   cd maelstrom && lein run test -w lin-kv --bin <node> \
#     --node-count 5 --time-limit 180 --rate 20 --concurrency 2n \
#     --nemesis partition,kill,pause --nemesis-interval 10
#
# Maelstrom's `kill` nemesis SIGKILLs a node and later re-executes the same binary. A
# restarted node must find its previous physiclog, so the data root is per *run*, not
# per container lifetime: a fresh directory for this invocation, shared by every node
# process (which inherit it through the environment, since they are exec'd with no
# arguments). Node data under it is keyed by Maelstrom node id, so n1's log is n1's
# across its restarts and never collides with n2's.
set -euo pipefail

cd /wpaxos/maelstrom

: "${NODES:=5}"
: "${TIME_LIMIT:=180}"
: "${RATE:=20}"
: "${INTERVAL:=10}"
: "${NEMESIS:=partition,kill,pause}"
: "${CONCURRENCY:=2n}"

# A unique data root per run, so a rerun never inherits a previous run's logs and a
# restart within this run always does.
RUN_DATA="$(mktemp -d /tmp/wpaxos-maelstrom-run-XXXXXX)"
export WPAXOS_MAELSTROM_DATA="$RUN_DATA"
trap 'rm -rf "$RUN_DATA"' EXIT

echo "wpaxos maelstrom: nodes=$NODES time-limit=$TIME_LIMIT rate=$RATE nemesis=$NEMESIS"
echo "wpaxos maelstrom: data root $RUN_DATA"

# --concurrency 2n is per-node in Maelstrom's CLI, so it scales with NODES itself.
exec lein run test \
  -w lin-kv \
  --bin /usr/local/bin/wpaxos-maelstrom-node \
  --node-count "$NODES" \
  --time-limit "$TIME_LIMIT" \
  --rate "$RATE" \
  --concurrency "$CONCURRENCY" \
  --nemesis "$NEMESIS" \
  --nemesis-interval "$INTERVAL"
