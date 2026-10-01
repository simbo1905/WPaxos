#!/bin/bash
# Three-node WPaxos cluster on 127.0.0.1 inside one container. One JVM per node; each JVM runs
# the SimpleClient sample, which starts a SimpleServer on that node and then drives load at it.
#
# usage: docker run <image> <groupCount> <useBatch:true|false> <indexType:0=LEVELDB,1=FILE> \
#                    <sendSize> <batchCount> <thdNum> <sendCount> <sleepMills>
#
# Throughput is reported on stdout of each JVM and captured to /opt/wpaxos/node-<port>.log; the
# metric is the "average qps : N" line, one per 10000 completed proposes per node.
#
# indexType=0 requires a linux/amd64 (or x86) native for leveldbjni-all-1.8 and does not work on
# linux/arm64; see docker/Dockerfile.
set -u
GC=$1; BATCH=$2; IDX=$3; SIZE=$4; BCNT=$5; THD=$6; SCNT=$7; SLEEP=$8
ROOT=/opt/wpaxos/data
# SimpleClient loads $rootPath/conf/log4j.properties before it creates a logger.
mkdir -p "$ROOT/conf"
cp /opt/wpaxos/conf/log4j.properties "$ROOT/conf/"
NODES="127.0.0.1:30000,127.0.0.1:30001,127.0.0.1:30002"
CP=$(ls /opt/wpaxos/lib/*.jar | tr '\n' ':')

for p in 30000 30001 30002; do
  java -Xmx1200m -Xms1200m -XX:+UseG1GC -cp "$CP" com.wuba.wpaxos.sample.simple.SimpleClient \
    "$ROOT" "127.0.0.1:$p" "$NODES" "$GC" "$BATCH" "$THD" "$SCNT" "$SLEEP" "$BCNT" "$IDX" "$SIZE" \
    > "/opt/wpaxos/node-$p.log" 2>&1 &
done
wait
