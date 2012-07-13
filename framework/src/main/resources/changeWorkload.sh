#!/bin/bash

## Load includes
if [ "x$RADARGUN_HOME" = "x" ]; then DIRNAME=`dirname $0`; RADARGUN_HOME=`cd $DIRNAME/..; pwd` ; fi; export RADARGUN_HOME
. ${RADARGUN_HOME}/bin/includes.sh
. ${RADARGUN_HOME}/bin/environment.sh

CP=${RADARGUN_HOME}/lib/radargun-*.jar
JAVA="org.radargun.WorkloadJmxRequest"
OP="-high"
OBJ="TpccBenchmark"

help_and_exit() {
echo "usage: $0 -high|-low|-random -write-percentage <write percentage> [-large-write-set] [-jmx-mbean <mbean name>] [-nr-threads <value>] <slaves list hostname:port>"
exit 0;
}

while [ -n "$1" ]; do
case $1 in
  -high) OP="-high"; shift 1;;
  -low) OP="-low"; shift 1;;
  -random) OP="-random"; shift 1;;
  -write-percentage) WRITE_PERCENT=$2; shift 2;;
  -large-write-set) LARGE_WS="-large-ws"; shift 1;;
  -jmx-mbean) OBJ=$2; shift 2;;
  -nr-threads) NR_THREADS="-nr-threads "$2; shift 2;;
  -h) help_and_exit;;
  -*) echo "Unknown option $1"; shift 1;;
  *) SLAVES=${SLAVES}" "$1; shift 1;;
esac
done

if [ -z "$WRITE_PERCENT" ]; then
echo "Write Percentage is required";
help_and_exit;
fi

if [ -z "$SLAVES" ]; then
echo "No slaves found!";
help_and_exit;
fi

for slave in ${SLAVES}; do

if [[ "$slave" == *:* ]]; then
HOST=`echo $slave | cut -d: -f1`
PORT=`echo $slave | cut -d: -f2`
else
HOST=$slave
PORT=${JMX_SLAVES_PORT}
fi

CMD="java -cp ${CP} ${JAVA} ${OP} -jmx-component ${OBJ} -write-percent ${WRITE_PERCENT} -hostname ${HOST} -port ${PORT} ${LARGE_WS} ${NR_THREADS}"
echo $CMD
eval $CMD

done
exit 0