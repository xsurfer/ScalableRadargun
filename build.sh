#!/bin/sh

#  build.sh
#  
#
#  Created by Diego Didona on 14/03/13.
#
set -e

CONF_DIR=target/distribution/RadarGun-1.1.0-SNAPSHOT/conf/
POM_DIR=plugins/infinispan4/
BENCH="benchmark.xml"
POM="pom.xml"
STATS="all-stats.xml"
BENCH_VERSION
POM_VERSION
STATS_VERSION

print_usage(){
    echo "Usage: ./build.sh version"
    echo "version can be either v5 or cloudtm"
}


if [ -z $1 ]
    then
    print_usage
if [ $1 == "cloudtm"]
    then
    BENCH_VERSION="benchmark_cloudtm.xml"
    POM_VERSION="pom_cloudtm.xml"
    STATS_VERSION="all-stats_cloudtm.xml"
else if [ $1 == "v5"]
    then
    BENCH_VERSION="benchmark_v5.xml"
    POM_VERSION="pom_v5.xml"
    STATS_VERSION="all-stats_v5.xml"
else
    print_usage
    exit(1)
fi

echo "Setting the correct pom to build version $1"
mv ${POM_DIR}/${POM_VERSION} ${POM_DIR}/POM

mvn clean install -DskipTests

echo "Setting the correct benchmark for version $1"
mv ${CONF_DIR}/${BENCH_VERSION} ${CONF_DIR}/${BENCH}

echo "Setting the correct stats file for version $1"
mv ${CONF_DIR}/${STATS_VERSION} ${CONF_DIR}/${STATS}

echo "Done. "


   