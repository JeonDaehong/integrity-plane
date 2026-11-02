#!/usr/bin/env bash
# Downloads the Iceberg runtime for Flink 1.20, the AWS bundle (S3FileIO) and the Hadoop client
# classes it needs into compat/engines/flink/jars (mounted into the Flink containers).
set -euo pipefail

ICEBERG=1.10.0
HADOOP=3.3.6
dir="$(dirname "$0")/flink/jars"
mkdir -p "$dir"
maven=https://repo1.maven.org/maven2
for jar in \
  "org/apache/iceberg/iceberg-flink-runtime-1.20/$ICEBERG/iceberg-flink-runtime-1.20-$ICEBERG.jar" \
  "org/apache/iceberg/iceberg-aws-bundle/$ICEBERG/iceberg-aws-bundle-$ICEBERG.jar" \
  "org/apache/hadoop/hadoop-client-api/$HADOOP/hadoop-client-api-$HADOOP.jar" \
  "org/apache/hadoop/hadoop-client-runtime/$HADOOP/hadoop-client-runtime-$HADOOP.jar"; do
  name="$(basename "$jar")"
  [ -f "$dir/$name" ] || curl -sSfL -o "$dir/$name" "$maven/$jar"
done
ls -l "$dir"
