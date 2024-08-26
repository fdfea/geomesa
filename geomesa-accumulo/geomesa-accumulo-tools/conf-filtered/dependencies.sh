#! /usr/bin/env bash
#
# Copyright (c) 2013-%%copyright.year%% Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# This file lists the dependencies required for running the geomesa-accumulo command-line tools.
# Usually these dependencies will be provided by the environment (e.g. ACCUMULO_HOME).
# Update the versions as required to match the target environment.

accumulo_install_version="%%accumulo.version.recommended%%"
hadoop_install_version="%%hadoop.version.recommended%%"
zookeeper_install_version="%%zookeeper.version.recommended%%"
# required for hadoop - make sure it corresponds to the hadoop installed version
guava_install_version="%%accumulo.guava.version%%"

# gets the dependencies for this module
# args:
#   $1 - current classpath
function dependencies() {
  local classpath="$1"

  local accumulo_version="$accumulo_install_version"
  local hadoop_version="$hadoop_install_version"
  local zk_version="$zookeeper_install_version"

  if [[ -n "$classpath" ]]; then
    accumulo_version="$(get_classpath_version accumulo-core "$classpath" "$accumulo_version")"
    hadoop_version="$(get_classpath_version hadoop-common "$classpath" "$hadoop_version")"
    zk_version="$(get_classpath_version zookeeper "$classpath" "$zk_version")"
  fi

  declare -a gavs=(
    "org.apache.accumulo:accumulo-core:${accumulo_version}:jar"
    "org.apache.accumulo:accumulo-server-base:${accumulo_version}:jar"
    "org.apache.accumulo:accumulo-start:${accumulo_version}:jar"
    "org.apache.accumulo:accumulo-hadoop-mapreduce:${accumulo_version}:jar"
    "org.apache.zookeeper:zookeeper:${zk_version}:jar"
    "org.apache.commons:commons-configuration2:2.10.1:jar"
    "org.apache.commons:commons-text:1.11.0:jar"
    "org.apache.commons:commons-collections4:4.4:jar"
    "org.apache.commons:commons-vfs2:2.9.0:jar"
    "commons-logging:commons-logging:1.3.3:jar"
    "org.apache.hadoop:hadoop-auth:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-common:${hadoop_version}:jar"
    "org.apache.hadoop:hadoop-hdfs:${hadoop_version}:jar"
    "com.fasterxml.woodstox:woodstox-core:5.3.0:jar"
    "org.codehaus.woodstox:stax2-api:4.2.1:jar"
    "com.google.guava:guava:${guava_install_version}:jar"
  )

  # add accumulo 2.1 jars if needed
  if version_ge "${accumulo_version}" 2.1.0; then
    local micrometer_version
    local opentelemetry_version

    if version_ge "${accumulo_version}" 2.1.3; then
      micrometer_version="1.12.2"
      opentelemetry_version="1.34.1"
    else
      # these versions seem compatible even though they're not the exact versions shipped
      micrometer_version="1.11.1"
      opentelemetry_version="1.27.0"
      gavs+=(
        "io.opentelemetry:opentelemetry-semconv:${opentelemetry_version}-alpha:jar"
      )
    fi

    gavs+=(
      "org.apache.thrift:libthrift:%%thrift.version%%:jar"
      "io.micrometer:micrometer-core:${micrometer_version}:jar"
      "io.micrometer:micrometer-commons:${micrometer_version}:jar"
      "io.opentelemetry:opentelemetry-api:${opentelemetry_version}:jar"
      "io.opentelemetry:opentelemetry-context:${opentelemetry_version}:jar"
    )
  else
    gavs+=(
      "org.apache.thrift:libthrift:0.12.0:jar"
    )
  fi

  # add hadoop 3+ jars if needed
  if version_ge "${hadoop_version}" 3.0.0; then
    gavs+=(
      "org.apache.hadoop:hadoop-client-api:${hadoop_version}:jar"
      "org.apache.hadoop:hadoop-client-runtime:${hadoop_version}:jar"
    )
  else
    gavs+=(
      "commons-configuration:commons-configuration:1.6:jar"
    )
  fi

  # compare the version of zookeeper to determine if we need zookeeper-jute (version >= 3.5.5)
  JUTE_FROM_VERSION="3.5.5"
  if version_ge "${zk_version}" $JUTE_FROM_VERSION; then
    gavs+=(
      "org.apache.zookeeper:zookeeper-jute:${zk_version}:jar"
    )
  fi

  echo "${gavs[@]}" | tr ' ' '\n' | sort | tr '\n' ' '
}

# gets any dependencies that should be removed from the classpath for this module
# args:
#   $1 - current classpath
function exclude_dependencies() {
  echo "commons-text-1.4.jar"
}
