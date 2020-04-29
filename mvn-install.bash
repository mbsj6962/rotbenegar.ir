#!/bin/bash
mvn install:install-file -Dfile=src/main/resources/lib/hadoop-lzo-0.4.21-SNAPSHOT.jar -DgroupId=com.hadoop.gplcompression -DartifactId=hadoop-lzo -Dversion=0.4.21-SNAPSHOT -Dpackaging=jar
mvn -Dmaven.test.skip=true install
