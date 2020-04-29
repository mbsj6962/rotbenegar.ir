#!/bin/bash
USAGE="USAGE: ./run.bash \"arg1 arg2 ...\""

echo ${USAGE}
read -p "Press any key continue..." anykey

arguments=$1
mvn exec:java -Dmaven.test.skip=true -Dspark.master=local[8] -Dexec.mainClass=ir.ac.itrc.rotbenegar.Pipeline.Webranking -Dexec.args="${arguments}"
#JAVA_HOME=/usr/java/jdk1.8.0_151 mvn "-Dexec.args=-classpath %classpath ir.ac.itrc.rotbenegar.Pipeline.Webranking ${arguments}" -Dexec.executable=/usr/java/jdk1.8.0_151/bin/java org.codehaus.mojo:exec-maven-plugin:1.2.1:exec
