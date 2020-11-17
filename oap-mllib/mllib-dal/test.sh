#!/usr/bin/env bash

# Check envs for building
if [[ -z $JAVA_HOME ]]; then
 echo $JAVA_HOME not defined!
 exit 1
fi

if [[ -z $DAALROOT ]]; then
 echo DAALROOT not defined!
 exit 1
fi

if [[ -z $TBBROOT ]]; then
 echo TBBROOT not defined!
 exit 1
fi

if [[ -z $CCL_ROOT ]]; then
 echo CCL_ROOT not defined!
 exit 1
fi

echo === Building Environments ===
echo JAVA_HOME=$JAVA_HOME
echo DAALROOT=$DAALROOT
echo TBBROOT=$TBBROOT
echo CCL_ROOT=$CCL_ROOT
echo GCC Version: $(gcc -dumpversion)
echo =============================

# Enable signal chaining support for JNI
export LD_PRELOAD=$JAVA_HOME/jre/lib/amd64/libjsig.so

# -Dtest=none to turn off the Java tests

# Test all
mvn -Dtest=none -Dmaven.test.skip=false test

# Individual test
# mvn -Dtest=none -DwildcardSuites=org.apache.spark.ml.clustering.IntelKMeansSuite test
# mvn -Dtest=none -DwildcardSuites=org.apache.spark.ml.feature.IntelPCASuite test
