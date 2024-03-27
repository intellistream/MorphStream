#!/bin/bash
source ../dir.sh || exit
javac -cp .:$RUNDIR/morph-core/src/main/java -h $RUNDIR/morph-lib/include $RUNDIR/morph-core/src/main/java/intellistream/morphstream/engine/db/impl/remote/RemoteCallLibrary.java
cd $LIBDIR
gcc -fPIC -shared -I$JAVA_HOME/include -I$RUNDIR/morph-lib/include $RUNDIR/morph-lib/src/RemoteCallLibrary.c -o RemoteCallLibrary.so
cd -
cd ../
mvn install -DskipTests
cd -

