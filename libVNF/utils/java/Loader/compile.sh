#!/bin/bash
javac com/Caller/Jcaller.java com/Person/Person.java
javac -h include/ com/Caller/Jcaller.java com/Person/Person.java
g++ -shared -g -o libmyjni.so -fPIC callee.cpp -I"$JAVA_HOME/include" -I"$JAVA_HOME/include/linux" -I"./include"
java -Djava.library.path=. -cp . com.Caller.Jcaller