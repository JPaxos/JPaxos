#!/bin/bash
echo "Starting client (`hostname`)"
CLASSPATH=jpaxos.jar:Bench.jar:.
host=`hostname`

#/home/desousa/jre1.6.0_21/bin/ -Dcom.sun.management.jmxremote
# Emulab needs to be given the full path to java
# génère une erreur de v, o et autres
#java -ea -Djava.util.logging.config.file=log-client.properties -cp ${CLASSPATH} lsr.paxos.test.EchoClient $*
java -ea -Djava.util.logging.config.file=log-client.properties -cp ${CLASSPATH} startClient
#java -ea -Djava.util.logging.config.file=logging.properties -cp jpaxos.jar lsr.paxos.test.MapClient $*
