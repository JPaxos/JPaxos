#!/bin/sh
java -ea -Djava.util.logging.config.file=logging.properties -cp jpaxos-1.0rc1.jar lsr.paxos.test.EchoServer $*
