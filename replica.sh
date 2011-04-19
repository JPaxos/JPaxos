#!/bin/sh
java -ea -Djava.util.logging.config.file=logging.properties -cp bin lsr.paxos.test.EchoServer $*
