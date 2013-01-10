#!/bin/sh
java -server -ea -Djava.util.logging.config.file=logging.properties -cp bin lsr.paxos.test.EchoService $*
