#!/bin/sh
java -ea -Djava.util.logging.config.file=logging.properties -cp jpaxos.jar lsr.paxos.test.MapClient $*
