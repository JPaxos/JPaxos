#!/bin/sh
java -server -ea -Djava.util.logging.config.file=logging.properties -cp jpaxos.jar lsr.paxos.test.map.MapClient $*
