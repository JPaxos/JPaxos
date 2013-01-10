#!/bin/sh
java -server -ea -cp bin lsr.paxos.test.GenericMultiClient $* true
