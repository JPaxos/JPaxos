#!/bin/sh
java -ea -cp jpaxos.jar lsr.paxos.test.MultiClient $*
