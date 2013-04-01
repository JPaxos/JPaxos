#!/bin/bash

java -server -ea -Djava.util.logging.config.file=logging.properties -cp bin lsr.paxos.test.InternalClientTest "${1}" "${2:-100}" "${3:-10}" "${4:-1024}"
