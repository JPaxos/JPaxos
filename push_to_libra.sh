#!/bin/bash
mountpoint=$(mktemp -d)
echo 'mounting libra'
sshfs jkonczak@libra.cs.put.poznan.pl: ${mountpoint}
let $? && { echo "failed to mount libra" ; exit ; }
trap "fusermount -u ${mountpoint}; echo 'umounted libra'" EXIT
git push --all ${mountpoint}/jpaxos
