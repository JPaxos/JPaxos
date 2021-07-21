#!/bin/sh

[ $# -eq 2 ] || { echo "bad argument count" ; exit 1 ; }
[ $(id -u) -eq 0 ]  || { echo "must be root" ; exit 1 ; }

CONTROLPIPE="$1"
INTERVAL="$2"

mkfifo -m a+rw "$CONTROLPIPE" || { echo "cannot create control pipe" ; exit 1 ; }

# cleanup potential previous config
iptables -F
tc qdisc del dev eth0 root &>/dev/null

# force cleanup on exit
trap "
iptables -F
tc qdisc del dev eth0 root &>/dev/null
rm \"$CONTROLPIPE\"
" exit

# traffic shaping by mark
tc qdisc add dev eth0 handle 1 root prio bands 3 priomap 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2
tc filter add dev eth0 parent 1:0 handle 2 fw flowid 0
tc filter add dev eth0 parent 1:0 handle 3 fw flowid 1

# configuring marking packets and counting traffic
iptables-restore << EOF
*filter
:INPUT ACCEPT [0:0]
:FORWARD ACCEPT [0:0]
:OUTPUT ACCEPT [0:0]
-A INPUT -p tcp -m tcp --sport 2000:2999
-A INPUT -p tcp -m tcp --dport 2000:2999
-A INPUT -p tcp -m tcp --dport 3000:3999
-A OUTPUT -p tcp -m tcp --sport 2000:2999 -j MARK --set-xmark 0x2/0xffffffff
-A OUTPUT -p tcp -m tcp --dport 2000:2999 -j MARK --set-xmark 0x2/0xffffffff
-A OUTPUT -p tcp -m tcp --sport 3000:3999 -j MARK --set-xmark 0x3/0xffffffff
COMMIT
EOF

# get stats & wait for any data on the control pipe
/opt/jkonczak/systemStats2 "$CONTROLPIPE" "$INTERVAL"

# cleanup happens upon exit handler
