#!env python
import sys
import numpy

wses = {}
wsrange = range(1,11)

for ws in wsrange:
  f = open(str(ws) + '.out', 'r')
  thisws = {}
  for line in f:
    l = line.split(' ')
    clients=int(l[0])
    if clients not in thisws:
      thisws[clients]=[]
    thisws[clients].append(float(l[1]))
  wses[ws]=thisws

cliCounts=[]
cliCounts+=wses[1].keys()
cliCounts.sort()

sys.stdout.write("ws")
for ws in wsrange:
  sys.stdout.write(" ")
  sys.stdout.write(str(ws))
  sys.stdout.write(" ")
  sys.stdout.write("±")
sys.stdout.write("\n")

#for cli in cliCounts:
  #sys.stdout.write(str(cli))
  #for ws in wsrange:
    #sys.stdout.write(" ")
    #sys.stdout.write(str(numpy.min(wses[ws][cli])))
    #sys.stdout.write(" ")
    #sys.stdout.write(str(numpy.std(wses[ws][cli])))
  #sys.stdout.write("\n")
    
for cli in cliCounts:
  for ws in wsrange:
    sys.stdout.write(str(cli))
    sys.stdout.write(" ")
    sys.stdout.write(str(ws))
    sys.stdout.write(" ")
    sys.stdout.write(str(numpy.min(wses[ws][cli])))
    sys.stdout.write("\n")