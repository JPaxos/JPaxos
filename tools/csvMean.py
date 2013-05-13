#!/usr/bin/env python2.7

import csv
import sys

header=[]

def parseFile(fname):
  f = open(fname, 'rb')
  reader = csv.reader(f)
  global header
  header = reader.next()
  result = {}
  for line in reader:
    result[line[0]]=line[1:]
  return result

#files
files=[]
#read files
for arg in sys.argv[1:]:
  files+=[parseFile(arg)]

#keys
keys=[]
# get keys
for file in files:
  keys+=file.keys()
# unique & sort keys
keys=list(set(keys))
keys.sort(key=float)

#printing header
output=csv.writer(sys.stdout)
output.writerow(header)

for key in keys:
  count=0
  sum=[0]*(len(header)-1)
  for f in files:
    if key in f:
      count+=1
      for i in range(0,len(header)-1):
        sum[i]+=float(f[key][i])
  if count!=0:
    for i in range(0,len(header)-1):
      sum[i]/=float(count)
    output.writerow([key] + sum)




