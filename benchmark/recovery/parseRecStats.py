#!/usr/bin/env python3

import os;
import collections;

f = open("recStats", "r")

header = f.readline()

results=collections.OrderedDict()
lines=0.0

for line in f:
  lines = lines + 1
  tokens = line.strip().split(',')
  for i in range(1, len(tokens)):
    if i not in results:
      results[i] = 0
  startTime = int(tokens[1])
  results[1] = results[1] + startTime
  for i in range(2,len(tokens)-2):
    if tokens[i] != '':
      results[i] = results[i] + int(tokens[i]) - startTime
  for i in range(len(tokens)-2, len(tokens)):
    if tokens[i] != '':
      results[i] = results[i] + int(tokens[i])

rr=list()

for value in results.values() :
  if value == 0:
    rr.append('')
  else:
    rr.append(str(value/lines))

print(','.join(rr))