#!/usr/bin/env python3
import sys

cur, total = None, 0.0
for line in sys.stdin:
    k, v = line.rstrip("\n").split("\t")
    v = float(v)
    if k == cur:
        total += v
    else:
        if cur is not None:
            print("{}\t{}".format(cur, total))
        cur, total = k, v
if cur is not None:
    print("{}\t{}".format(cur, total))
