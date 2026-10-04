#!/usr/bin/env python3

import sys

data = {}

for line in sys.stdin:
    line = line.strip()

    if not line:
        continue

    partition, status, value = line.split("\t")

    data[status] = data.get(status, 0) + int(value)

for status, count in sorted(data.items(), key=lambda x: int(x[0])):
    print(f"{status}\t{count}")
