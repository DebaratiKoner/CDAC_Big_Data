#!/usr/bin/env python3

import sys

data = {}

for line in sys.stdin:
    line = line.strip()

    if not line:
        continue

    partition, status, value = line.split("\t")

    key = (partition, status)

    data[key] = data.get(key, 0) + int(value)

for (partition, status), count in sorted(data.items()):
    print(f"{partition}\t{status}\t{count}")
