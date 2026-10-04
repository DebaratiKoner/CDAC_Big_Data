#!/usr/bin/env python3

import sys

for line in sys.stdin:

    line = line.strip()

    if not line:
        continue

    key, value = line.split("\t")

    status = int(key)

    if status == 200:
        reducer = 0
    else:
        reducer = 1

    print(f"{reducer}\t{key}\t{value}")
