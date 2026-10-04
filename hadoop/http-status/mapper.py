#!/usr/bin/env python3

import sys

for line in sys.stdin:
    fields = line.strip().split()

    if len(fields) >= 4:
        status = fields[3]

        if status == "200":
            partition = "0"
        else:
            partition = "1"

        print(f"{partition}\t{status}\t1")
