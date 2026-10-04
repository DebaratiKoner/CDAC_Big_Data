#!/usr/bin/env python3

import sys

current_hashtag = None
total = 0

for line in sys.stdin:
    line = line.strip()

    if not line:
        continue

    hashtag, count = line.split("\t")

    if hashtag == current_hashtag:
        total += int(count)
    else:
        if current_hashtag is not None:
            print(f"{current_hashtag}\t{total}")

        current_hashtag = hashtag
        total = int(count)

if current_hashtag is not None:
    print(f"{current_hashtag}\t{total}")
